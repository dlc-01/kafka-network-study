package usecase

import (
	"sort"

	"github.com/codecrafters-io/kafka-starter-go/internal/domain"
	"github.com/codecrafters-io/kafka-starter-go/internal/domain/request"
	"github.com/codecrafters-io/kafka-starter-go/internal/domain/response"
	"github.com/codecrafters-io/kafka-starter-go/internal/ports"
)

type RequestProcessor struct {
	metadataRepo ports.MetadataRepository
	logManager   ports.LogManager
}

func NewRequestProcessor(
	metadataRepo ports.MetadataRepository,
	logManager ports.LogManager,
) *RequestProcessor {
	return &RequestProcessor{
		metadataRepo: metadataRepo,
		logManager:   logManager,
	}
}

func (p *RequestProcessor) Process(
	req *request.MessageRequest,
) (*response.MessageResponse, error) {

	switch body := req.Body.(type) {
	case *request.ApiVersionsRequest:
		return p.processApiVersions(req.Header), nil

	case *request.DescribeTopicPartitionsRequest:
		return p.processDescribeTopicPartitions(req.Header, body), nil

	case *request.FetchRequest:
		return p.processFetch(req.Header, body), nil

	case *request.ProduceRequest:
		return p.processProduce(req.Header, body), nil

	default:
		return p.processApiVersions(req.Header), nil
	}
}

func (p *RequestProcessor) processApiVersions(
	h request.RequestHeader,
) *response.MessageResponse {

	var errorCode uint16
	if h.ApiVersion > domain.MaximumVersionApiKey {
		errorCode = domain.ErrorNotSupportedApiVersion
	}

	body := &response.ApiVersionsResponseBody{
		ErrorCode: errorCode,
		ApiKeys: []response.ApiKeyResponse{
			response.GetApiVersions(),
			response.GetDescribeTopicPartitionsApikey(),
			response.GetFetchApiKey(),
			response.GetProduceApiKey(),
		},
		ThrottleTime: 0,
	}

	return &response.MessageResponse{
		CorrelationID: h.CorrelationID,
		Body:          body,
	}
}

func (p *RequestProcessor) processDescribeTopicPartitions(
	h request.RequestHeader,
	r *request.DescribeTopicPartitionsRequest,
) *response.MessageResponse {

	topics := make([]response.TopicDescription, 0, len(r.Topics))

	for _, t := range r.Topics {
		meta, err := p.metadataRepo.GetTopic(t.Name)

		if err != nil || meta == nil {
			topics = append(topics, response.TopicDescription{
				ErrorCode:  domain.ErrorUnknownTopicOrPartition,
				Name:       t.Name,
				TopicID:    [16]byte{},
				IsInternal: false,
				Partitions: nil,
			})
			continue
		}

		topicDesc := response.TopicDescription{
			ErrorCode:    0,
			Name:         meta.Name,
			TopicID:      meta.TopicID,
			IsInternal:   false,
			AuthorizedOp: 0,
		}

		for _, pm := range meta.Partitions {
			topicDesc.Partitions = append(
				topicDesc.Partitions,
				response.PartitionDescription{
					ErrorCode:       0,
					PartitionIndex:  pm.PartitionIndex,
					LeaderID:        pm.LeaderID,
					LeaderEpoch:     pm.LeaderEpoch,
					Replicas:        pm.Replicas,
					ISR:             pm.ISR,
					EligibleLeaders: []int32{},
					LastKnownELR:    []int32{},
					OfflineReplicas: []int32{},
				},
			)
		}

		topics = append(topics, topicDesc)
	}

	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})

	body := &response.DescribeTopicPartitionsResponseBody{
		ThrottleTime: 0,
		Topics:       topics,
		NextCursor:   -1,
	}

	return &response.MessageResponse{
		CorrelationID: h.CorrelationID,
		HeaderVersion: 1,
		Body:          body,
	}
}

func (p *RequestProcessor) processFetch(
	h request.RequestHeader,
	r *request.FetchRequest,
) *response.MessageResponse {

	responses := make([]response.FetchTopicResponse, 0, len(r.Topics))

	for _, t := range r.Topics {
		meta, _ := p.metadataRepo.GetTopicByID(t.TopicID)

		partitionResp := response.FetchPartitionResponse{
			PartitionIndex:   0,
			ErrorCode:        domain.ErrorUnknownTopicId,
			HighWatermark:    0,
			LastStableOffset: 0,
			LogStartOffset:   0,
			Records:          nil,
		}

		if meta != nil {
			raw, err := p.logManager.LoadLog(meta.Name, 0)
			if err != nil {
				raw = nil
			}

			partitionResp.ErrorCode = 0
			partitionResp.Records = raw
		}

		responses = append(
			responses,
			response.FetchTopicResponse{
				TopicID:    t.TopicID,
				Partitions: []response.FetchPartitionResponse{partitionResp},
			},
		)
	}

	body := &response.FetchResponseBody{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		SessionID:      0,
		Responses:      responses,
	}

	return &response.MessageResponse{
		CorrelationID: h.CorrelationID,
		HeaderVersion: 1,
		Body:          body,
	}
}

func (p *RequestProcessor) processProduce(
	h request.RequestHeader,
	r *request.ProduceRequest,
) *response.MessageResponse {

	topics := make([]response.ProduceTopicResponse, 0, len(r.Topics))

	for _, t := range r.Topics {
		topicResp := response.ProduceTopicResponse{
			Name:       t.Name,
			Partitions: make([]response.ProducePartitionResponse, 0, len(t.Partitions)),
		}

		meta, err := p.metadataRepo.GetTopic(t.Name)
		topicExists := err == nil && meta != nil

		for _, part := range t.Partitions {
			partitionResp := response.ProducePartitionResponse{
				Index:           part.Index,
				ErrorCode:       domain.ErrorUnknownTopicOrPartition,
				BaseOffset:      -1,
				LogAppendTimeMs: -1,
				LogStartOffset:  -1,
			}

			if topicExists && partitionExists(meta, part.Index) {
				p.logManager.AppendLog(t.Name, part.Index, part.Records)

				partitionResp.ErrorCode = 0
				partitionResp.BaseOffset = 0
				partitionResp.LogAppendTimeMs = -1
				partitionResp.LogStartOffset = 0
			}

			topicResp.Partitions = append(topicResp.Partitions, partitionResp)
		}

		topics = append(topics, topicResp)
	}

	body := &response.ProduceResponseBody{
		ThrottleTimeMs: 0,
		Topics:         topics,
	}

	return &response.MessageResponse{
		CorrelationID: h.CorrelationID,
		HeaderVersion: 1,
		Body:          body,
	}
}

func partitionExists(meta *domain.TopicMetadata, index int32) bool {
	for _, p := range meta.Partitions {
		if p.PartitionIndex == index {
			return true
		}
	}
	return false
}
