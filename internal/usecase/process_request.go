package usecase

import (
	"fmt"
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

func NewRequestProcessor(metadataRepo ports.MetadataRepository, logManager ports.LogManager) *RequestProcessor {
	return &RequestProcessor{metadataRepo: metadataRepo, logManager: logManager}
}

func (p *RequestProcessor) Process(req *request.MessageRequest) (*response.MessageResponse, error) {
	fmt.Println(req)
	switch body := req.Body.(type) {
	case *request.ApiVersionsRequest:
		return p.processApiVersions(req.Header), nil
	case *request.DescribeTopicPartitionsRequest:
		return p.processDescribeTopicPartitions(req.Header, body), nil
	case *request.FetchRequest:
		return p.processFetch(req.Header, body), nil
	default:
		return p.processApiVersions(req.Header), nil
	}
}

func (p *RequestProcessor) processApiVersions(h request.RequestHeader) *response.MessageResponse {
	var errCode uint16

	if h.ApiVersion > domain.MaximumVersionApiKey {
		errCode = domain.ErrorNotSupportedApiVersion
	}

	body := &response.ApiVersionsResponseBody{
		ErrorCode: errCode,
		ApiKeys: []response.ApiKeyResponse{
			response.GetApiVersions(),
			response.GetDescribeTopicPartitionsApikey(),
			response.GetFetchApiKey(),
			response.GetProduceApiKey()},
		ThrottleTime: 0,
	}

	return &response.MessageResponse{
		CorrelationID: h.CorrelationID,
		Body:          body,
	}
}

func (p *RequestProcessor) processDescribeTopicPartitions(h request.RequestHeader, r *request.DescribeTopicPartitionsRequest) *response.MessageResponse {
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

		td := response.TopicDescription{
			ErrorCode:    0,
			Name:         meta.Name,
			TopicID:      meta.TopicID,
			IsInternal:   false,
			AuthorizedOp: 0,
		}

		for _, pmeta := range meta.Partitions {
			td.Partitions = append(td.Partitions, response.PartitionDescription{
				ErrorCode:       0,
				PartitionIndex:  pmeta.PartitionIndex,
				LeaderID:        pmeta.LeaderID,
				LeaderEpoch:     pmeta.LeaderEpoch,
				Replicas:        pmeta.Replicas,
				ISR:             pmeta.ISR,
				EligibleLeaders: []int32{},
				LastKnownELR:    []int32{},
				OfflineReplicas: []int32{},
			})
		}

		topics = append(topics, td)
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
		meta, err := p.metadataRepo.GetTopicByID(t.TopicID)
		if err != nil || meta == nil {
		}
		partition := response.FetchPartitionResponse{
			PartitionIndex:   0,
			ErrorCode:        domain.ErrorUnknownTopicId,
			HighWatermark:    0,
			LastStableOffset: 0,
			LogStartOffset:   0,
			Records:          nil,
		}

		if meta != nil {
			partition.ErrorCode = 0
			raw, err := p.logManager.LoadLog(meta.Name, 0)
			if err != nil {
				raw = nil
			}
			fmt.Println(raw)
			partition.Records = raw
		}

		topicResp := response.FetchTopicResponse{
			TopicID:    t.TopicID,
			Partitions: []response.FetchPartitionResponse{partition},
		}

		responses = append(responses, topicResp)
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
