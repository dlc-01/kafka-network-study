package usecase

import (
	"github.com/codecrafters-io/kafka-starter-go/internal/domain"
	"github.com/codecrafters-io/kafka-starter-go/internal/domain/request"
	"github.com/codecrafters-io/kafka-starter-go/internal/domain/response"
)

type RequestProcessor struct{}

func NewRequestProcessor() *RequestProcessor {
	return &RequestProcessor{}
}

func (p *RequestProcessor) Process(req *request.MessageRequest) (*response.MessageResponse, error) {
	switch body := req.Body.(type) {
	case *request.ApiVersionsRequest:
		return p.processApiVersions(req.Header), nil
	case *request.DescribeTopicPartitionsRequest:
		return p.processDescribeTopicPartitions(req.Header, body), nil
	default:
		return p.processApiVersions(req.Header), nil
	}
}

func (p *RequestProcessor) processApiVersions(h request.RequestHeader) *response.MessageResponse {
	var errCode uint16

	if h.ApiVersion > domain.MaximumVersion {
		errCode = domain.ErrorNotSupportedApiVersion
	}

	body := &response.ApiVersionsResponseBody{
		ErrorCode:    errCode,
		ApiKeys:      []response.ApiKeyResponse{response.GetApiVersions(), response.GetDescribeTopicPartitionsApikey()},
		ThrottleTime: 0,
	}

	return &response.MessageResponse{
		CorrelationID: h.CorrelationID,
		Body:          body,
	}
}

func (p *RequestProcessor) processDescribeTopicPartitions(h request.RequestHeader, r *request.DescribeTopicPartitionsRequest) *response.MessageResponse {
	var zeroUUID [16]byte

	topics := make([]response.TopicDescription, 0, len(r.Topics))
	for _, t := range r.Topics {
		topics = append(topics, response.TopicDescription{
			ErrorCode:    domain.ErrorUnknownTopicOrPartition,
			Name:         t.Name,
			TopicID:      zeroUUID,
			IsInternal:   false,
			Partitions:   nil,
			AuthorizedOp: 0,
		})
	}

	body := &response.DescribeTopicPartitionsResponseBody{
		ThrottleTime: 0,
		Topics:       topics,
		NextCursor:   -1,
	}

	return &response.MessageResponse{
		CorrelationID: h.CorrelationID,
		Body:          body,
	}
}
