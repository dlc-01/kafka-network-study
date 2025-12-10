package codec

import (
	"encoding/binary"
	"errors"

	"github.com/codecrafters-io/kafka-starter-go/internal/domain/response"
	"github.com/codecrafters-io/kafka-starter-go/internal/ports"
)

type BinaryResponseBuilder struct{}

func NewBinaryResponseBuilder() ports.ResponseBuilder {
	return &BinaryResponseBuilder{}
}

func (b *BinaryResponseBuilder) Build(resp *response.MessageResponse) ([]byte, error) {
	if resp.Body == nil {
		return nil, errors.New("nil response body")
	}

	switch body := resp.Body.(type) {
	case *response.ApiVersionsResponseBody:
		return b.buildApiVersions(resp.CorrelationID, body)
	case *response.DescribeTopicPartitionsResponseBody:
		return b.buildDescribeTopicPartitions(resp.CorrelationID, body)
	default:
		return nil, errors.New("unsupported response body type")
	}
}

func (b *BinaryResponseBuilder) buildApiVersions(
	correlationID uint32,
	body *response.ApiVersionsResponseBody,
) ([]byte, error) {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, correlationID)

	outBody := make([]byte, 0)

	errBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errBytes, body.ErrorCode)
	outBody = append(outBody, errBytes...)

	outBody = append(outBody, byte(len(body.ApiKeys)+1))

	tmp := make([]byte, 2)
	for _, k := range body.ApiKeys {
		binary.BigEndian.PutUint16(tmp, k.ApiKey)
		outBody = append(outBody, tmp...)

		binary.BigEndian.PutUint16(tmp, k.MinVersion)
		outBody = append(outBody, tmp...)

		binary.BigEndian.PutUint16(tmp, k.MaxVersion)
		outBody = append(outBody, tmp...)

		outBody = append(outBody, 0)
	}

	throttle := make([]byte, 4)
	binary.BigEndian.PutUint32(throttle, body.ThrottleTime)
	outBody = append(outBody, throttle...)

	outBody = append(outBody, 0)

	payload := append(header, outBody...)
	size := uint32(len(payload))

	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, size)

	return append(sizeBytes, payload...), nil
}

func (b *BinaryResponseBuilder) buildDescribeTopicPartitions(
	correlationID uint32,
	body *response.DescribeTopicPartitionsResponseBody,
) ([]byte, error) {
	header := make([]byte, 0, 5)

	corrBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(corrBytes, correlationID)
	header = append(header, corrBytes...)

	header = append(header, 0)

	outBody := make([]byte, 0)

	throttle := make([]byte, 4)
	binary.BigEndian.PutUint32(throttle, body.ThrottleTime)
	outBody = append(outBody, throttle...)

	outBody = append(outBody, byte(len(body.Topics)+1))

	for _, t := range body.Topics {
		errBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(errBytes, t.ErrorCode)
		outBody = append(outBody, errBytes...)

		nameLen := len(t.Name)
		outBody = append(outBody, byte(nameLen+1))
		outBody = append(outBody, []byte(t.Name)...)

		outBody = append(outBody, t.TopicID[:]...)

		if t.IsInternal {
			outBody = append(outBody, 1)
		} else {
			outBody = append(outBody, 0)
		}

		outBody = append(outBody, 1)

		authBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(authBytes, t.AuthorizedOp)
		outBody = append(outBody, authBytes...)

		outBody = append(outBody, 0)
	}

	outBody = append(outBody, 0xff)
	
	outBody = append(outBody, 0)

	payload := append(header, outBody...)
	size := uint32(len(payload))

	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, size)

	return append(sizeBytes, payload...), nil
}
