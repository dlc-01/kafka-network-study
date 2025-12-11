package codec

import (
	"encoding/binary"
	"errors"

	"github.com/codecrafters-io/kafka-starter-go/internal/domain"
	"github.com/codecrafters-io/kafka-starter-go/internal/domain/request"
	"github.com/codecrafters-io/kafka-starter-go/internal/ports"
)

type BinaryRequestParser struct{}

func NewBinaryRequestParser() ports.RequestParser {
	return &BinaryRequestParser{}
}

func (p *BinaryRequestParser) Parse(data []byte) (*request.MessageRequest, error) {
	if len(data) < 12 {
		return nil, errors.New("buffer too short for request header")
	}

	size := binary.BigEndian.Uint32(data[:4])

	header := request.RequestHeader{
		ApiKey:        binary.BigEndian.Uint16(data[4:6]),
		ApiVersion:    binary.BigEndian.Uint16(data[6:8]),
		CorrelationID: binary.BigEndian.Uint32(data[8:12]),
	}

	payload := data[12:]

	var body request.RequestBody
	var err error

	switch header.ApiKey {
	case domain.ApiVersionApikey:
		body = &request.ApiVersionsRequest{}

	case domain.DescribeTopicPartitionsApikey:
		body, err = parseDescribeTopicPartitionsRequest(payload)
		if err != nil {
			return nil, err
		}
	case domain.FetchApikey:
		body, err = parseFetchRequest(payload)
		if err != nil {
			return nil, err
		}

	default:
		body = &request.ApiVersionsRequest{}
	}

	return &request.MessageRequest{
		Size:   size,
		Header: header,
		Body:   body,
	}, nil
}

func parseDescribeTopicPartitionsRequest(payload []byte) (*request.DescribeTopicPartitionsRequest, error) {
	if len(payload) < 2 {
		return nil, errors.New("payload too short for client_id length")
	}

	clientLen := int(binary.BigEndian.Uint16(payload[0:2]))
	offset := 2

	if len(payload) < offset+clientLen {
		return nil, errors.New("payload too short for client_id bytes")
	}
	offset += clientLen

	if len(payload) < offset+1 {
		return nil, errors.New("payload too short for TAG_BUFFER after client_id")
	}
	offset++

	if len(payload) < offset+1 {
		return nil, errors.New("payload too short for topics compact array length")
	}
	topicsLenByte := int(payload[offset])
	offset++
	topicsCount := topicsLenByte - 1
	if topicsCount < 0 {
		return nil, errors.New("invalid topics count")
	}

	topics := make([]request.TopicRequest, 0, topicsCount)

	for i := 0; i < topicsCount; i++ {
		if len(payload) < offset+1 {
			return nil, errors.New("payload too short for topic name length byte")
		}
		nameLenByte := int(payload[offset])
		offset++
		nameLen := nameLenByte - 1
		if nameLen < 0 {
			return nil, errors.New("invalid topic name length")
		}

		if len(payload) < offset+nameLen {
			return nil, errors.New("payload too short for topic name bytes")
		}
		name := string(payload[offset : offset+nameLen])
		offset += nameLen

		if len(payload) < offset+1 {
			return nil, errors.New("payload too short for topic TAG_BUFFER")
		}
		offset++

		topics = append(topics, request.TopicRequest{Name: name})
	}

	if len(payload) >= offset+4 {
		offset += 4
	}

	var cursor int8 = -1
	if len(payload) >= offset+1 {
		cursor = int8(payload[offset])
		offset++
	}

	return &request.DescribeTopicPartitionsRequest{
		Topics: topics,
		Cursor: cursor,
	}, nil
}
func readUvarint(b []byte) (uint64, int, error) {
	v, n := binary.Uvarint(b)
	if n == 0 {
		return 0, 0, errors.New("uvarint: buffer too small")
	}
	if n < 0 {
		return 0, 0, errors.New("uvarint: overflow")
	}
	return v, n, nil
}

func parseFetchRequest(payload []byte) (*request.FetchRequest, error) {
	r := &request.FetchRequest{}
	offset := 0

	if len(payload) < offset+2 {
		return nil, errors.New("fetch: too short for client_id length")
	}
	clientLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	offset += 2

	if len(payload) < offset+clientLen {
		return nil, errors.New("fetch: too short for client_id bytes")
	}
	offset += clientLen

	if len(payload) < offset+1 {
		return nil, errors.New("fetch: too short for header tag buffer")
	}
	offset++

	if len(payload) < offset+4 {
		return nil, errors.New("fetch: too short for max_wait_ms")
	}
	offset += 4

	if len(payload) < offset+4 {
		return nil, errors.New("fetch: too short for min_bytes")
	}
	offset += 4

	if len(payload) < offset+4 {
		return nil, errors.New("fetch: too short for max_bytes")
	}
	offset += 4

	if len(payload) < offset+1 {
		return nil, errors.New("fetch: too short for isolation_level")
	}
	offset++

	if len(payload) < offset+4 {
		return nil, errors.New("fetch: too short for session_id")
	}
	offset += 4

	if len(payload) < offset+4 {
		return nil, errors.New("fetch: too short for session_epoch")
	}
	offset += 4

	if len(payload) <= offset {
		return nil, errors.New("fetch: too short for topics compact length")
	}
	topicsLen, n, err := readUvarint(payload[offset:])
	if err != nil {
		return nil, err
	}
	offset += n

	topicsCount := int(topicsLen) - 1
	if topicsCount < 0 {
		return nil, errors.New("fetch: invalid topics count")
	}
	if topicsCount == 0 {
		return r, nil
	}

	if len(payload) < offset+16 {
		return nil, errors.New("fetch: truncated topic_id")
	}
	var id [16]byte
	copy(id[:], payload[offset:offset+16])
	offset += 16

	r.Topics = append(r.Topics, request.FetchTopic{
		TopicID: id,
	})

	return r, nil
}
