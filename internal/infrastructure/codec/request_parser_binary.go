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
		return nil, errors.New("request: buffer too short for header")
	}

	size := binary.BigEndian.Uint32(data[:4])
	header := request.RequestHeader{
		ApiKey:        binary.BigEndian.Uint16(data[4:6]),
		ApiVersion:    binary.BigEndian.Uint16(data[6:8]),
		CorrelationID: binary.BigEndian.Uint32(data[8:12]),
	}

	payload := data[12:]

	var (
		body request.RequestBody
		err  error
	)

	switch header.ApiKey {
	case domain.ApiVersionApikey:
		body = &request.ApiVersionsRequest{}

	case domain.DescribeTopicPartitionsApikey:
		body, err = parseDescribeTopicPartitionsRequest(payload)

	case domain.FetchApikey:
		body, err = parseFetchRequest(payload)

	case domain.ProduceApiKey:
		body, err = parseProduceRequest(payload)

	default:
		body = &request.ApiVersionsRequest{}
	}

	if err != nil {
		return nil, err
	}

	return &request.MessageRequest{
		Size:   size,
		Header: header,
		Body:   body,
	}, nil
}

/* ========================= DescribeTopicPartitions ========================= */

func parseDescribeTopicPartitionsRequest(b []byte) (*request.DescribeTopicPartitionsRequest, error) {
	offset := 0

	if err := need(b, offset, 2, "describe: client_id length"); err != nil {
		return nil, err
	}
	clientLen := int(binary.BigEndian.Uint16(b[offset:]))
	offset += 2

	if err := need(b, offset, clientLen, "describe: client_id bytes"); err != nil {
		return nil, err
	}
	offset += clientLen

	if err := need(b, offset, 1, "describe: client tag buffer"); err != nil {
		return nil, err
	}
	offset++

	if err := need(b, offset, 1, "describe: topics length"); err != nil {
		return nil, err
	}
	topicsCount := int(b[offset]) - 1
	offset++

	if topicsCount < 0 {
		return nil, errors.New("describe: invalid topics count")
	}

	topics := make([]request.TopicRequest, 0, topicsCount)

	for i := 0; i < topicsCount; i++ {
		if err := need(b, offset, 1, "describe: topic name length"); err != nil {
			return nil, err
		}
		nameLen := int(b[offset]) - 1
		offset++

		if nameLen < 0 {
			return nil, errors.New("describe: invalid topic name length")
		}
		if err := need(b, offset, nameLen, "describe: topic name"); err != nil {
			return nil, err
		}

		name := string(b[offset : offset+nameLen])
		offset += nameLen

		if err := need(b, offset, 1, "describe: topic tag buffer"); err != nil {
			return nil, err
		}
		offset++

		topics = append(topics, request.TopicRequest{Name: name})
	}

	if len(b) >= offset+4 {
		offset += 4
	}

	cursor := int8(-1)
	if len(b) >= offset+1 {
		cursor = int8(b[offset])
		offset++
	}

	return &request.DescribeTopicPartitionsRequest{
		Topics: topics,
		Cursor: cursor,
	}, nil
}

/* ================================ Fetch =================================== */

func parseFetchRequest(b []byte) (*request.FetchRequest, error) {
	offset := 0
	r := &request.FetchRequest{}

	if err := need(b, offset, 2, "fetch: client_id length"); err != nil {
		return nil, err
	}
	clientLen := int(binary.BigEndian.Uint16(b[offset:]))
	offset += 2

	if err := need(b, offset, clientLen, "fetch: client_id bytes"); err != nil {
		return nil, err
	}
	offset += clientLen

	if err := need(b, offset, 1, "fetch: header tag buffer"); err != nil {
		return nil, err
	}
	offset++

	offset += 4 // max_wait_ms
	offset += 4 // min_bytes
	offset += 4 // max_bytes
	offset++    // isolation_level
	offset += 4 // session_id
	offset += 4 // session_epoch

	topicsPlus1, err := readUvarintPayload(b, &offset)
	if err != nil {
		return nil, err
	}

	topicsCount := int(topicsPlus1) - 1
	if topicsCount <= 0 {
		return r, nil
	}

	if err := need(b, offset, 16, "fetch: topic_id"); err != nil {
		return nil, err
	}

	var id [16]byte
	copy(id[:], b[offset:offset+16])
	offset += 16

	r.Topics = append(r.Topics, request.FetchTopic{TopicID: id})
	return r, nil
}

/* ================================ Produce ================================= */

func parseProduceRequest(b []byte) (*request.ProduceRequest, error) {
	offset := 0
	r := &request.ProduceRequest{}

	if err := need(b, offset, 2, "produce: client_id length"); err != nil {
		return nil, err
	}
	clientLen := int(int16(binary.BigEndian.Uint16(b[offset:])))
	offset += 2

	if clientLen >= 0 {
		if err := need(b, offset, clientLen, "produce: client_id bytes"); err != nil {
			return nil, err
		}
		offset += clientLen
	}

	if _, err := skipTagBuffer(b, &offset); err != nil {
		return nil, err
	}

	if _, err := readCompactNullableString(b, &offset); err != nil {
		return nil, err
	}

	offset += 2 // acks
	offset += 4 // timeout_ms

	topicsPlus1, err := readUvarintPayload(b, &offset)
	if err != nil {
		return nil, err
	}
	topicsCount := int(topicsPlus1) - 1
	if topicsCount < 0 {
		return nil, errors.New("produce: invalid topics count")
	}

	for i := 0; i < topicsCount; i++ {
		name, err := readCompactString(b, &offset)
		if err != nil {
			return nil, err
		}

		topic := request.ProduceTopic{Name: name}

		partsPlus1, err := readUvarintPayload(b, &offset)
		if err != nil {
			return nil, err
		}
		partsCount := int(partsPlus1) - 1
		if partsCount < 0 {
			return nil, errors.New("produce: invalid partitions count")
		}

		for j := 0; j < partsCount; j++ {
			if err := need(b, offset, 4, "produce: partition index"); err != nil {
				return nil, err
			}
			idx := int32(binary.BigEndian.Uint32(b[offset:]))
			offset += 4

			records, err := readCompactBytes(b, &offset)
			if err != nil {
				return nil, err
			}

			if _, err := skipTagBuffer(b, &offset); err != nil {
				return nil, err
			}

			topic.Partitions = append(topic.Partitions, request.ProducePartition{
				Index:   idx,
				Records: records,
			})
		}

		if _, err := skipTagBuffer(b, &offset); err != nil {
			return nil, err
		}

		r.Topics = append(r.Topics, topic)
	}

	return r, nil
}

/* ================================ Helpers ================================= */

func need(b []byte, offset, size int, ctx string) error {
	if len(b) < offset+size {
		return errors.New(ctx + ": truncated")
	}
	return nil
}

func readUvarintPayload(b []byte, offset *int) (uint64, error) {
	v, n := binary.Uvarint(b[*offset:])
	if n <= 0 {
		return 0, errors.New("uvarint: decode error")
	}
	*offset += n
	return v, nil
}

func readCompactBytes(b []byte, offset *int) ([]byte, error) {
	lnPlus1, err := readUvarintPayload(b, offset)
	if err != nil || lnPlus1 == 0 {
		return nil, err
	}

	ln := int(lnPlus1) - 1
	if err := need(b, *offset, ln, "compact bytes"); err != nil {
		return nil, err
	}

	out := b[*offset : *offset+ln]
	*offset += ln
	return out, nil
}

func readCompactString(b []byte, offset *int) (string, error) {
	lnPlus1, err := readUvarintPayload(b, offset)
	if err != nil || lnPlus1 == 0 {
		return "", err
	}

	ln := int(lnPlus1) - 1
	if err := need(b, *offset, ln, "compact string"); err != nil {
		return "", err
	}

	s := string(b[*offset : *offset+ln])
	*offset += ln
	return s, nil
}

func readCompactNullableString(b []byte, offset *int) (*string, error) {
	lnPlus1, err := readUvarintPayload(b, offset)
	if err != nil || lnPlus1 == 0 {
		return nil, err
	}

	ln := int(lnPlus1) - 1
	if err := need(b, *offset, ln, "compact nullable string"); err != nil {
		return nil, err
	}

	s := string(b[*offset : *offset+ln])
	*offset += ln
	return &s, nil
}

func skipTagBuffer(b []byte, offset *int) (uint64, error) {
	count, err := readUvarintPayload(b, offset)
	if err != nil {
		return 0, errors.New("tag buffer: truncated")
	}

	for i := uint64(0); i < count; i++ {
		if _, err := readUvarintPayload(b, offset); err != nil {
			return 0, errors.New("tag id truncated")
		}
		size, err := readUvarintPayload(b, offset)
		if err != nil {
			return 0, errors.New("tag size truncated")
		}
		if err := need(b, *offset, int(size), "tag value"); err != nil {
			return 0, err
		}
		*offset += int(size)
	}

	return count, nil
}
