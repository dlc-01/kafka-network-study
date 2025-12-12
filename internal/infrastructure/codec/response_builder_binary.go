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
	case *response.FetchResponseBody:
		return b.buildFetch(resp.CorrelationID, body)
	case *response.ProduceResponseBody:
		return b.buildProduce(resp.CorrelationID, body)
	default:
		return nil, errors.New("unsupported response body type")
	}
}

func (b *BinaryResponseBuilder) buildApiVersions(
	correlationID uint32,
	body *response.ApiVersionsResponseBody,
) ([]byte, error) {

	header := make([]byte, 0)
	header = appendUint32(header, correlationID)

	out := make([]byte, 0)
	out = appendUint16(out, body.ErrorCode)

	out = append(out, byte(len(body.ApiKeys)+1))

	for _, k := range body.ApiKeys {
		out = appendUint16(out, k.ApiKey)
		out = appendUint16(out, k.MinVersion)
		out = appendUint16(out, k.MaxVersion)
		out = append(out, 0)
	}

	out = appendUint32(out, body.ThrottleTime)
	out = append(out, 0)

	payload := append(header, out...)
	return wrapWithSize(payload), nil
}

func (b *BinaryResponseBuilder) buildDescribeTopicPartitions(
	correlationID uint32,
	body *response.DescribeTopicPartitionsResponseBody,
) ([]byte, error) {

	header := make([]byte, 0)
	header = appendUint32(header, correlationID)
	header = append(header, 0)

	out := make([]byte, 0)

	out = appendUint32(out, body.ThrottleTime)
	out = append(out, byte(len(body.Topics)+1))

	for _, t := range body.Topics {
		out = appendUint16(out, t.ErrorCode)

		out = append(out, byte(len(t.Name)+1))
		out = append(out, t.Name...)
		out = append(out, t.TopicID[:]...)

		if t.IsInternal {
			out = append(out, 1)
		} else {
			out = append(out, 0)
		}

		out = append(out, byte(len(t.Partitions)+1))

		for _, p := range t.Partitions {
			out = appendUint16(out, p.ErrorCode)
			out = appendInt32(out, p.PartitionIndex)
			out = appendInt32(out, p.LeaderID)
			out = appendInt32(out, p.LeaderEpoch)

			out = append(out, byte(len(p.Replicas)+1))
			for _, r := range p.Replicas {
				out = appendInt32(out, r)
			}

			out = append(out, byte(len(p.ISR)+1))
			for _, r := range p.ISR {
				out = appendInt32(out, r)
			}

			out = append(out, 1)
			out = append(out, 1)
			out = append(out, 1)
			out = append(out, 0)
		}

		out = appendUint32(out, t.AuthorizedOp)
		out = append(out, 0)
	}

	out = append(out, 0xff)
	out = append(out, 0)

	payload := append(header, out...)
	return wrapWithSize(payload), nil
}

func (b *BinaryResponseBuilder) buildFetch(
	correlationID uint32,
	body *response.FetchResponseBody,
) ([]byte, error) {

	header := make([]byte, 0)
	header = appendUint32(header, correlationID)
	header = append(header, 0)

	out := make([]byte, 0)

	out = appendInt32(out, body.ThrottleTimeMs)
	out = appendInt16(out, body.ErrorCode)
	out = appendInt32(out, body.SessionID)

	out = appendUvarint(out, uint64(len(body.Responses)+1))

	for _, resp := range body.Responses {
		out = append(out, resp.TopicID[:]...)

		out = appendUvarint(out, uint64(len(resp.Partitions)+1))

		for _, p := range resp.Partitions {
			out = appendInt32(out, p.PartitionIndex)
			out = appendInt16(out, p.ErrorCode)
			out = appendInt64(out, p.HighWatermark)
			out = appendInt64(out, p.LastStableOffset)
			out = appendInt64(out, p.LogStartOffset)

			out = appendUvarint(out, 1)
			out = appendInt32(out, -1)

			if len(p.Records) == 0 {
				out = appendUvarint(out, 1)
			} else {
				out = appendUvarint(out, uint64(len(p.Records)+1))
				out = append(out, p.Records...)
			}

			out = append(out, 0)
		}

		out = append(out, 0)
	}

	out = append(out, 0)

	payload := append(header, out...)
	return wrapWithSize(payload), nil
}

func (b *BinaryResponseBuilder) buildProduce(
	correlationID uint32,
	body *response.ProduceResponseBody,
) ([]byte, error) {

	header := make([]byte, 0)
	header = appendUint32(header, correlationID)
	header = append(header, 0)

	out := make([]byte, 0)

	out = appendUvarint(out, uint64(len(body.Topics)+1))

	for _, t := range body.Topics {
		out = appendUvarint(out, uint64(len(t.Name)+1))
		out = append(out, t.Name...)

		out = appendUvarint(out, uint64(len(t.Partitions)+1))

		for _, p := range t.Partitions {
			out = appendInt32(out, p.Index)
			out = appendInt16(out, p.ErrorCode)
			out = appendInt64(out, p.BaseOffset)
			out = appendInt64(out, p.LogAppendTimeMs)
			out = appendInt64(out, p.LogStartOffset)

			out = appendUvarint(out, 0)
			out = appendUvarint(out, 0)
			out = appendUvarint(out, 0)
		}

		out = appendUvarint(out, 0)
	}

	out = appendInt32(out, body.ThrottleTimeMs)

	out = appendUvarint(out, 0)

	payload := append(header, out...)
	return wrapWithSize(payload), nil
}

func wrapWithSize(payload []byte) []byte {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(payload)))
	return append(size[:], payload...)
}

func appendUint16(buf []byte, v uint16) []byte {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], v)
	return append(buf, tmp[:]...)
}

func appendUint32(buf []byte, v uint32) []byte {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	return append(buf, tmp[:]...)
}

func appendUint64(buf []byte, v uint64) []byte {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	return append(buf, tmp[:]...)
}

func appendUvarint(buf []byte, v uint64) []byte {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}

func appendInt16(buf []byte, v int16) []byte {
	return appendUint16(buf, uint16(v))
}

func appendInt32(buf []byte, v int32) []byte {
	return appendUint32(buf, uint32(v))
}

func appendInt64(buf []byte, v int64) []byte {
	return appendUint64(buf, uint64(v))
}
