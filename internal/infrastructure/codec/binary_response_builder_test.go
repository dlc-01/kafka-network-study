package codec

import (
	"encoding/binary"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/internal/domain/response"
)

func TestBuild_NilBody(t *testing.T) {
	b := NewBinaryResponseBuilder()
	_, err := b.Build(&response.MessageResponse{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestBuild_ApiVersions(t *testing.T) {
	b := NewBinaryResponseBuilder()

	resp := &response.MessageResponse{
		CorrelationID: 42,
		Body: &response.ApiVersionsResponseBody{
			ErrorCode: 0,
			ApiKeys: []response.ApiKeyResponse{
				{ApiKey: 18, MinVersion: 0, MaxVersion: 4},
			},
			ThrottleTime: 0,
		},
	}

	out, err := b.Build(resp)
	if err != nil {
		t.Fatal(err)
	}

	size := binary.BigEndian.Uint32(out[:4])
	if int(size) != len(out)-4 {
		t.Fatal("invalid size")
	}

	corr := binary.BigEndian.Uint32(out[4:8])
	if corr != 42 {
		t.Fatal("wrong correlation id")
	}
}

func TestBuild_DescribeTopicPartitions(t *testing.T) {
	b := NewBinaryResponseBuilder()

	var topicID [16]byte
	for i := range topicID {
		topicID[i] = byte(i)
	}

	resp := &response.MessageResponse{
		CorrelationID: 1,
		Body: &response.DescribeTopicPartitionsResponseBody{
			ThrottleTime: 0,
			NextCursor:   -1,
			Topics: []response.TopicDescription{
				{
					ErrorCode:  0,
					Name:       "test",
					TopicID:    topicID,
					IsInternal: false,
					Partitions: []response.PartitionDescription{
						{
							ErrorCode:      0,
							PartitionIndex: 0,
							LeaderID:       1,
							LeaderEpoch:    0,
							Replicas:       []int32{1},
							ISR:            []int32{1},
						},
					},
					AuthorizedOp: 0,
				},
			},
		},
	}

	out, err := b.Build(resp)
	if err != nil {
		t.Fatal(err)
	}

	size := binary.BigEndian.Uint32(out[:4])
	if int(size) != len(out)-4 {
		t.Fatal("invalid size")
	}

	corr := binary.BigEndian.Uint32(out[4:8])
	if corr != 1 {
		t.Fatal("wrong correlation id")
	}
}

func TestBuild_Fetch(t *testing.T) {
	b := NewBinaryResponseBuilder()

	var topicID [16]byte
	for i := range topicID {
		topicID[i] = byte(i)
	}

	resp := &response.MessageResponse{
		CorrelationID: 7,
		Body: &response.FetchResponseBody{
			ThrottleTimeMs: 0,
			ErrorCode:      0,
			SessionID:      0,
			Responses: []response.FetchTopicResponse{
				{
					TopicID: topicID,
					Partitions: []response.FetchPartitionResponse{
						{
							PartitionIndex:   0,
							ErrorCode:        0,
							HighWatermark:    0,
							LastStableOffset: 0,
							LogStartOffset:   0,
							Records:          []byte{},
						},
					},
				},
			},
		},
	}

	out, err := b.Build(resp)
	if err != nil {
		t.Fatal(err)
	}

	size := binary.BigEndian.Uint32(out[:4])
	if int(size) != len(out)-4 {
		t.Fatal("invalid size")
	}
}

func TestBuild_Produce(t *testing.T) {
	b := NewBinaryResponseBuilder()

	resp := &response.MessageResponse{
		CorrelationID: 99,
		Body: &response.ProduceResponseBody{
			ThrottleTimeMs: 0,
			Topics: []response.ProduceTopicResponse{
				{
					Name: "test",
					Partitions: []response.ProducePartitionResponse{
						{
							Index:           0,
							ErrorCode:       0,
							BaseOffset:      0,
							LogAppendTimeMs: 0,
							LogStartOffset:  0,
						},
					},
				},
			},
		},
	}

	out, err := b.Build(resp)
	if err != nil {
		t.Fatal(err)
	}

	size := binary.BigEndian.Uint32(out[:4])
	if int(size) != len(out)-4 {
		t.Fatal("invalid size")
	}
}
