package usecase

import (
	"errors"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/internal/domain"
	"github.com/codecrafters-io/kafka-starter-go/internal/domain/request"
	"github.com/codecrafters-io/kafka-starter-go/internal/domain/response"
)

type fakeMetadataRepo struct {
	topicsByName map[string]*domain.TopicMetadata
	topicsByID   map[[16]byte]*domain.TopicMetadata
}

func (f *fakeMetadataRepo) GetTopic(name string) (*domain.TopicMetadata, error) {
	t, ok := f.topicsByName[name]
	if !ok {
		return nil, errors.New("not found")
	}
	return t, nil
}

func (f *fakeMetadataRepo) GetTopicByID(id [16]byte) (*domain.TopicMetadata, error) {
	t, ok := f.topicsByID[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return t, nil
}

type fakeLogManager struct {
	logs map[string][]byte
}

func (f *fakeLogManager) LoadLog(topic string, partition int32) ([]byte, error) {
	if v, ok := f.logs[topic]; ok {
		return v, nil
	}
	return nil, errors.New("not found")
}

func (f *fakeLogManager) AppendLog(topic string, partition int32, data []byte) error {
	f.logs[topic] = data
	return nil
}

func TestProcess_ApiVersions(t *testing.T) {
	p := NewRequestProcessor(&fakeMetadataRepo{}, &fakeLogManager{})

	req := &request.MessageRequest{
		Header: request.RequestHeader{
			ApiKey:        domain.ApiVersionApikey,
			ApiVersion:    0,
			CorrelationID: 1,
		},
		Body: &request.ApiVersionsRequest{},
	}

	resp, err := p.Process(req)
	if err != nil {
		t.Fatal(err)
	}

	if resp.CorrelationID != 1 {
		t.Fatal("wrong correlation id")
	}

	_, ok := resp.Body.(*response.ApiVersionsResponseBody)
	if !ok {
		t.Fatal("wrong body type")
	}
}

func TestProcess_DescribeTopicPartitions_UnknownTopic(t *testing.T) {
	p := NewRequestProcessor(&fakeMetadataRepo{}, &fakeLogManager{})

	req := &request.MessageRequest{
		Header: request.RequestHeader{CorrelationID: 2},
		Body: &request.DescribeTopicPartitionsRequest{
			Topics: []request.TopicRequest{{Name: "missing"}},
		},
	}

	resp, err := p.Process(req)
	if err != nil {
		t.Fatal(err)
	}

	body := resp.Body.(*response.DescribeTopicPartitionsResponseBody)
	if body.Topics[0].ErrorCode != domain.ErrorUnknownTopicOrPartition {
		t.Fatal("expected unknown topic error")
	}
}

func TestProcess_DescribeTopicPartitions_ExistingTopic(t *testing.T) {
	var id [16]byte
	id[0] = 1

	meta := &domain.TopicMetadata{
		Name:    "test",
		TopicID: id,
		Partitions: []domain.PartitionMetadata{
			{
				PartitionIndex: 0,
				LeaderID:       1,
				LeaderEpoch:    0,
				Replicas:       []int32{1},
				ISR:            []int32{1},
			},
		},
	}

	repo := &fakeMetadataRepo{
		topicsByName: map[string]*domain.TopicMetadata{"test": meta},
	}

	p := NewRequestProcessor(repo, &fakeLogManager{})

	req := &request.MessageRequest{
		Header: request.RequestHeader{CorrelationID: 3},
		Body: &request.DescribeTopicPartitionsRequest{
			Topics: []request.TopicRequest{{Name: "test"}},
		},
	}

	resp, err := p.Process(req)
	if err != nil {
		t.Fatal(err)
	}

	body := resp.Body.(*response.DescribeTopicPartitionsResponseBody)
	if len(body.Topics[0].Partitions) != 1 {
		t.Fatal("expected 1 partition")
	}
}

func TestProcess_Fetch_UnknownTopicID(t *testing.T) {
	p := NewRequestProcessor(&fakeMetadataRepo{}, &fakeLogManager{})

	var id [16]byte
	id[0] = 9

	req := &request.MessageRequest{
		Header: request.RequestHeader{CorrelationID: 4},
		Body: &request.FetchRequest{
			Topics: []request.FetchTopic{{TopicID: id}},
		},
	}

	resp, err := p.Process(req)
	if err != nil {
		t.Fatal(err)
	}

	body := resp.Body.(*response.FetchResponseBody)
	if body.Responses[0].Partitions[0].ErrorCode != domain.ErrorUnknownTopicId {
		t.Fatal("expected unknown topic id error")
	}
}

func TestProcess_Fetch_WithLog(t *testing.T) {
	var id [16]byte
	id[0] = 7

	meta := &domain.TopicMetadata{
		Name:    "test",
		TopicID: id,
	}

	repo := &fakeMetadataRepo{
		topicsByID: map[[16]byte]*domain.TopicMetadata{id: meta},
	}

	logs := &fakeLogManager{
		logs: map[string][]byte{"test": {0x01, 0x02}},
	}

	p := NewRequestProcessor(repo, logs)

	req := &request.MessageRequest{
		Header: request.RequestHeader{CorrelationID: 5},
		Body: &request.FetchRequest{
			Topics: []request.FetchTopic{{TopicID: id}},
		},
	}

	resp, err := p.Process(req)
	if err != nil {
		t.Fatal(err)
	}

	body := resp.Body.(*response.FetchResponseBody)
	if len(body.Responses[0].Partitions[0].Records) == 0 {
		t.Fatal("expected records")
	}
}

func TestProcess_Produce(t *testing.T) {
	meta := &domain.TopicMetadata{
		Name: "test",
		Partitions: []domain.PartitionMetadata{
			{PartitionIndex: 0},
		},
	}

	repo := &fakeMetadataRepo{
		topicsByName: map[string]*domain.TopicMetadata{"test": meta},
	}

	logs := &fakeLogManager{logs: map[string][]byte{}}

	p := NewRequestProcessor(repo, logs)

	req := &request.MessageRequest{
		Header: request.RequestHeader{CorrelationID: 6},
		Body: &request.ProduceRequest{
			Topics: []request.ProduceTopic{
				{
					Name: "test",
					Partitions: []request.ProducePartition{
						{Index: 0, Records: []byte{0x01}},
					},
				},
			},
		},
	}

	resp, err := p.Process(req)
	if err != nil {
		t.Fatal(err)
	}

	body := resp.Body.(*response.ProduceResponseBody)
	if body.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatal("expected success")
	}
}
