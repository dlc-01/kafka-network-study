package repository

import (
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/internal/infrastructure/metadata/parser"
)

func TestBuildDomainTopics_TopicOnly(t *testing.T) {
	var uuid [16]byte
	uuid[0] = 1

	batches := []parser.RecordBatch{
		{
			Records: []parser.Record{
				{
					Value: parser.RecordTopic{
						TopicName: "test",
						TopicUUID: uuid,
					},
				},
			},
		},
	}

	res := buildDomainTopics(batches)

	if len(res.ByName) != 1 {
		t.Fatal("expected 1 topic")
	}

	tm, ok := res.ByName["test"]
	if !ok {
		t.Fatal("topic not found by name")
	}

	if tm.TopicID != uuid {
		t.Fatal("wrong topic uuid")
	}

	if len(tm.Partitions) != 0 {
		t.Fatal("expected no partitions")
	}
}

func TestBuildDomainTopics_TopicAndPartition(t *testing.T) {
	var uuid [16]byte
	uuid[0] = 2

	batches := []parser.RecordBatch{
		{
			Records: []parser.Record{
				{
					Value: parser.RecordTopic{
						TopicName: "topic",
						TopicUUID: uuid,
					},
				},
				{
					Value: parser.RecordPartition{
						PartitionID: 0,
						TopicUUID:   uuid,
						Leader:      1,
						LeaderEpoch: 2,
						ReplicaArray: []int32{
							1, 2,
						},
						SyncReplicaArray: []int32{
							1,
						},
					},
				},
			},
		},
	}

	res := buildDomainTopics(batches)

	tm := res.ByName["topic"]
	if tm == nil {
		t.Fatal("topic not found")
	}

	if len(tm.Partitions) != 1 {
		t.Fatal("expected 1 partition")
	}

	pm := tm.Partitions[0]
	if pm.PartitionIndex != 0 {
		t.Fatal("wrong partition index")
	}
	if pm.LeaderID != 1 {
		t.Fatal("wrong leader id")
	}
	if pm.LeaderEpoch != 2 {
		t.Fatal("wrong leader epoch")
	}

	if len(pm.Replicas) != 2 {
		t.Fatal("wrong replicas")
	}
	if len(pm.ISR) != 1 {
		t.Fatal("wrong isr")
	}
}

func TestBuildDomainTopics_PartitionBeforeTopic(t *testing.T) {
	var uuid [16]byte
	uuid[0] = 3

	batches := []parser.RecordBatch{
		{
			Records: []parser.Record{
				{
					Value: parser.RecordPartition{
						PartitionID: 1,
						TopicUUID:   uuid,
						Leader:      5,
						LeaderEpoch: 1,
					},
				},
				{
					Value: parser.RecordTopic{
						TopicName: "late",
						TopicUUID: uuid,
					},
				},
			},
		},
	}

	res := buildDomainTopics(batches)

	tm := res.ByName["late"]
	if tm == nil {
		t.Fatal("topic not found")
	}

	if len(tm.Partitions) != 1 {
		t.Fatal("expected partition to be preserved")
	}

	if tm.Partitions[0].PartitionIndex != 1 {
		t.Fatal("wrong partition index")
	}
}

func TestBuildDomainTopics_MultipleBatches(t *testing.T) {
	var uuid [16]byte
	uuid[0] = 4

	batches := []parser.RecordBatch{
		{
			Records: []parser.Record{
				{
					Value: parser.RecordTopic{
						TopicName: "a",
						TopicUUID: uuid,
					},
				},
			},
		},
		{
			Records: []parser.Record{
				{
					Value: parser.RecordPartition{
						PartitionID: 0,
						TopicUUID:   uuid,
						Leader:      1,
					},
				},
			},
		},
	}

	res := buildDomainTopics(batches)

	tm := res.ByName["a"]
	if tm == nil {
		t.Fatal("topic not found")
	}

	if len(tm.Partitions) != 1 {
		t.Fatal("expected partition from second batch")
	}
}

func TestBuildDomainTopics_UnknownRecordIgnored(t *testing.T) {
	batches := []parser.RecordBatch{
		{
			Records: []parser.Record{
				{Value: nil},
			},
		},
	}

	res := buildDomainTopics(batches)

	if len(res.ByName) != 0 {
		t.Fatal("expected no topics")
	}
}
