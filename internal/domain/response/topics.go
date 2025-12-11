package response

import "github.com/codecrafters-io/kafka-starter-go/internal/domain"

type DescribeTopicPartitionsResponseBody struct {
	ThrottleTime uint32
	Topics       []TopicDescription
	NextCursor   int8
}

func (b *DescribeTopicPartitionsResponseBody) ApiKey() uint16 {
	return domain.DescribeTopicPartitionsApikey
}

type TopicDescription struct {
	ErrorCode    uint16
	Name         string
	TopicID      [16]byte
	IsInternal   bool
	Partitions   []PartitionDescription
	AuthorizedOp uint32
}

type PartitionDescription struct {
	ErrorCode       uint16
	PartitionIndex  int32
	LeaderID        int32
	LeaderEpoch     int32
	Replicas        []int32
	ISR             []int32
	EligibleLeaders []int32
	LastKnownELR    []int32
	OfflineReplicas []int32
}
