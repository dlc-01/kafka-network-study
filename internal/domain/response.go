package domain

type MessageResponse struct {
	CorrelationID           uint32
	ErrorCode               uint16
	ApiKeys                 []ApiKey
	DescribeTopicPartitions DescribeTopicPartitions
	ThrottleTime            uint32
}
