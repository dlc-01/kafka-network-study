package response

import "github.com/codecrafters-io/kafka-starter-go/internal/domain"

type ProduceResponseBody struct {
	ThrottleTimeMs int32
	Topics         []ProduceTopicResponse
}

func (b *ProduceResponseBody) ApiKey() uint16 { return domain.ProduceApiKey }

type ProduceTopicResponse struct {
	Name       string
	Partitions []ProducePartitionResponse
}

type ProducePartitionResponse struct {
	Index           int32
	ErrorCode       int16
	BaseOffset      int64
	LogAppendTimeMs int64
	LogStartOffset  int64
}
