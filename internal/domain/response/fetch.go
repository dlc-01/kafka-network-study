package response

import "github.com/codecrafters-io/kafka-starter-go/internal/domain"

type FetchResponseBody struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionID      int32
	Responses      []FetchTopicResponse
}

func (r *FetchResponseBody) ApiKey() uint16 {
	return domain.FetchApikey
}

type FetchTopicResponse struct {
	Topic      string
	Partitions []FetchPartitionResponse
}

type FetchPartitionResponse struct {
	PartitionIndex   int32
	ErrorCode        int16
	HighWatermark    int64
	LastStableOffset int64
	LogStartOffset   int64
	Records          []byte
}
