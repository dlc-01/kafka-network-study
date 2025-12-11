package request

import "github.com/codecrafters-io/kafka-starter-go/internal/domain"

type FetchRequest struct {
	Topics []FetchTopic
}

type FetchTopic struct {
	TopicID [16]byte
}

func (*FetchRequest) ApiKey() uint16 { return domain.FetchApikey }
