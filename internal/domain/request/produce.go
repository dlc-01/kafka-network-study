package request

import "github.com/codecrafters-io/kafka-starter-go/internal/domain"

type ProduceRequest struct {
	Topics []ProduceTopic
}

func (p *ProduceRequest) ApiKey() uint16 {
	return domain.ProduceApiKey
}

type ProduceTopic struct {
	Name       string
	Partitions []ProducePartition
}

type ProducePartition struct {
	Index   int32
	Records []byte
}
