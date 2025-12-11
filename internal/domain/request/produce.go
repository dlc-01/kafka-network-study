package request

import "github.com/codecrafters-io/kafka-starter-go/internal/domain"

type ProduceRequest struct {
}

func (*ProduceRequest) ApiKey() uint16 { return domain.ProduceApiKey }
