package request

import "github.com/codecrafters-io/kafka-starter-go/internal/domain"

type FetchRequest struct {
}

func (*FetchRequest) ApiKey() uint16 { return domain.FetchApikey }
