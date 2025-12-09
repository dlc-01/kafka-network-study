package response

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/domain/request"
)

type Header struct {
	CorrelationID uint32
}

func (h *Header) ToBytes() []byte {
	correlationID := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationID, h.CorrelationID)
	return correlationID
}

func ParseRequestHeader(headerReq *request.Header) Header {
	return Header{
		CorrelationID: headerReq.CorrelationID,
	}
}
