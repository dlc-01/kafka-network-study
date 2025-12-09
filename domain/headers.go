package domain

import "encoding/binary"

type Header struct {
	CorrelationID uint32
}

func (h *Header) ToBytes() []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, h.CorrelationID)
	return b
}
