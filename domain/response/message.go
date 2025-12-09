package response

import "encoding/binary"

type MessageResponse struct {
	size   uint32
	header HeaderResponse
}

func NewMessageResponse(header HeaderResponse) *MessageResponse {
	size := uint32(len(header.ToBytes()))
	return &MessageResponse{size: size, header: header}
}

func (m *MessageResponse) ToBytes() []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, m.size)
	return append(b, m.header.ToBytes()...)
}
