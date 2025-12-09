package response

import "encoding/binary"

type Message struct {
	size   uint32
	header Header
}

func NewMessage(header Header) *Message {
	size := uint32(len(header.ToBytes()))
	return &Message{size: size, header: header}
}

func (m *Message) ToBytes() []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, m.size)
	return append(b, m.header.ToBytes()...)
}
