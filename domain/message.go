package domain

import "encoding/binary"

type Message struct {
	Size   uint32
	Header Header
}

func (m *Message) ToBytes() []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, m.Size)
	return append(b, m.Header.ToBytes()...)
}
