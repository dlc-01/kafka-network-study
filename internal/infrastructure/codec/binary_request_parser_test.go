package codec

import (
	"encoding/binary"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/internal/domain"
	"github.com/codecrafters-io/kafka-starter-go/internal/domain/request"
)

func uvarint(v uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	return buf[:n]
}

func compactString(s string) []byte {
	b := []byte{}
	b = append(b, uvarint(uint64(len(s)+1))...)
	b = append(b, []byte(s)...)
	return b
}

func compactBytes(bts []byte) []byte {
	b := []byte{}
	b = append(b, uvarint(uint64(len(bts)+1))...)
	b = append(b, bts...)
	return b
}

func emptyTagBuffer() []byte {
	return uvarint(0)
}

func TestParse_ShortBuffer(t *testing.T) {
	p := NewBinaryRequestParser()
	_, err := p.Parse([]byte{0x00, 0x01})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParse_ApiVersions(t *testing.T) {
	p := NewBinaryRequestParser()

	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], 8)
	binary.BigEndian.PutUint16(buf[4:6], domain.ApiVersionApikey)
	binary.BigEndian.PutUint16(buf[6:8], 4)
	binary.BigEndian.PutUint32(buf[8:12], 42)

	req, err := p.Parse(buf)
	if err != nil {
		t.Fatal(err)
	}

	if req.Header.ApiKey != domain.ApiVersionApikey {
		t.Fatal("wrong api key")
	}

	if req.Header.CorrelationID != 42 {
		t.Fatal("wrong correlation id")
	}
}

func TestParse_DescribeTopicPartitions(t *testing.T) {
	p := NewBinaryRequestParser()

	payload := []byte{}
	payload = append(payload, 0x00, 0x00)
	payload = append(payload, emptyTagBuffer()...)
	payload = append(payload, byte(2))
	payload = append(payload, byte(len("test")+1))
	payload = append(payload, []byte("test")...)
	payload = append(payload, emptyTagBuffer()...)
	payload = append(payload, 0xFF)

	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint16(buf[4:6], domain.DescribeTopicPartitionsApikey)
	binary.BigEndian.PutUint16(buf[6:8], 0)
	binary.BigEndian.PutUint32(buf[8:12], 1)
	buf = append(buf, payload...)

	req, err := p.Parse(buf)
	if err != nil {
		t.Fatal(err)
	}

	body := req.Body.(*request.DescribeTopicPartitionsRequest)
	if len(body.Topics) != 1 {
		t.Fatal("expected 1 topic")
	}

	if body.Topics[0].Name != "test" {
		t.Fatal("wrong topic name")
	}
}

func TestParse_Fetch(t *testing.T) {
	p := NewBinaryRequestParser()

	payload := []byte{}
	payload = append(payload, 0x00, 0x00)
	payload = append(payload, emptyTagBuffer()...)
	payload = append(payload, make([]byte, 4+4+4+1+4+4)...)
	payload = append(payload, uvarint(2)...)

	var id [16]byte
	for i := range id {
		id[i] = byte(i)
	}
	payload = append(payload, id[:]...)

	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint16(buf[4:6], domain.FetchApikey)
	binary.BigEndian.PutUint16(buf[6:8], 0)
	binary.BigEndian.PutUint32(buf[8:12], 7)
	buf = append(buf, payload...)

	req, err := p.Parse(buf)
	if err != nil {
		t.Fatal(err)
	}

	fetch := req.Body.(*request.FetchRequest)
	if len(fetch.Topics) != 1 {
		t.Fatal("expected 1 topic")
	}
}

func TestParse_Produce(t *testing.T) {
	p := NewBinaryRequestParser()

	payload := []byte{}
	payload = append(payload, 0x00, 0x00)
	payload = append(payload, emptyTagBuffer()...)
	payload = append(payload, uvarint(1)...)
	payload = append(payload, 0x00, 0x00)
	payload = append(payload, 0x00, 0x00, 0x00, 0x01)
	payload = append(payload, uvarint(2)...)
	payload = append(payload, compactString("test")...)
	payload = append(payload, uvarint(2)...)
	payload = append(payload, 0x00, 0x00, 0x00, 0x00)
	payload = append(payload, compactBytes([]byte{0x01, 0x02})...)
	payload = append(payload, emptyTagBuffer()...)
	payload = append(payload, emptyTagBuffer()...)

	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint16(buf[4:6], domain.ProduceApiKey)
	binary.BigEndian.PutUint16(buf[6:8], 9)
	binary.BigEndian.PutUint32(buf[8:12], 99)
	buf = append(buf, payload...)

	req, err := p.Parse(buf)
	if err != nil {
		t.Fatal(err)
	}

	prod := req.Body.(*request.ProduceRequest)
	if len(prod.Topics) != 1 {
		t.Fatal("expected 1 topic")
	}
}
