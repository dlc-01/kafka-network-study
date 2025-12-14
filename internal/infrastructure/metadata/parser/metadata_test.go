package parser

import (
	"encoding/binary"
	"testing"
)

func makeEmptyRecordBatch() []byte {
	buf := make([]byte, 61)

	binary.BigEndian.PutUint64(buf[0:8], 0)
	binary.BigEndian.PutUint32(buf[8:12], 0)
	binary.BigEndian.PutUint32(buf[12:16], 0)
	buf[16] = 2
	binary.BigEndian.PutUint32(buf[17:21], 0)
	binary.BigEndian.PutUint16(buf[21:23], 0)
	binary.BigEndian.PutUint32(buf[23:27], 0)
	binary.BigEndian.PutUint64(buf[27:35], 0)
	binary.BigEndian.PutUint64(buf[35:43], 0)
	binary.BigEndian.PutUint64(buf[43:51], 0)
	binary.BigEndian.PutUint16(buf[51:53], 0)
	binary.BigEndian.PutUint32(buf[53:57], 0)
	binary.BigEndian.PutUint32(buf[57:61], 0)

	return buf
}

func TestParseMetadata_Empty(t *testing.T) {
	res, err := Decode([]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 0 {
		t.Fatal("expected empty result")
	}
}

func TestParseMetadata_SingleBatch(t *testing.T) {
	raw := makeEmptyRecordBatch()

	batches, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}

	if len(batches) != 1 {
		t.Fatal("expected one batch")
	}

	if len(batches[0].Records) != 0 {
		t.Fatal("expected no records")
	}
}

func TestParseMetadata_MultipleBatches(t *testing.T) {
	raw := append(makeEmptyRecordBatch(), makeEmptyRecordBatch()...)

	batches, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}

	if len(batches) != 2 {
		t.Fatal("expected two batches")
	}
}

func TestParseMetadata_InvalidBatchSize(t *testing.T) {
	raw := makeEmptyRecordBatch()
	raw = raw[:len(raw)-1]

	_, err := Decode(raw)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseMetadata_Garbage(t *testing.T) {
	_, err := Decode([]byte{0x01, 0x02, 0x03})
	if err == nil {
		t.Fatal("expected error")
	}
}
