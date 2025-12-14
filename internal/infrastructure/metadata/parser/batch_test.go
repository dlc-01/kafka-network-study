package parser

import (
	"encoding/binary"
	"testing"
)

func TestParseRecordBatch_BufferTooSmall(t *testing.T) {
	_, _, err := parseRecordBatch([]byte{0x00, 0x01})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseRecordBatch_NegativeRecordsLength(t *testing.T) {
	buf := make([]byte, 8+4+4+1+4+2+4+8+8+8+2+4+4)

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
	binary.BigEndian.PutUint32(buf[57:61], uint32(0xffffffff))

	_, _, err := parseRecordBatch(buf)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseRecordBatch_NoRecords(t *testing.T) {
	buf := make([]byte, 8+4+4+1+4+2+4+8+8+8+2+4+4)

	binary.BigEndian.PutUint64(buf[0:8], 1)
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

	rb, n, err := parseRecordBatch(buf)
	if err != nil {
		t.Fatal(err)
	}

	if rb.RecordsLength != 0 {
		t.Fatal("expected zero records")
	}

	if n != len(buf) {
		t.Fatal("wrong bytes read")
	}
}
