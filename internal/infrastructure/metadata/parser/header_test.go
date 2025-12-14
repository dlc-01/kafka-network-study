package parser

import "testing"

func TestParseRecordHeader_OK(t *testing.T) {
	b := []byte{0x01, 0x02}

	h, err := parseRecordHeader(b)
	if err != nil {
		t.Fatal(err)
	}

	if h.frameVersion != 0x01 {
		t.Fatal("wrong frameVersion")
	}

	if h.GetRecordTypeId() != 0x02 {
		t.Fatal("wrong record type id")
	}
}

func TestParseRecordHeader_BufferTooSmall(t *testing.T) {
	_, err := parseRecordHeader([]byte{0x01})
	if err == nil {
		t.Fatal("expected error")
	}
}
