package parser

import "testing"

func TestParseRecord_BufferTooSmall(t *testing.T) {
	_, _, err := parseRecord([]byte{0x00})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseRecord_NegativeValueLength(t *testing.T) {
	buf := []byte{}

	buf = append(buf, 0x00)
	buf = append(buf, 0x00)
	buf = append(buf, 0x00)

	buf = append(buf, 0x01)

	buf = append(buf, 0x00)
	buf = append(buf, 0x00)

	buf = append(buf, 0x01)
	buf = append(buf, 0x01)

	buf = append(buf, 0x01)
	buf = append(buf, 0x01)

	buf = append(buf, 0x01)
	buf = append(buf, 0x01)

	buf = append(buf, 0x01)

	_, _, err := parseRecord(buf)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseRecord_EmptyValue_Error(t *testing.T) {
	buf := []byte{}

	buf = append(buf, 0x00)
	buf = append(buf, 0x00)
	buf = append(buf, 0x00)
	buf = append(buf, 0x00)
	buf = append(buf, 0x01)
	buf = append(buf, 0x00)
	buf = append(buf, 0x00)

	buf = append(buf, 0x00, 0x00)

	_, _, err := parseRecord(buf)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "recordValue: buffer too small" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseRecordValue_BufferTooSmall(t *testing.T) {
	_, err := parseRecordValue([]byte{0x00})
	if err == nil {
		t.Fatal("expected error")
	}
}
