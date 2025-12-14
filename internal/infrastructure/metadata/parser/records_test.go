package parser

import (
	"encoding/binary"
	"testing"
)

func TestNewRecordFeatureLevel_OK(t *testing.T) {
	h := recordHeader{frameVersion: featureLevelRecordType}

	buf := []byte{}
	buf = append(buf, 1)
	buf = append(buf, byte(len("test")+1))
	buf = append(buf, []byte("test")...)
	buf = append(buf, 0x00, 0x01)
	buf = append(buf, 0x00)

	r, err := newRecordFeatureLevel(h, buf)
	if err != nil {
		t.Fatal(err)
	}

	if r.name != "test" {
		t.Fatal("wrong name")
	}
}

func TestNewRecordFeatureLevel_BufferTooSmall(t *testing.T) {
	h := recordHeader{frameVersion: featureLevelRecordType}
	_, err := newRecordFeatureLevel(h, []byte{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewRecordTopic_OK(t *testing.T) {
	h := recordHeader{frameVersion: topicRecordType}

	buf := []byte{}
	buf = append(buf, 1)
	buf = append(buf, byte(len("topic")+1))
	buf = append(buf, []byte("topic")...)

	var uuid [16]byte
	for i := range uuid {
		uuid[i] = byte(i)
	}
	buf = append(buf, uuid[:]...)
	buf = append(buf, 0x00)

	r, err := newRecordTopic(h, buf)
	if err != nil {
		t.Fatal(err)
	}

	if r.TopicName != "topic" {
		t.Fatal("wrong topic name")
	}
}

func TestNewRecordTopic_BufferTooSmall(t *testing.T) {
	h := recordHeader{frameVersion: topicRecordType}
	_, err := newRecordTopic(h, []byte{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewRecordPartition_OK(t *testing.T) {
	h := recordHeader{frameVersion: partitionRecordType}

	buf := []byte{}
	buf = append(buf, 1)

	binary.BigEndian.PutUint32(append(buf, 0, 0, 0, 0), 0)
	buf = buf[:len(buf)+4]
	binary.BigEndian.PutUint32(buf[len(buf)-4:], 1)

	var uuid [16]byte
	for i := range uuid {
		uuid[i] = byte(i)
	}
	buf = append(buf, uuid[:]...)

	buf = append(buf, 1)
	buf = append(buf, 1)
	buf = append(buf, 1)
	buf = append(buf, 1)

	buf = append(buf, 0x00, 0x00, 0x00, 0x01)
	buf = append(buf, 0x00, 0x00, 0x00, 0x01)
	buf = append(buf, 0x00, 0x00, 0x00, 0x01)

	buf = append(buf, 1)
	buf = append(buf, 0x00)

	r, err := newRecordPartition(h, buf)
	if err != nil {
		t.Fatal(err)
	}

	if r.PartitionID != 1 {
		t.Fatal("wrong partition id")
	}
}

func TestNewRecordPartition_BufferTooSmall(t *testing.T) {
	h := recordHeader{frameVersion: partitionRecordType}
	_, err := newRecordPartition(h, []byte{})
	if err == nil {
		t.Fatal("expected error")
	}
}
