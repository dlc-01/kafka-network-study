package storage

import (
	"fmt"
	"os"
)

type LogManager struct {
	base string
}

func NewLogManager(base string) *LogManager {
	return &LogManager{base: base}
}

func (m *LogManager) LoadLog(topicName string, partition int32) ([]byte, error) {
	path := fmt.Sprintf("%s/%s-%d/00000000000000000000.log",
		m.base, topicName, partition)

	return os.ReadFile(path)
}
