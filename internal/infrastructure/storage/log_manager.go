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
	path := m.logPath(topicName, partition)

	return os.ReadFile(path)
}
func (m *LogManager) logPath(topic string, partition int32) string {
	return fmt.Sprintf(
		"%s/%s-%d/00000000000000000000.log",
		m.base,
		topic,
		partition,
	)
}

func (m *LogManager) AppendLog(topicName string, partition int32, data []byte) error {
	path := m.logPath(topicName, partition)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	return err
}
