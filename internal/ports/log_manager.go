package ports

type LogManager interface {
	LoadLog(topicName string, partition int32) ([]byte, error)
}
