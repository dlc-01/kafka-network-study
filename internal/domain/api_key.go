package domain

type ApiKey struct {
	ApiKey     uint16
	MinVersion uint16
	MaxVersion uint16
}

func GetDescribeTopicPartitionsApikey(apiKey uint16) ApiKey {
	return ApiKey{
		ApiKey:     apiKey,
		MinVersion: 0,
		MaxVersion: 4,
	}
}
