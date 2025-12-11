package response

import "github.com/codecrafters-io/kafka-starter-go/internal/domain"

type ApiKeyResponse struct {
	ApiKey     uint16
	MinVersion uint16
	MaxVersion uint16
}

func GetApiVersions() ApiKeyResponse {
	return ApiKeyResponse{
		ApiKey:     domain.ApiVersionApikey,
		MinVersion: domain.NONE,
		MaxVersion: domain.MaximumVersionApiKey,
	}
}

func GetDescribeTopicPartitionsApikey() ApiKeyResponse {
	return ApiKeyResponse{
		ApiKey:     domain.DescribeTopicPartitionsApikey,
		MinVersion: domain.NONE,
		MaxVersion: domain.NONE,
	}
}

func GetFetchApiKey() ApiKeyResponse {
	return ApiKeyResponse{
		ApiKey:     domain.FetchApikey,
		MinVersion: domain.NONE,
		MaxVersion: domain.MaximumVersionFetchApiKey,
	}
}

func GetProduceApiKey() ApiKeyResponse {
	return ApiKeyResponse{
		ApiKey:     domain.ProduceApiKey,
		MinVersion: domain.NONE,
		MaxVersion: domain.MaximumVersionProduceApiKey,
	}
}
