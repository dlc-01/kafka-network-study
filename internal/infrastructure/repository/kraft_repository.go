package repository

import (
	"bytes"
	"errors"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/internal/domain"
)

type KraftMetadataRepository struct {
	path string
}

func NewKraftMetadataRepository(path string) *KraftMetadataRepository {
	return &KraftMetadataRepository{path: path}
}

func (r *KraftMetadataRepository) GetTopic(name string) (*domain.TopicMetadata, error) {
	data, err := os.ReadFile(r.path)
	if err != nil {
		return nil, err
	}

	nameBytes := []byte(name)

	for i := 0; i+len(nameBytes)+16 <= len(data); i++ {
		if bytes.Equal(data[i:i+len(nameBytes)], nameBytes) {
			var topicID [16]byte
			copy(topicID[:], data[i+len(nameBytes):i+len(nameBytes)+16])

			partitions := []domain.PartitionMetadata{
				{
					PartitionIndex: 0,
					LeaderID:       1,
					LeaderEpoch:    0,
					Replicas:       []int32{1},
					ISR:            []int32{1},
				},
			}

			return &domain.TopicMetadata{
				Name:       name,
				TopicID:    topicID,
				Partitions: partitions,
			}, nil
		}
	}

	return nil, errors.New("not found")
}
