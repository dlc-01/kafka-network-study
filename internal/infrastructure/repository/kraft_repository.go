package repository

import (
	"errors"

	"github.com/codecrafters-io/kafka-starter-go/internal/domain"
)

type KraftMetadataRepository struct {
	topics map[string]*domain.TopicMetadata
	byUUID map[[16]byte]*domain.TopicMetadata
}

func NewKraftMetadataRepository(meta *LoadedMetadata) *KraftMetadataRepository {
	return &KraftMetadataRepository{
		topics: meta.ByName,
		byUUID: meta.ByUUID,
	}
}

func (r *KraftMetadataRepository) GetTopic(name string) (*domain.TopicMetadata, error) {
	t, ok := r.topics[name]
	if !ok {
		return nil, errors.New("topic not found")
	}
	return t, nil
}

func (r *KraftMetadataRepository) GetTopicByID(id [16]byte) (*domain.TopicMetadata, error) {
	t, ok := r.byUUID[id]
	if !ok {
		return nil, errors.New("topic not found")
	}
	return t, nil
}
