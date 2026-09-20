package usecase

import (
	"fmt"
	"maps"
	"time"
)

const operationTimeout = 15 * time.Second

// Service выполняет сценарии публикации, просмотра сообщений и получения списка типов.
type Service struct {
	// publisher записывает подготовленные бинарные сообщения.
	publisher Publisher
	// reader читает последние доступные записи без изменения позиции потребителей.
	reader Reader
	// codec проверяет контракты и преобразует тела сообщений между JSON и бинарным форматом.
	codec Codec
	// topicTypes хранит копию настроек типов по умолчанию для каждого топика.
	topicTypes map[string]string
}

func New(
	publisher Publisher,
	reader Reader,
	codec Codec,
	topicTypes map[string]string,
) (*Service, error) {
	for topic, name := range topicTypes {
		if err := codec.ValidateType(name); err != nil {
			return nil, fmt.Errorf("topic %q: %w", topic, err)
		}
	}
	return &Service{
		publisher:  publisher,
		reader:     reader,
		codec:      codec,
		topicTypes: maps.Clone(topicTypes),
	}, nil
}

func (s *Service) messageType(
	topic string,
	name string,
) (string, error) {
	if name == "" {
		name = s.topicTypes[topic]
	}
	if name == "" {
		return "", &TypeError{Err: fmt.Errorf("message type is required")}
	}
	if err := s.codec.ValidateType(name); err != nil {
		return "", &TypeError{
			Name: name,
			Err:  err,
		}
	}
	return name, nil
}
