package usecase

import (
	"context"
	"sort"

	"github.com/repzspb/raf/internal/message/model"
)

// TopicSummary объединяет сведения брокера с настройкой контракта raf.
type TopicSummary struct {
	// Topic содержит имя и признак служебного топика.
	Topic model.TopicSummary
	// Type — настроенный тип по умолчанию; пустая строка означает отсутствие настройки.
	Type string
}

// TopicDetails содержит состояние топика и его контракт в raf.
type TopicDetails struct {
	// Topic содержит метаданные и границы партиций.
	Topic model.Topic
	// Type — настроенный тип по умолчанию; тип содержимого записей автоматически не определяется.
	Type string
}

func (s *Service) ListTopics(ctx context.Context) ([]TopicSummary, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	topics, err := s.catalog.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]TopicSummary, 0, len(topics))
	for _, topic := range topics {
		result = append(result, TopicSummary{
			Topic: topic,
			Type:  s.topicTypes[topic.Name],
		})
	}
	sort.Slice(result, func(
		i int,
		j int,
	) bool {
		return result[i].Topic.Name < result[j].Topic.Name
	})
	return result, nil
}

func (s *Service) DescribeTopic(
	ctx context.Context,
	name string,
) (TopicDetails, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	topic, err := s.catalog.DescribeTopic(ctx, name)
	if err != nil {
		return TopicDetails{}, err
	}
	return TopicDetails{
		Topic: topic,
		Type:  s.topicTypes[name],
	}, nil
}
