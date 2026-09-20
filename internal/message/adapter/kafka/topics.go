package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/repzspb/raf/internal/message/model"
	"github.com/repzspb/raf/internal/message/usecase"
	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

// ListTopics запрашивает только метаданные, не читая содержимое топиков.
func (c *Client) ListTopics(ctx context.Context) ([]model.TopicSummary, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	metadata, err := c.reader.Metadata(ctx, &kafkago.MetadataRequest{})
	if err != nil {
		return nil, brokerError(err)
	}
	topics := make([]model.TopicSummary, 0, len(metadata.Topics))
	for _, topic := range metadata.Topics {
		if topic.Error != nil {
			return nil, brokerError(fmt.Errorf("read topic %q: %w", topic.Name, topic.Error))
		}
		topics = append(topics, model.TopicSummary{
			Name:     topic.Name,
			Internal: topic.Internal,
		})
	}
	sort.Slice(topics, func(
		i int,
		j int,
	) bool {
		return topics[i].Name < topics[j].Name
	})
	return topics, nil
}

// DescribeTopic получает диапазоны партиций независимо друг от друга.
// Чтение метаданных не создаёт топик и не меняет позиции потребителей.
func (c *Client) DescribeTopic(
	ctx context.Context,
	name string,
) (model.Topic, error) {
	topic, err := c.describeTopic(ctx, name)
	if errors.Is(err, kafkago.UnknownTopicOrPartition) {
		return model.Topic{}, &usecase.NotFoundError{Err: err}
	}
	return topic, brokerError(err)
}

func (c *Client) describeTopic(
	ctx context.Context,
	name string,
) (model.Topic, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	metadata, err := c.reader.Metadata(ctx, &kafkago.MetadataRequest{Topics: []string{name}})
	if err != nil {
		return model.Topic{}, err
	}
	var found *kafkago.Topic
	for i := range metadata.Topics {
		if metadata.Topics[i].Name == name {
			found = &metadata.Topics[i]
			break
		}
	}
	if found == nil {
		return model.Topic{}, fmt.Errorf("topic %q: %w", name, kafkago.UnknownTopicOrPartition)
	}
	if found.Error != nil {
		return model.Topic{}, fmt.Errorf("topic %q: %w", name, found.Error)
	}
	result := model.Topic{
		TopicSummary: model.TopicSummary{
			Name:     name,
			Internal: found.Internal,
		},
		Partitions: make([]model.Partition, len(found.Partitions)),
	}
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	for i, partition := range found.Partitions {
		group.Go(func() error {
			if partition.Error != nil {
				return fmt.Errorf("partition %d: %w", partition.ID, partition.Error)
			}
			bounds, err := c.offsets(ctx, name, partition.ID)
			if err != nil {
				return fmt.Errorf("partition %d: %w", partition.ID, err)
			}
			result.Partitions[i] = model.Partition{
				ID:          partition.ID,
				FirstOffset: bounds.first,
				EndOffset:   bounds.end,
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return model.Topic{}, err
	}
	sort.Slice(result.Partitions, func(
		i int,
		j int,
	) bool {
		return result.Partitions[i].ID < result.Partitions[j].ID
	})
	return result, nil
}
