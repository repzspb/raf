package kafka

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/repzspb/raf/internal/message/model"
	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

// readClient предоставляет запросы Kafka для чтения без группы потребителей.
// Ошибки отдельных топиков и партиций могут находиться внутри ответа.
type readClient interface {
	// Metadata возвращает сведения о запрошенных топиках и их партициях.
	Metadata(
		context.Context,
		*kafkago.MetadataRequest,
	) (*kafkago.MetadataResponse, error)

	// ListOffsets запрашивает позиции в партициях; адаптер использует их для определения
	// первой доступной записи и исключительной верхней границы чтения.
	ListOffsets(
		context.Context,
		*kafkago.ListOffsetsRequest,
	) (*kafkago.ListOffsetsResponse, error)

	// Fetch запрашивает пакет записей начиная с указанного offset.
	// Вызывающий код освобождает буферы записей и учитывает, что сжатый пакет
	// может содержать записи до запрошенной позиции.
	Fetch(
		context.Context,
		*kafkago.FetchRequest,
	) (*kafkago.FetchResponse, error)
}

// Recent читает последние доступные записи каждой партиции, затем объединяет
// их по времени. Чтение не использует группы потребителей и не фиксирует offset.
func (c *Client) Recent(
	ctx context.Context,
	topic string,
	limit int,
) ([]model.Message, error) {
	messages, err := c.readRecent(ctx, topic, limit)
	return messages, brokerError(err)
}

func (c *Client) readRecent(
	ctx context.Context,
	topic string,
	limit int,
) ([]model.Message, error) {
	if limit < 1 || limit > 100 {
		return nil, fmt.Errorf("limit must be between 1 and 100")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	partitions, err := c.topicPartitions(ctx, topic)
	if err != nil {
		return nil, err
	}

	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	var mu sync.Mutex
	var messages []model.Message
	for _, partition := range partitions {
		group.Go(func() error {
			if partition.Error != nil {
				return fmt.Errorf("read %s partition %d: %w", topic, partition.ID, partition.Error)
			}
			partMessages, err := c.recentPartition(ctx, topic, partition.ID, limit)
			if err != nil {
				return fmt.Errorf("read %s partition %d: %w", topic, partition.ID, err)
			}
			mu.Lock()
			defer mu.Unlock()
			messages = latestMessages(append(messages, partMessages...), limit)
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return messages, nil
}

func (c *Client) topicPartitions(
	ctx context.Context,
	topic string,
) ([]kafkago.Partition, error) {
	metadata, err := c.reader.Metadata(ctx, &kafkago.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return nil, fmt.Errorf("read topic partitions: %w", err)
	}
	for _, entry := range metadata.Topics {
		if entry.Name != topic {
			continue
		}
		if entry.Error != nil {
			return nil, fmt.Errorf("read topic %q: %w", topic, entry.Error)
		}
		if len(entry.Partitions) != 0 {
			return entry.Partitions, nil
		}
	}
	return nil, fmt.Errorf("topic %q has no partitions", topic)
}

// latestMessages ограничивает общий результат после каждой прочитанной партиции.
// При равном времени порядок задаётся партицией, затем offset.
func latestMessages(
	messages []model.Message,
	limit int,
) []model.Message {
	sort.Slice(messages, func(
		i int,
		j int,
	) bool {
		left, right := messages[i], messages[j]
		if !left.Time.Equal(right.Time) {
			return left.Time.After(right.Time)
		}
		if left.Partition != right.Partition {
			return left.Partition < right.Partition
		}
		return left.Offset > right.Offset
	})
	if len(messages) > limit {
		clear(messages[limit:])
		messages = messages[:limit]
	}
	return messages
}
