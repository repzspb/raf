package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/repzspb/raf/internal/message/model"
	kafkago "github.com/segmentio/kafka-go"
)

// offsetRange задаёт полуоткрытый диапазон [first, end).
// end — позиция следующей записи, она не включается в чтение.
type offsetRange struct {
	// first — включённая нижняя граница диапазона.
	first int64
	// end — исключённая верхняя граница; равенство first и end означает пустой диапазон.
	end int64
}

func (c *Client) offsets(
	ctx context.Context,
	topic string,
	partition int,
) (offsetRange, error) {
	response, err := c.reader.ListOffsets(ctx, &kafkago.ListOffsetsRequest{
		Topics: map[string][]kafkago.OffsetRequest{topic: {kafkago.FirstOffsetOf(partition), kafkago.LastOffsetOf(partition)}},
	})
	if err != nil {
		return offsetRange{}, err
	}
	for _, p := range response.Topics[topic] {
		if p.Partition == partition {
			if p.Error != nil {
				return offsetRange{}, p.Error
			}
			if p.FirstOffset < 0 || p.LastOffset < p.FirstOffset {
				return offsetRange{}, fmt.Errorf("invalid offset range [%d, %d)", p.FirstOffset, p.LastOffset)
			}
			return offsetRange{
				first: p.FirstOffset,
				end:   p.LastOffset,
			}, nil
		}
	}
	return offsetRange{}, fmt.Errorf("partition missing from offsets response")
}

func (c *Client) recentPartition(
	ctx context.Context,
	topic string,
	partition int,
	limit int,
) ([]model.Message, error) {
	bounds, err := c.offsets(ctx, topic, partition)
	if err != nil {
		return nil, fmt.Errorf("read offsets: %w", err)
	}
	first, end := bounds.first, bounds.end
	// Просматриваем непересекающиеся окна от конца партиции к началу.
	// Разница offset не равна числу записей: после compaction в окне могут быть пропуски.
	width := int64(limit)
	var messages []model.Message
	for end > first && len(messages) < limit {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		start := max(first, end-width)
		window, err := c.readWindow(ctx, topic, partition, offsetRange{
			first: start,
			end:   end,
		}, limit-len(messages))
		if err != nil {
			return nil, err
		}
		messages = append(window, messages...)
		end = start
		// Ограничиваем ширину до умножения, чтобы избежать переполнения.
		if width >= (end-first)/2 {
			width = end - first
		} else {
			width *= 2
		}
	}
	return messages, nil
}

func (c *Client) readWindow(
	ctx context.Context,
	topic string,
	partition int,
	window offsetRange,
	limit int,
) ([]model.Message, error) {
	var messages []model.Message
	for offset := window.first; offset < window.end; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		response, err := c.reader.Fetch(ctx, &kafkago.FetchRequest{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
			MinBytes:  0,
			MaxBytes:  10 << 20,
			MaxWait:   100 * time.Millisecond,
		})
		if err != nil {
			return nil, fmt.Errorf("fetch at offset %d: %w", offset, err)
		}
		if response.Error != nil {
			_ = visitRecords(response.Records, false, func(
				*kafkago.Record,
				bool,
			) error {
				return nil
			})
		}
		if errors.Is(response.Error, kafkago.OffsetOutOfRange) {
			bounds, err := c.offsets(ctx, topic, partition)
			if err != nil {
				return nil, err
			}
			window.end = min(window.end, bounds.end)
			if offset >= window.end {
				break
			}
			if bounds.first <= offset {
				return nil, fmt.Errorf("fetch at offset %d: %w", offset, response.Error)
			}
			offset = bounds.first // Retention удалил начало партиции во время чтения.
			continue
		}
		if response.Error != nil {
			return nil, response.Error
		}
		next := offset
		err = visitRecords(response.Records, false, func(
			record *kafkago.Record,
			control bool,
		) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			next = max(next, record.Offset+1)
			if control || record.Offset < offset || record.Offset >= window.end {
				return nil
			}
			message, err := copyRecord(topic, partition, record)
			if err != nil {
				return err
			}
			if len(messages) == limit {
				copy(messages, messages[1:])
				messages = messages[:limit-1]
			}
			messages = append(messages, message)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if next <= offset {
			var hasBatches bool
			next, hasBatches = nextAfterEmptyBatch(response.Records, offset)
			if !hasBatches {
				break // Пустой ответ: новых публикаций не ждём.
			}
		}
		offset = next
	}
	return messages, nil
}
