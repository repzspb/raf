package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

type readClient interface {
	Metadata(context.Context, *kafkago.MetadataRequest) (*kafkago.MetadataResponse, error)
	ListOffsets(context.Context, *kafkago.ListOffsetsRequest) (*kafkago.ListOffsetsResponse, error)
	Fetch(context.Context, *kafkago.FetchRequest) (*kafkago.FetchResponse, error)
}

// Recent reads the last available records by offset in each partition, then
// sorts those candidates by timestamp. It never joins a consumer group.
func (c *Client) Recent(ctx context.Context, topic string, limit int) ([]kafkago.Message, error) {
	if limit < 1 || limit > 100 {
		return nil, fmt.Errorf("limit must be between 1 and 100")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	metadata, err := c.reader.Metadata(ctx, &kafkago.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return nil, fmt.Errorf("read topic partitions: %w", err)
	}
	var partitions []kafkago.Partition
	for _, t := range metadata.Topics {
		if t.Name == topic {
			if t.Error != nil {
				return nil, fmt.Errorf("read topic %q: %w", topic, t.Error)
			}
			partitions = t.Partitions
		}
	}
	if len(partitions) == 0 {
		return nil, fmt.Errorf("topic %q has no partitions", topic)
	}
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	var mu sync.Mutex
	var messages []kafkago.Message
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
			messages = append(messages, partMessages...)
			sort.Slice(messages, func(i, j int) bool {
				if messages[i].Time.Equal(messages[j].Time) {
					if messages[i].Partition == messages[j].Partition {
						return messages[i].Offset > messages[j].Offset
					}
					return messages[i].Partition < messages[j].Partition
				}
				return messages[i].Time.After(messages[j].Time)
			})
			if len(messages) > limit {
				clear(messages[limit:])
				messages = messages[:limit]
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return messages, nil
}

func (c *Client) offsets(ctx context.Context, topic string, partition int) (int64, int64, error) {
	response, err := c.reader.ListOffsets(ctx, &kafkago.ListOffsetsRequest{
		Topics: map[string][]kafkago.OffsetRequest{topic: {kafkago.FirstOffsetOf(partition), kafkago.LastOffsetOf(partition)}},
	})
	if err != nil {
		return 0, 0, err
	}
	for _, p := range response.Topics[topic] {
		if p.Partition == partition {
			if p.Error != nil {
				return 0, 0, p.Error
			}
			if p.FirstOffset < 0 || p.LastOffset < p.FirstOffset {
				return 0, 0, fmt.Errorf("invalid offset range [%d, %d)", p.FirstOffset, p.LastOffset)
			}
			return p.FirstOffset, p.LastOffset, nil
		}
	}
	return 0, 0, fmt.Errorf("partition missing from offsets response")
}

func (c *Client) recentPartition(ctx context.Context, topic string, partition, limit int) ([]kafkago.Message, error) {
	first, end, err := c.offsets(ctx, topic, partition)
	if err != nil {
		return nil, fmt.Errorf("read offsets: %w", err)
	}
	// Search disjoint windows backwards. Offsets are not record counts: a
	// compacted window may contain fewer than limit records, or none at all.
	width := int64(limit)
	var messages []kafkago.Message
	for end > first && len(messages) < limit {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		start := max(first, end-width)
		window, err := c.readWindow(ctx, topic, partition, start, end, limit-len(messages))
		if err != nil {
			return nil, err
		}
		messages = append(window, messages...)
		end = start
		// Clamp before multiplying so even very large offsets cannot overflow.
		if width >= (end-first)/2 {
			width = end - first
		} else {
			width *= 2
		}
	}
	return messages, nil
}

func (c *Client) readWindow(ctx context.Context, topic string, partition int, start, end int64, limit int) ([]kafkago.Message, error) {
	var messages []kafkago.Message
	for offset := start; offset < end; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		response, err := c.reader.Fetch(ctx, &kafkago.FetchRequest{
			Topic: topic, Partition: partition, Offset: offset,
			MinBytes: 0, MaxBytes: 10 << 20, MaxWait: 100 * time.Millisecond,
		})
		if err != nil {
			return nil, fmt.Errorf("fetch at offset %d: %w", offset, err)
		}
		if response.Error != nil {
			_ = visitRecords(response.Records, false, func(*kafkago.Record, bool) error { return nil })
		}
		if errors.Is(response.Error, kafkago.OffsetOutOfRange) {
			first, last, err := c.offsets(ctx, topic, partition)
			if err != nil {
				return nil, err
			}
			end = min(end, last)
			if offset >= end {
				break
			}
			if first <= offset {
				return nil, fmt.Errorf("fetch at offset %d: %w", offset, response.Error)
			}
			offset = first // retention advanced while we were reading
			continue
		}
		if response.Error != nil {
			return nil, response.Error
		}
		next := offset
		err = visitRecords(response.Records, false, func(record *kafkago.Record, control bool) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			next = max(next, record.Offset+1)
			if control || record.Offset < offset || record.Offset >= end {
				return nil
			}
			key, err := kafkago.ReadAll(record.Key)
			if err != nil {
				return err
			}
			value, err := kafkago.ReadAll(record.Value)
			if err != nil {
				return err
			}
			if len(messages) == limit {
				copy(messages, messages[1:])
				messages = messages[:limit-1]
			}
			headers := make([]kafkago.Header, len(record.Headers))
			for i, header := range record.Headers {
				headers[i] = kafkago.Header{Key: header.Key, Value: bytes.Clone(header.Value)}
			}
			messages = append(messages, kafkago.Message{
				Topic: topic, Partition: partition, Offset: record.Offset,
				Time: record.Time, Key: key, Value: value,
				Headers: headers,
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
		if next <= offset {
			var hasBatches bool
			next, hasBatches = nextAfterEmptyBatch(response.Records, offset)
			if !hasBatches {
				break // empty fetch: do not wait for future publications
			}
		}
		offset = next
	}
	return messages, nil
}
