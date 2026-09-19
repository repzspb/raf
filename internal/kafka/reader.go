package kafka

import (
	"context"
	"fmt"
	"sort"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Recent reads a bounded number of records from every partition without joining a consumer group.
func (c *Client) Recent(ctx context.Context, topic string, limit int) ([]kafkago.Message, error) {
	var metadata *kafkago.Conn
	var err error
	var connectedBroker string
	for _, broker := range c.brokers {
		metadata, err = kafkago.DialContext(ctx, "tcp", broker)
		if err == nil {
			connectedBroker = broker
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("connect to Kafka: %w", err)
	}
	partitions, err := metadata.ReadPartitions(topic)
	metadata.Close()
	if err != nil {
		return nil, fmt.Errorf("read topic partitions: %w", err)
	}
	if len(partitions) == 0 {
		return nil, fmt.Errorf("topic %q has no partitions", topic)
	}

	var messages []kafkago.Message
	for _, partition := range partitions {
		partMessages, err := c.recentPartition(ctx, connectedBroker, topic, partition.ID, limit)
		if err != nil {
			return nil, err
		}
		messages = append(messages, partMessages...)
	}
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
		messages = messages[:limit]
	}
	return messages, nil
}

func (c *Client) recentPartition(ctx context.Context, broker, topic string, partition, limit int) ([]kafkago.Message, error) {
	conn, err := kafkago.DialLeader(ctx, "tcp", broker, topic, partition)
	if err != nil {
		return nil, fmt.Errorf("connect to %s partition %d: %w", topic, partition, err)
	}
	defer conn.Close()
	first, last, err := conn.ReadOffsets()
	if err != nil {
		return nil, fmt.Errorf("read offsets for %s partition %d: %w", topic, partition, err)
	}
	if last <= first {
		return nil, nil
	}
	start := last - int64(limit)
	if start < first {
		start = first
	}
	if _, err := conn.Seek(start, kafkago.SeekAbsolute); err != nil {
		return nil, fmt.Errorf("seek %s partition %d: %w", topic, partition, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	var messages []kafkago.Message
	for len(messages) < limit {
		message, err := conn.ReadMessage(10 << 20)
		if err != nil {
			return nil, fmt.Errorf("read %s partition %d: %w", topic, partition, err)
		}
		if message.Offset >= last {
			break
		}
		messages = append(messages, message)
		if message.Offset+1 >= last {
			break
		}
	}
	return messages, nil
}
