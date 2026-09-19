package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

type Client struct {
	brokers []string
	writer  *kafkago.Writer
}

func New(brokers []string) *Client {
	client := &Client{brokers: brokers}
	client.writer = &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		Completion: func(messages []kafkago.Message, err error) {
			if err != nil {
				return
			}
			for _, message := range messages {
				if receipt, ok := message.WriterData.(chan kafkago.Message); ok {
					receipt <- message
				}
			}
		},
	}
	return client
}

func (c *Client) Close() error { return c.writer.Close() }

func (c *Client) Publish(ctx context.Context, topic, key string, headers []kafkago.Header, value []byte) (kafkago.Message, error) {
	receipt := make(chan kafkago.Message, 1)
	message := kafkago.Message{Topic: topic, Key: []byte(key), Value: value, Headers: headers, WriterData: receipt}
	if err := c.writer.WriteMessages(ctx, message); err != nil {
		return kafkago.Message{}, fmt.Errorf("write Kafka message: %w", err)
	}
	return <-receipt, nil
}
