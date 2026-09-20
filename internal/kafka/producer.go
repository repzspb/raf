package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type Client struct {
	reader    readClient
	writer    messageWriter
	transport *kafkago.Transport
	cancel    context.CancelFunc
	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error
}

type messageWriter interface {
	WriteMessages(context.Context, ...kafkago.Message) error
	Close() error
}

func New(brokers []string) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	transport := &kafkago.Transport{Context: ctx, DialTimeout: 5 * time.Second}
	client := &Client{
		reader:    &kafkago.Client{Addr: kafkago.TCP(brokers...), Transport: transport, Timeout: 5 * time.Second},
		transport: transport, cancel: cancel, closed: make(chan struct{}),
	}
	client.writer = &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Transport:    transport,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		MaxAttempts:  3,
		BatchSize:    1,
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

// Close flushes pending writes within ctx's deadline. On cancellation it stops
// waiting, cancels discovery and closes idle connections. In-flight requests
// finish at their own network deadlines without holding up application exit.
func (c *Client) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		go func() {
			c.closeErr = c.writer.Close()
			c.cancel()
			c.transport.CloseIdleConnections()
			close(c.closed)
		}()
	})
	select {
	case <-c.closed:
		return c.closeErr
	case <-ctx.Done():
		c.cancel()
		c.transport.CloseIdleConnections()
		return ctx.Err()
	}
}

func (c *Client) Publish(ctx context.Context, topic, key string, headers []kafkago.Header, value []byte) (kafkago.Message, error) {
	receipt := make(chan kafkago.Message, 1)
	message := kafkago.Message{Topic: topic, Key: []byte(key), Value: value, Headers: headers, WriterData: receipt}
	if err := c.writer.WriteMessages(ctx, message); err != nil {
		return kafkago.Message{}, fmt.Errorf("write Kafka message: %w", err)
	}
	return <-receipt, nil
}
