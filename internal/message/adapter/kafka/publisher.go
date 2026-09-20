package kafka

import (
	"context"
	"fmt"

	"github.com/repzspb/raf/internal/message/model"
	kafkago "github.com/segmentio/kafka-go"
)

func (c *Client) Publish(
	ctx context.Context,
	input model.Message,
) (model.Position, error) {
	receipt := make(chan kafkago.Message, 1)
	headers := make([]kafkago.Header, len(input.Headers))
	for i, header := range input.Headers {
		headers[i] = kafkago.Header{
			Key:   header.Key,
			Value: header.Value,
		}
	}
	message := kafkago.Message{
		Topic:      input.Topic,
		Key:        input.Key,
		Value:      input.Value,
		Headers:    headers,
		WriterData: receipt,
	}
	if err := c.writer.WriteMessages(ctx, message); err != nil {
		return model.Position{}, brokerError(fmt.Errorf("write Kafka message: %w", err))
	}
	select {
	case delivered := <-receipt:
		return model.Position{
			Topic:     delivered.Topic,
			Partition: delivered.Partition,
			Offset:    delivered.Offset,
		}, nil
	case <-ctx.Done():
		return model.Position{}, brokerError(ctx.Err())
	}
}
