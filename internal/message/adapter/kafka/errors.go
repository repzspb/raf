package kafka

import (
	"context"
	"errors"
	"net"

	"github.com/repzspb/raf/internal/message/usecase"
	kafkago "github.com/segmentio/kafka-go"
)

// brokerError сохраняет исходную ошибку и обозначает таймаут на границе адаптера.
func brokerError(err error) error {
	var networkError net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kafkago.RequestTimedOut) ||
		(errors.As(err, &networkError) && networkError.Timeout()) {
		return &usecase.TimeoutError{Err: err}
	}
	return err
}
