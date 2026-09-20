package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// blockedWriter задерживает закрытие отправителя для проверки отмены ожидания.
type blockedWriter struct {
	// started сообщает тесту о входе в Close.
	started chan struct{}
	// release разрешает заблокированному Close завершиться.
	release chan struct{}
	// calls считает вызовы Close, чтобы проверить однократность закрытия.
	calls atomic.Int32
}

func (w *blockedWriter) WriteMessages(
	context.Context,
	...kafkago.Message,
) error {
	return nil
}
func (w *blockedWriter) Close() error {
	w.calls.Add(1)
	close(w.started)
	<-w.release
	return nil
}

func TestCloseRespectsCancellationAndCanBeWaitedAgain(t *testing.T) {
	c := New([]string{"unused:9092"})
	w := &blockedWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	c.writer = w
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- c.Close(ctx) }()
	<-w.started
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("close error = %v", err)
		}
	case <-time.After(time.Second):
		close(w.release)
		t.Fatal("close ignored cancellation")
	}
	if c.transport.Context.Err() == nil {
		t.Error("transport was not canceled")
	}
	close(w.release)
	waitCtx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err := c.Close(waitCtx); err != nil {
		t.Fatal(err)
	}
	if w.calls.Load() != 1 {
		t.Fatal("writer closed more than once")
	}
}
