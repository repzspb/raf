package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/repzspb/raf/internal/message/usecase"
	kafkago "github.com/segmentio/kafka-go"
)

func TestBrokerTimeoutClassification(t *testing.T) {
	for _, tt := range []struct {
		err     error
		timeout bool
	}{
		{kafkago.RequestTimedOut, true},
		{fmt.Errorf("fetch: %w", context.DeadlineExceeded), true},
		{context.Canceled, false},
		{kafkago.UnknownTopicOrPartition, false},
	} {
		got := brokerError(tt.err)
		var timeout *usecase.TimeoutError
		if errors.As(got, &timeout) != tt.timeout || !errors.Is(got, tt.err) {
			t.Fatalf("classification of %v: %v", tt.err, got)
		}
	}
	if brokerError(nil) != nil {
		t.Fatal("nil became an error")
	}
}
