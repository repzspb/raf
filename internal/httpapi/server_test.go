package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/repzspb/raf/internal/protobuf"
	kafkago "github.com/segmentio/kafka-go"
)

type fakeBroker struct {
	message kafkago.Message
}

func (b *fakeBroker) Publish(_ context.Context, topic, key string, headers []kafkago.Header, value []byte) (kafkago.Message, error) {
	b.message = kafkago.Message{
		Topic: topic, Partition: 2, Offset: 7, Time: time.Now(),
		Key: []byte(key), Headers: headers, Value: value,
	}
	return b.message, nil
}

func (b *fakeBroker) Recent(_ context.Context, _ string, _ int) ([]kafkago.Message, error) {
	return []kafkago.Message{b.message}, nil
}

func TestPublishAndInspectProtobuf(t *testing.T) {
	codec, err := protobuf.New(t.Context(), []string{"event.proto"}, []string{"../../examples/proto"})
	if err != nil {
		t.Fatal(err)
	}
	broker := new(fakeBroker)
	handler := New(broker, codec, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest(http.MethodPost,
		"/topics/example.events/messages?type=example.Event&key=event-1",
		bytes.NewBufferString(`{"id":"event-1","status":"CREATED"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Raf-Headers", `{"id":"manual-1"}`)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("publish status = %d, body = %s", response.Code, response.Body.String())
	}
	if broker.message.Topic != "example.events" || string(broker.message.Key) != "event-1" || len(broker.message.Headers) != 1 {
		t.Fatalf("wrong Kafka message metadata: %+v", broker.message)
	}
	decoded, err := codec.Decode("example.Event", broker.message.Value)
	if err != nil || string(decoded) != `{"id":"event-1","status":"CREATED"}` {
		t.Fatalf("Kafka value is not the expected Protobuf: %s, %v", decoded, err)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/topics/example.events/messages?type=example.Event&limit=10", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("inspect status = %d, body = %s", response.Code, response.Body.String())
	}
	var result struct {
		Messages []struct {
			Partition int             `json:"partition"`
			Offset    int64           `json:"offset"`
			Value     json.RawMessage `json:"value"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 1 || result.Messages[0].Partition != 2 || result.Messages[0].Offset != 7 ||
		string(result.Messages[0].Value) != string(decoded) {
		t.Fatalf("unexpected inspection response: %s", response.Body.String())
	}
}
