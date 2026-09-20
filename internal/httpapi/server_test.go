package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/repzspb/raf/internal/protobuf"
	kafkago "github.com/segmentio/kafka-go"
)

type fakeBroker struct {
	message      kafkago.Message
	messages     []kafkago.Message
	err          error
	publishCalls int
	recentCalls  int
}

func (b *fakeBroker) Publish(_ context.Context, topic, key string, headers []kafkago.Header, value []byte) (kafkago.Message, error) {
	b.publishCalls++
	if b.err != nil {
		return kafkago.Message{}, b.err
	}
	b.message = kafkago.Message{
		Topic: topic, Partition: 2, Offset: 7, Time: time.Now(),
		Key: []byte(key), Headers: headers, Value: value,
	}
	return b.message, nil
}

func (b *fakeBroker) Recent(_ context.Context, _ string, _ int) ([]kafkago.Message, error) {
	b.recentCalls++
	if b.err != nil {
		return nil, b.err
	}
	if b.messages != nil {
		return b.messages, nil
	}
	return []kafkago.Message{b.message}, nil
}

func TestPublishAndInspectProtobuf(t *testing.T) {
	codec, err := protobuf.New(t.Context(), []string{"event.proto"}, []string{"../../examples/proto"})
	if err != nil {
		t.Fatal(err)
	}
	broker := new(fakeBroker)
	handler := New(broker, codec, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	if err != nil {
		t.Fatalf("Kafka value is not the expected Protobuf: %s, %v", decoded, err)
	}
	assertJSONEqual(t, decoded, []byte(`{"id":"event-1","status":"CREATED"}`))

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
	if len(result.Messages) != 1 || result.Messages[0].Partition != 2 || result.Messages[0].Offset != 7 {
		t.Fatalf("unexpected inspection response: %s", response.Body.String())
	}
	assertJSONEqual(t, result.Messages[0].Value, decoded)
}

func assertJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()
	var actual, expected any
	if err := json.Unmarshal(got, &actual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(want, &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("JSON = %s, want %s", got, want)
	}
}

func testServer(t *testing.T, broker *fakeBroker, mappings map[string]string) http.Handler {
	t.Helper()
	codec, err := protobuf.New(t.Context(), []string{"event.proto"}, []string{"../../examples/proto"})
	if err != nil {
		t.Fatal(err)
	}
	return New(broker, codec, slog.New(slog.NewTextHandler(io.Discard, nil)), mappings)
}

func TestInvalidRequestsNeverReachKafka(t *testing.T) {
	tests := []struct {
		name, method, query, body, contentType, headers string
		status                                          int
	}{
		{"missing type", "POST", "", `{}`, "application/json", "", 400},
		{"unknown type", "POST", "?type=missing.Type", `{}`, "application/json", "", 400},
		{"trailing dot", "GET", "?type=example.Event.", "", "", "", 400},
		{"wrong content type", "POST", "?type=example.Event", `{}`, "text/plain", "", 415},
		{"invalid json", "POST", "?type=example.Event", `{`, "application/json", "", 400},
		{"unknown field", "POST", "?type=example.Event", `{"unknown":1}`, "application/json", "", 400},
		{"bad header", "POST", "?type=example.Event", `{}`, "application/json", `{"id":1}`, 400},
		{"null header", "POST", "?type=example.Event", `{}`, "application/json", `{"id":null}`, 400},
		{"null headers", "POST", "?type=example.Event", `{}`, "application/json", `null`, 400},
		{"large body", "POST", "?type=example.Event", strings.Repeat(" ", maxBodyBytes+1), "application/json", "", 413},
		{"zero limit", "GET", "?type=example.Event&limit=0", "", "", "", 400},
		{"large limit", "GET", "?type=example.Event&limit=101", "", "", "", 400},
		{"bad limit", "GET", "?type=example.Event&limit=no", "", "", "", 400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			broker := new(fakeBroker)
			r := httptest.NewRequest(tt.method, "/topics/events/messages"+tt.query, strings.NewReader(tt.body))
			r.Header.Set("Content-Type", tt.contentType)
			r.Header.Set("X-Raf-Headers", tt.headers)
			w := httptest.NewRecorder()
			testServer(t, broker, nil).ServeHTTP(w, r)
			if w.Code != tt.status || broker.publishCalls != 0 || broker.recentCalls != 0 {
				t.Fatalf("status=%d, publish=%d, recent=%d, body=%s", w.Code, broker.publishCalls, broker.recentCalls, w.Body)
			}
		})
	}
}

func TestTypesAndTopicDefaults(t *testing.T) {
	broker := new(fakeBroker)
	handler := testServer(t, broker, map[string]string{"events": "example.Event"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/types", nil))
	if w.Code != 200 {
		t.Fatalf("status=%d", w.Code)
	}
	assertJSONEqual(t, w.Body.Bytes(), []byte(`{"types":["example.Event"]}`))
	if broker.recentCalls != 0 {
		t.Fatal("listing types contacted Kafka")
	}
	w = httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/topics/events/messages", strings.NewReader(`{}`))
	r.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, r)
	if w.Code != 201 || broker.message.Value == nil || len(broker.message.Value) != 0 {
		t.Fatalf("empty Protobuf message must not become a tombstone: status=%d value=%#v body=%s", w.Code, broker.message.Value, w.Body)
	}
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/topics/events/messages", nil))
	if w.Code != 200 {
		t.Fatalf("default type was not used: %s", w.Body)
	}
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/topics/events/messages?type=unknown.Type", nil))
	if w.Code != 400 {
		t.Fatalf("explicit type must override default: %s", w.Body)
	}
}

func TestInspectSpecialRecords(t *testing.T) {
	broker := &fakeBroker{messages: []kafkago.Message{
		{Offset: 0, Value: nil},
		{Offset: 1, Key: []byte{}, Value: []byte{}},
		{Offset: 2, Key: []byte{0xff}, Value: []byte{0xff}, Headers: []kafkago.Header{{Key: "binary", Value: []byte{0xff}}, {Key: "text", Value: []byte("base64:/w==")}}},
	}}
	w := httptest.NewRecorder()
	testServer(t, broker, nil).ServeHTTP(w, httptest.NewRequest("GET", "/topics/events/messages?type=example.Event", nil))
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
	var result struct {
		Messages []record `json:"messages"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 3 {
		t.Fatalf("messages=%s", w.Body)
	}
	tombstone, empty, binary := result.Messages[0], result.Messages[1], result.Messages[2]
	if !tombstone.Tombstone || string(tombstone.Value) != "null" || tombstone.Key != nil {
		t.Fatalf("bad tombstone: %+v", tombstone)
	}
	if empty.Tombstone || string(empty.Value) != "{}" || empty.Key == nil || *empty.Key != "" {
		t.Fatalf("bad empty message: %+v", empty)
	}
	if binary.Key != nil || binary.KeyBase64 != "/w==" || binary.DecodeError == "" || binary.ValueBase64 != "/w==" {
		t.Fatalf("lost binary record: %+v", binary)
	}
	if binary.Headers[0].Value != nil || binary.Headers[0].ValueBase64 != "/w==" || binary.Headers[1].Value == nil || *binary.Headers[1].Value != "base64:/w==" {
		t.Fatalf("ambiguous headers: %+v", binary.Headers)
	}
}

func TestBrokerErrors(t *testing.T) {
	for _, method := range []string{"GET", "POST"} {
		for _, tt := range []struct {
			name   string
			err    error
			status int
		}{
			{"timeout", context.DeadlineExceeded, 504},
			{"kafka timeout", kafkago.RequestTimedOut, 504},
			{"unavailable", errors.New("broker unavailable"), 502},
		} {
			t.Run(method+"/"+tt.name, func(t *testing.T) {
				w := httptest.NewRecorder()
				r := httptest.NewRequest(method, "/topics/events/messages?type=example.Event", strings.NewReader(`{}`))
				r.Header.Set("Content-Type", "application/json")
				testServer(t, &fakeBroker{err: tt.err}, nil).ServeHTTP(w, r)
				if w.Code != tt.status {
					t.Fatalf("status=%d body=%s", w.Code, w.Body)
				}
			})
		}
	}
}
