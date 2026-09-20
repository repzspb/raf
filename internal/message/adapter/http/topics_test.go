package httpapi

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/repzspb/raf/internal/message/model"
	"github.com/repzspb/raf/internal/message/usecase"
)

func TestTopicResponsesIncludeConfiguredType(t *testing.T) {
	broker := &fakeBroker{
		topics: []model.TopicSummary{{Name: "unmapped"}, {Name: "events"}},
		topic: model.Topic{
			TopicSummary: model.TopicSummary{Name: "events"},
			Partitions: []model.Partition{{
				ID:          0,
				FirstOffset: 10,
				EndOffset:   10,
			}},
		},
	}
	handler := testServer(t, broker, map[string]string{"events": "example.Event"})
	for path, body := range map[string]string{
		"/topics":        `{"topics":[{"name":"events","type":"example.Event","internal":false},{"name":"unmapped","type":null,"internal":false}]}`,
		"/topics/events": `{"name":"events","type":"example.Event","internal":false,"partitions":[{"id":0,"first_offset":10,"end_offset":10}]}`,
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != 200 {
			t.Fatalf("status=%d body=%s", response.Code, response.Body)
		}
		assertJSONEqual(t, response.Body.Bytes(), []byte(body))
	}
	if broker.publishCalls != 0 || broker.recentCalls != 0 {
		t.Fatal("catalog touched message operations")
	}
	broker.topics = nil
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/topics", nil))
	assertJSONEqual(t, response.Body.Bytes(), []byte(`{"topics":[]}`))
}

func TestTopicHTTPErrorStatuses(t *testing.T) {
	for status, err := range map[int]error{
		404: &usecase.NotFoundError{Err: errors.New("missing topic")},
		504: context.DeadlineExceeded,
		502: errors.New("broker unavailable"),
	} {
		response := httptest.NewRecorder()
		testServer(t, &fakeBroker{err: err}, nil).ServeHTTP(response, httptest.NewRequest("GET", "/topics/events", nil))
		if response.Code != status {
			t.Fatalf("status=%d want=%d body=%s", response.Code, status, response.Body)
		}
	}
}
