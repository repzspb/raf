package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/repzspb/raf/internal/message/model"
	"github.com/repzspb/raf/internal/message/usecase"
)

// serviceStub подменяет сценарии для отдельной проверки HTTP-параметров и ответов.
type serviceStub struct {
	// publish проверяет переданные данные и задаёт результат публикации.
	publish func(
		context.Context,
		usecase.PublishInput,
	) (model.Position, error)
	// inspect проверяет параметры просмотра и задаёт возвращаемые записи.
	inspect func(
		context.Context,
		usecase.InspectInput,
	) ([]usecase.InspectedMessage, error)
}

func (s serviceStub) Example(
	ctx context.Context,
	name string,
) ([]byte, error) {
	return []byte("{}"), nil
}

func (s serviceStub) Publish(
	ctx context.Context,
	input usecase.PublishInput,
) (model.Position, error) {
	return s.publish(ctx, input)
}
func (s serviceStub) Inspect(
	ctx context.Context,
	input usecase.InspectInput,
) ([]usecase.InspectedMessage, error) {
	return s.inspect(ctx, input)
}
func (s serviceStub) ListTypes() []string { return []string{"example.Event"} }

func TestHTTPPassesInputToScenarios(t *testing.T) {
	service := serviceStub{
		publish: func(
			ctx context.Context,
			got usecase.PublishInput,
		) (model.Position, error) {
			want := usecase.PublishInput{
				Topic: "events",
				Type:  "example.Event",
				Key:   []byte("a b"),
				JSON:  []byte("{}"),
				Headers: []model.Header{{
					Key:   "a",
					Value: []byte(""),
				}, {
					Key:   "z",
					Value: []byte("last"),
				}},
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("publish input = %+v", got)
			}
			return model.Position{
				Topic:     got.Topic,
				Partition: 2,
				Offset:    7,
			}, nil
		},
		inspect: func(
			ctx context.Context,
			got usecase.InspectInput,
		) ([]usecase.InspectedMessage, error) {
			if got != (usecase.InspectInput{
				Topic: "events",
				Limit: 10,
			}) {
				t.Fatalf("inspect input = %+v", got)
			}
			return nil, nil
		},
	}
	handler := New(service, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := httptest.NewRequest("POST", "/topics/events/messages?type=example.Event&key=a%20b", strings.NewReader("{}"))
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	request.Header.Set("X-Raf-Headers", `{"z":"last","a":""}`)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 201 {
		t.Fatalf("publish: %d %s", response.Code, response.Body)
	}
	assertJSONEqual(t, response.Body.Bytes(), []byte(`{"topic":"events","partition":2,"offset":7}`))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/topics/events/messages", nil))
	if response.Code != 200 {
		t.Fatalf("inspect: %d %s", response.Code, response.Body)
	}
	assertJSONEqual(t, response.Body.Bytes(), []byte(`{"topic":"events","messages":[]}`))
}
