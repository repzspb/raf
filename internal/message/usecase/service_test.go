package usecase_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/repzspb/raf/internal/message/model"
	"github.com/repzspb/raf/internal/message/usecase"
)

// brokerStub подставляет заданное тестом поведение публикации и чтения.
type brokerStub struct {
	// publish обрабатывает вызов публикации без подключения к брокеру.
	publish func(
		context.Context,
		model.Message,
	) (model.Position, error)
	// recent возвращает подготовленные записи или ошибку чтения.
	recent func(
		context.Context,
		string,
		int,
	) ([]model.Message, error)
}

func (b brokerStub) Publish(
	ctx context.Context,
	message model.Message,
) (model.Position, error) {
	return b.publish(ctx, message)
}

func (b brokerStub) Recent(
	ctx context.Context,
	topic string,
	limit int,
) ([]model.Message, error) {
	return b.recent(ctx, topic, limit)
}

// codecStub подставляет преобразования тела сообщения при проверке сценариев.
type codecStub struct {
	// encode задаёт результат кодирования входного JSON.
	encode func(
		string,
		[]byte,
	) ([]byte, error)
	// decode задаёт результат декодирования бинарного тела.
	decode func(
		string,
		[]byte,
	) ([]byte, error)
}

func (c codecStub) ValidateType(name string) error {
	if name == "example.Event" || name == "example.Other" {
		return nil
	}
	return errors.New("unknown type")
}
func (c codecStub) MessageTypes() []string { return []string{"example.Event", "example.Other"} }
func (c codecStub) Encode(
	name string,
	value []byte,
) ([]byte, error) {
	return c.encode(name, value)
}
func (c codecStub) Decode(
	name string,
	value []byte,
) ([]byte, error) {
	return c.decode(name, value)
}

func TestPublishResolvesTypeAndPreservesMessage(t *testing.T) {
	for _, explicit := range []string{"", "example.Other"} {
		t.Run("type="+explicit, func(t *testing.T) {
			mappings := map[string]string{"events": "example.Event"}
			expectedType := "example.Event"
			if explicit != "" {
				expectedType = explicit
			}
			expected := model.Message{
				Topic: "events",
				Key:   []byte{0xff},
				Headers: []model.Header{{
					Key:   "id",
					Value: []byte("manual"),
				}},
				Value: []byte{},
			}
			position := model.Position{
				Topic:     "events",
				Partition: 2,
				Offset:    42,
			}
			codec := codecStub{encode: func(
				name string,
				value []byte,
			) ([]byte, error) {
				if name != expectedType || string(value) != "{}" {
					t.Fatalf("encode(%q, %s)", name, value)
				}
				return []byte{}, nil
			}}
			broker := brokerStub{publish: func(
				ctx context.Context,
				got model.Message,
			) (model.Position, error) {
				if !reflect.DeepEqual(got, expected) {
					t.Fatalf("message = %#v", got)
				}
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 15*time.Second {
					t.Fatal("missing operation deadline")
				}
				return position, nil
			}}
			service, err := usecase.New(broker, broker, codec, mappings)
			if err != nil {
				t.Fatal(err)
			}
			// Изменение исходной карты после сборки не должно менять конфигурацию сценария.
			mappings["events"] = "missing.Type"
			got, err := service.Publish(t.Context(), usecase.PublishInput{
				Topic:   "events",
				Type:    explicit,
				Key:     expected.Key,
				Headers: expected.Headers,
				JSON:    []byte("{}"),
			})
			if err != nil || got != position {
				t.Fatalf("position = %+v, error = %v", got, err)
			}
		})
	}
}

func TestInvalidInputNeverReachesBroker(t *testing.T) {
	codec := codecStub{encode: func(
		string,
		[]byte,
	) ([]byte, error) {
		return nil, errors.New("invalid JSON")
	}}
	// Пустые функции брокера обнаружат любой ошибочный вызов после неудачной проверки.
	service, err := usecase.New(brokerStub{}, brokerStub{}, codec, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", "unknown.Type"} {
		_, err := service.Publish(t.Context(), usecase.PublishInput{Type: name})
		var typeError *usecase.TypeError
		if !errors.As(err, &typeError) {
			t.Fatalf("publish error = %v", err)
		}
		_, err = service.Inspect(t.Context(), usecase.InspectInput{
			Type:  name,
			Limit: 10,
		})
		if !errors.As(err, &typeError) {
			t.Fatalf("inspect error = %v", err)
		}
	}
	_, err = service.Publish(t.Context(), usecase.PublishInput{Type: "example.Event"})
	var validationError *usecase.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("encode error = %v", err)
	}
	for _, limit := range []int{0, 101} {
		_, err = service.Inspect(t.Context(), usecase.InspectInput{
			Type:  "example.Event",
			Limit: limit,
		})
		if !errors.As(err, &validationError) {
			t.Fatalf("limit error = %v", err)
		}
	}
}

func TestInspectPreservesTombstonesAndDecodeFailures(t *testing.T) {
	decodeError := errors.New("invalid protobuf")
	messages := []model.Message{
		{
			Offset: 1,
			Value:  nil,
		},
		{
			Offset: 2,
			Value:  []byte{},
		},
		{
			Offset: 3,
			Value:  []byte{0xff},
		},
		{
			Offset: 4,
			Value:  []byte("valid"),
		},
	}
	broker := brokerStub{recent: func(
		ctx context.Context,
		topic string,
		limit int,
	) ([]model.Message, error) {
		if topic != "events" || limit != 10 {
			t.Fatalf("read %s limit %d", topic, limit)
		}
		return messages, nil
	}}
	decodeCalls := 0
	codec := codecStub{decode: func(
		name string,
		value []byte,
	) ([]byte, error) {
		decodeCalls++
		if value == nil {
			t.Fatal("tombstone reached decoder")
		}
		if len(value) == 1 {
			return nil, decodeError
		}
		return []byte("{}"), nil
	}}
	service, err := usecase.New(broker, broker, codec, map[string]string{"events": "example.Event"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Inspect(t.Context(), usecase.InspectInput{
		Topic: "events",
		Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 4 || decodeCalls != 3 {
		t.Fatalf("records=%d decoded=%d", len(result), decodeCalls)
	}
	for i, item := range result {
		if !reflect.DeepEqual(item.Message, messages[i]) {
			t.Fatalf("record %d changed: %+v", i, item)
		}
	}
	if result[0].JSON != nil || result[0].DecodeError != nil ||
		string(result[1].JSON) != "{}" || !errors.Is(result[2].DecodeError, decodeError) ||
		string(result[3].JSON) != "{}" {
		t.Fatalf("unexpected decode results: %+v", result)
	}
}

func TestCancellationReachesBroker(t *testing.T) {
	for _, operation := range []string{"publish", "inspect"} {
		t.Run(operation, func(t *testing.T) {
			started := make(chan struct{})
			wait := func(ctx context.Context) error { close(started); <-ctx.Done(); return ctx.Err() }
			broker := brokerStub{
				publish: func(
					ctx context.Context,
					_ model.Message,
				) (model.Position, error) {
					return model.Position{}, wait(ctx)
				},
				recent: func(
					ctx context.Context,
					_ string,
					_ int,
				) ([]model.Message, error) {
					return nil, wait(ctx)
				},
			}
			codec := codecStub{encode: func(
				string,
				[]byte,
			) ([]byte, error) {
				return []byte{}, nil
			}}
			service, err := usecase.New(broker, broker, codec, map[string]string{"events": "example.Event"})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				if operation == "publish" {
					_, err = service.Publish(ctx, usecase.PublishInput{Topic: "events"})
				} else {
					_, err = service.Inspect(ctx, usecase.InspectInput{
						Topic: "events",
						Limit: 10,
					})
				}
				done <- err
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("broker was not called")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("operation ignored cancellation")
			}
		})
	}
}

func TestMappingsAndTypeDiscovery(t *testing.T) {
	_, err := usecase.New(brokerStub{}, brokerStub{}, codecStub{}, map[string]string{"events": "unknown.Type"})
	if err == nil {
		t.Fatal("invalid mapping accepted")
	}
	service, err := usecase.New(brokerStub{}, brokerStub{}, codecStub{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := service.ListTypes(); !reflect.DeepEqual(got, []string{"example.Event", "example.Other"}) {
		t.Fatalf("types = %v", got)
	}
}
