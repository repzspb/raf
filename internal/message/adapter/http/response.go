package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/repzspb/raf/internal/message/usecase"
)

// record задаёт представление одной прочитанной записи в HTTP-ответе.
type record struct {
	// Partition — номер партиции, из которой прочитана запись.
	Partition int `json:"partition"`
	// Offset — позиция записи внутри партиции.
	Offset int64 `json:"offset"`
	// Time — временная метка записи из Kafka.
	Time time.Time `json:"time"`
	// Key — ключ в UTF-8; nil означает отсутствующий или бинарный ключ.
	Key *string `json:"key"`
	// KeyBase64 — исходный ключ в Base64, если его байты не являются корректным UTF-8.
	KeyBase64 string `json:"key_base64,omitempty"`
	// Headers — заголовки сообщения; при их отсутствии возвращается пустой массив.
	Headers []header `json:"headers"`
	// Value — декодированный JSON либо JSON null для tombstone; при ошибке поле опускается.
	Value json.RawMessage `json:"value,omitempty"`
	// ValueBase64 — исходное тело в Base64 при ошибке декодирования.
	ValueBase64 string `json:"value_base64,omitempty"`
	// DecodeError — описание ошибки декодирования конкретной записи.
	DecodeError string `json:"decode_error,omitempty"`
	// Tombstone указывает, что в Kafka записано null вместо бинарного тела.
	Tombstone bool `json:"tombstone,omitempty"`
}

// header задаёт представление заголовка Kafka в HTTP-ответе.
type header struct {
	// Name — имя заголовка.
	Name string `json:"name"`
	// Value — значение в UTF-8; nil означает отсутствующее или бинарное значение.
	Value *string `json:"value"`
	// ValueBase64 — значение в Base64, если его байты не являются корректным UTF-8.
	ValueBase64 string `json:"value_base64,omitempty"`
}

func messageRecord(item usecase.InspectedMessage) record {
	message := item.Message
	rec := record{
		Partition: message.Partition,
		Offset:    message.Offset,
		Time:      message.Time,
		Headers:   make([]header, 0, len(message.Headers)),
	}
	rec.Key, rec.KeyBase64 = textOrBase64(message.Key)
	for _, value := range message.Headers {
		entry := header{Name: value.Key}
		entry.Value, entry.ValueBase64 = textOrBase64(value.Value)
		rec.Headers = append(rec.Headers, entry)
	}
	switch {
	case message.Value == nil:
		rec.Tombstone = true
		rec.Value = json.RawMessage("null")
	case item.DecodeError != nil:
		rec.DecodeError = item.DecodeError.Error()
		rec.ValueBase64 = base64.StdEncoding.EncodeToString(message.Value)
	default:
		rec.Value = item.JSON
	}
	return rec
}

func textOrBase64(value []byte) (*string, string) {
	if value == nil {
		return nil, ""
	}
	if !utf8.Valid(value) {
		return nil, base64.StdEncoding.EncodeToString(value)
	}
	text := string(value)
	return &text, ""
}

func (h *Handler) writeServiceError(
	w http.ResponseWriter,
	operation string,
	topic string,
	err error,
) {
	var typeError *usecase.TypeError
	var validationError *usecase.ValidationError
	// timeout распознаёт ошибки таймаута без зависимости от библиотеки брокера.
	var timeout interface {
		// Timeout сообщает, вызвана ли ошибка истечением времени ожидания.
		Timeout() bool
	}
	switch {
	case errors.As(err, &typeError):
		if typeError.Name == "" {
			err = fmt.Errorf("query parameter type is required; configure RAF_TOPIC_TYPES for a default or see GET /types for loaded types")
		} else {
			err = fmt.Errorf("%w; see GET /types for loaded types", err)
		}
		writeError(w, http.StatusBadRequest, err)
	case errors.As(err, &validationError):
		writeError(w, http.StatusBadRequest, err)
	default:
		h.logger.Error(operation, "topic", topic, "error", err)
		status := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
			status = http.StatusGatewayTimeout
		}
		writeError(w, status, err)
	}
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	value any,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(
	w http.ResponseWriter,
	status int,
	err error,
) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
