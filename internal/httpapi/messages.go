package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"
)

type record struct {
	Partition   int             `json:"partition"`
	Offset      int64           `json:"offset"`
	Time        time.Time       `json:"time"`
	Key         string          `json:"key"`
	Headers     []header        `json:"headers"`
	Value       json.RawMessage `json:"value,omitempty"`
	ValueBase64 string          `json:"value_base64,omitempty"`
	DecodeError string          `json:"decode_error,omitempty"`
}

type header struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func (s *Server) messages(w http.ResponseWriter, r *http.Request) {
	messageType := r.URL.Query().Get("type")
	if messageType == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("query parameter type is required"))
		return
	}
	if err := s.codec.ValidateType(messageType); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	limit := 10
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			writeError(w, http.StatusBadRequest, fmt.Errorf("limit must be between 1 and 100"))
			return
		}
		limit = parsed
	}
	topic := r.PathValue("topic")
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	messages, err := s.broker.Recent(ctx, topic, limit)
	if err != nil {
		s.logger.Error("read failed", "topic", topic, "error", err)
		writeError(w, http.StatusBadGateway, err)
		return
	}
	records := make([]record, 0, len(messages))
	for _, message := range messages {
		value, err := s.codec.Decode(messageType, message.Value)
		rec := record{
			Partition: message.Partition,
			Offset:    message.Offset,
			Time:      message.Time,
			Key:       string(message.Key),
			Headers:   make([]header, 0, len(message.Headers)),
		}
		for _, h := range message.Headers {
			value := string(h.Value)
			if !utf8.Valid(h.Value) {
				value = "base64:" + base64.StdEncoding.EncodeToString(h.Value)
			}
			rec.Headers = append(rec.Headers, header{Name: h.Key, Value: value})
		}
		if err != nil {
			rec.DecodeError = err.Error()
			rec.ValueBase64 = base64.StdEncoding.EncodeToString(message.Value)
		} else {
			rec.Value = value
		}
		records = append(records, rec)
	}
	writeJSON(w, http.StatusOK, map[string]any{"topic": topic, "messages": records})
}
