package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"sort"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

const maxBodyBytes = 1 << 20

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	messageType := r.URL.Query().Get("type")
	if messageType == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("query parameter type is required"))
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, fmt.Errorf("Content-Type must be application/json"))
		return
	}
	value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeError(w, http.StatusRequestEntityTooLarge, err)
		} else {
			writeError(w, http.StatusBadRequest, err)
		}
		return
	}
	encoded, err := s.codec.Encode(messageType, value)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	headers, err := requestHeaders(r.Header.Get("X-Raf-Headers"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	message, err := s.broker.Publish(ctx, topic, r.URL.Query().Get("key"), headers, encoded)
	if err != nil {
		s.logger.Error("publish failed", "topic", topic, "error", err)
		writeError(w, http.StatusBadGateway, err)
		return
	}
	s.logger.Info("message published", "topic", topic, "partition", message.Partition, "offset", message.Offset)
	writeJSON(w, http.StatusCreated, map[string]any{
		"topic": topic, "partition": message.Partition, "offset": message.Offset,
	})
}

func requestHeaders(raw string) ([]kafkago.Header, error) {
	if raw == "" {
		return nil, nil
	}
	var values map[string]string
	if err := json.Unmarshal([]byte(raw), &values); err != nil || values == nil {
		return nil, fmt.Errorf("X-Raf-Headers must be a JSON object of string values")
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	headers := make([]kafkago.Header, 0, len(names))
	for _, name := range names {
		headers = append(headers, kafkago.Header{Key: name, Value: []byte(values[name])})
	}
	return headers, nil
}
