package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"sort"

	"github.com/repzspb/raf/internal/message/model"
	"github.com/repzspb/raf/internal/message/usecase"
)

const maxBodyBytes = 1 << 20

func (h *Handler) publish(
	w http.ResponseWriter,
	r *http.Request,
) {
	topic := r.PathValue("topic")
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
	headers, err := requestHeaders(r.Header.Get("X-Raf-Headers"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	message, err := h.service.Publish(r.Context(), usecase.PublishInput{
		Topic:   topic,
		Type:    r.URL.Query().Get("type"),
		Key:     []byte(r.URL.Query().Get("key")),
		Headers: headers,
		JSON:    value,
	})
	if err != nil {
		h.writeServiceError(w, "publish failed", topic, err)
		return
	}
	h.logger.Info("message published", "topic", topic, "partition", message.Partition, "offset", message.Offset)
	writeJSON(w, http.StatusCreated, map[string]any{
		"topic":     topic,
		"partition": message.Partition,
		"offset":    message.Offset,
	})
}

func requestHeaders(raw string) ([]model.Header, error) {
	if raw == "" {
		return nil, nil
	}
	var values map[string]*string
	if err := json.Unmarshal([]byte(raw), &values); err != nil || values == nil {
		return nil, fmt.Errorf("X-Raf-Headers must be a JSON object of string values")
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	headers := make([]model.Header, 0, len(names))
	for _, name := range names {
		if values[name] == nil {
			return nil, fmt.Errorf("X-Raf-Headers must be a JSON object of string values")
		}
		headers = append(headers, model.Header{
			Key:   name,
			Value: []byte(*values[name]),
		})
	}
	return headers, nil
}
