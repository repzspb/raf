package httpapi

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/repzspb/raf/internal/message/usecase"
)

func (h *Handler) inspect(
	w http.ResponseWriter,
	r *http.Request,
) {
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
	messages, err := h.service.Inspect(r.Context(), usecase.InspectInput{
		Topic: topic,
		Type:  r.URL.Query().Get("type"),
		Limit: limit,
	})
	if err != nil {
		h.writeServiceError(w, "read failed", topic, err)
		return
	}
	records := make([]record, 0, len(messages))
	for _, message := range messages {
		records = append(records, messageRecord(message))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"topic":    topic,
		"messages": records,
	})
}
