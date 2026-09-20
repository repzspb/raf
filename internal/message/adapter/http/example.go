package httpapi

import (
	"encoding/json"
	"net/http"
)

func (h *Handler) example(
	w http.ResponseWriter,
	r *http.Request,
) {
	value, err := h.service.Example(r.Context(), r.PathValue("type"))
	if err != nil {
		h.writeServiceError(w, "generate example failed", "", err)
		return
	}
	writeJSON(w, http.StatusOK, json.RawMessage(value))
}
