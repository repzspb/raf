package httpapi

import "net/http"

func (h *Handler) types(
	w http.ResponseWriter,
	r *http.Request,
) {
	writeJSON(w, http.StatusOK, map[string]any{"types": h.service.ListTypes()})
}
