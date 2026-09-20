package httpapi

import (
	"net/http"

	"github.com/repzspb/raf/internal/message/model"
)

// topicSummary задаёт представление топика и настроенного контракта в HTTP.
type topicSummary struct {
	// Name — имя существующего топика Kafka.
	Name string `json:"name"`
	// Internal отмечает служебные топики Kafka.
	Internal bool `json:"internal"`
	// Type — контракт по умолчанию в raf; null означает, что он не настроен.
	Type *string `json:"type"`
}

// topicDetails дополняет общие сведения диапазонами партиций.
type topicDetails struct {
	// topicSummary содержит общие поля HTTP-ответа.
	topicSummary
	// Partitions — партиции в порядке возрастания номера.
	Partitions []partitionDetails `json:"partitions"`
}

// partitionDetails описывает доступный диапазон offset в HTTP-ответе.
type partitionDetails struct {
	// ID — номер партиции.
	ID int `json:"id"`
	// FirstOffset — включённая нижняя граница.
	FirstOffset int64 `json:"first_offset"`
	// EndOffset — исключённая верхняя граница.
	EndOffset int64 `json:"end_offset"`
}

func topicResponse(
	topic model.TopicSummary,
	name string,
) topicSummary {
	var messageType *string
	if name != "" {
		messageType = &name
	}
	return topicSummary{
		Name:     topic.Name,
		Internal: topic.Internal,
		Type:     messageType,
	}
}

func (h *Handler) topics(
	w http.ResponseWriter,
	r *http.Request,
) {
	topics, err := h.service.ListTopics(r.Context())
	if err != nil {
		h.writeServiceError(w, "list topics failed", "", err)
		return
	}
	result := make([]topicSummary, 0, len(topics))
	for _, topic := range topics {
		result = append(result, topicResponse(topic.Topic, topic.Type))
	}
	writeJSON(w, http.StatusOK, map[string]any{"topics": result})
}

func (h *Handler) topic(
	w http.ResponseWriter,
	r *http.Request,
) {
	name := r.PathValue("topic")
	topic, err := h.service.DescribeTopic(r.Context(), name)
	if err != nil {
		h.writeServiceError(w, "describe topic failed", name, err)
		return
	}
	result := topicDetails{
		topicSummary: topicResponse(topic.Topic.TopicSummary, topic.Type),
		Partitions:   make([]partitionDetails, 0, len(topic.Topic.Partitions)),
	}
	for _, partition := range topic.Topic.Partitions {
		result.Partitions = append(result.Partitions, partitionDetails{
			ID:          partition.ID,
			FirstOffset: partition.FirstOffset,
			EndOffset:   partition.EndOffset,
		})
	}
	writeJSON(w, http.StatusOK, result)
}
