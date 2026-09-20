package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/repzspb/raf/internal/message/model"
	"github.com/repzspb/raf/internal/message/usecase"
)

// Service описывает сценарии, доступные через HTTP.
type Service interface {
	// ListTopics возвращает топики брокера с настроенными в raf типами.
	ListTopics(ctx context.Context) ([]usecase.TopicSummary, error)

	// DescribeTopic возвращает настройки raf и границы партиций указанного топика.
	DescribeTopic(
		ctx context.Context,
		name string,
	) (usecase.TopicDetails, error)

	// Example возвращает готовое JSON-тело для публикации сообщения указанного типа.
	Example(
		ctx context.Context,
		name string,
	) ([]byte, error)

	// Publish выбирает контракт, кодирует JSON и возвращает подтверждённую позицию записи.
	// Явный тип в input имеет приоритет над настройкой топика.
	Publish(
		context.Context,
		usecase.PublishInput,
	) (model.Position, error)

	// Inspect читает и декодирует последние сообщения с общим ограничением input.Limit.
	// Ошибка декодирования сохраняется в отдельной записи; ошибка чтения прерывает сценарий.
	Inspect(
		context.Context,
		usecase.InspectInput,
	) ([]usecase.InspectedMessage, error)

	// ListTypes возвращает отсортированные полные имена загруженных типов без обращения к брокеру.
	ListTypes() []string
}

// Handler связывает HTTP-маршруты со сценариями и формирует ответы клиенту.
type Handler struct {
	// service выполняет операции с сообщениями независимо от HTTP.
	service Service
	// logger записывает результаты публикации и ошибки выполнения запросов.
	logger *slog.Logger
}

func New(
	service Service,
	logger *slog.Logger,
) http.Handler {
	handler := &Handler{
		service: service,
		logger:  logger,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /topics/{topic}/messages", handler.publish)
	mux.HandleFunc("GET /topics/{topic}/messages", handler.inspect)
	mux.HandleFunc("GET /types", handler.types)
	mux.HandleFunc("GET /types/{type}/example", handler.example)
	mux.HandleFunc("GET /topics", handler.topics)
	mux.HandleFunc("GET /topics/{topic}", handler.topic)
	return mux
}
