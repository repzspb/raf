package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"

	"github.com/repzspb/raf/internal/protobuf"
	kafkago "github.com/segmentio/kafka-go"
)

type Broker interface {
	Publish(ctx context.Context, topic, key string, headers []kafkago.Header, value []byte) (kafkago.Message, error)
	Recent(ctx context.Context, topic string, limit int) ([]kafkago.Message, error)
}

type Server struct {
	broker     Broker
	codec      *protobuf.Codec
	logger     *slog.Logger
	topicTypes map[string]string
}

func New(broker Broker, codec *protobuf.Codec, logger *slog.Logger, topicTypes map[string]string) http.Handler {
	server := &Server{broker: broker, codec: codec, logger: logger, topicTypes: topicTypes}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /topics/{topic}/messages", server.publish)
	mux.HandleFunc("GET /topics/{topic}/messages", server.messages)
	mux.HandleFunc("GET /types", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"types": codec.MessageTypes()})
	})
	return mux
}

func (s *Server) messageType(r *http.Request) (string, error) {
	name := r.URL.Query().Get("type")
	if name == "" {
		name = s.topicTypes[r.PathValue("topic")]
	}
	if name == "" {
		return "", fmt.Errorf("query parameter type is required; configure RAF_TOPIC_TYPES for a default or see GET /types for loaded types")
	}
	if err := s.codec.ValidateType(name); err != nil {
		return "", fmt.Errorf("%w; see GET /types for loaded types", err)
	}
	return name, nil
}

func writeBrokerError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	var networkError net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kafkago.RequestTimedOut) ||
		(errors.As(err, &networkError) && networkError.Timeout()) {
		status = http.StatusGatewayTimeout
	}
	writeError(w, status, err)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
