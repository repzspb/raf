package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/repzspb/raf/internal/protobuf"
	kafkago "github.com/segmentio/kafka-go"
)

type Broker interface {
	Publish(ctx context.Context, topic, key string, headers []kafkago.Header, value []byte) (kafkago.Message, error)
	Recent(ctx context.Context, topic string, limit int) ([]kafkago.Message, error)
}

type Server struct {
	broker Broker
	codec  *protobuf.Codec
	logger *slog.Logger
}

func New(broker Broker, codec *protobuf.Codec, logger *slog.Logger) http.Handler {
	server := &Server{broker: broker, codec: codec, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /topics/{topic}/messages", server.publish)
	mux.HandleFunc("GET /topics/{topic}/messages", server.messages)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
