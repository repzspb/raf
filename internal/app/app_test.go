package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/repzspb/raf/internal/config"
)

// closerStub фиксирует закрытие брокера при проверке жизненного цикла приложения.
type closerStub struct {
	// closed передаёт тесту контекст, с которым был вызван Close.
	closed chan context.Context
	// err возвращается из Close для проверки обработки ошибок остановки.
	err error
}

func (c *closerStub) Close(ctx context.Context) error {
	c.closed <- ctx
	return c.err
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewWiresContractsAndValidatesConfiguration(t *testing.T) {
	cfg := config.Config{
		HTTPAddr:         "127.0.0.1:0",
		KafkaBrokers:     []string{"localhost:9092"},
		ProtoImportPaths: []string{"../../examples/proto"},
		ProtoFiles:       []string{"event.proto"},
		TopicTypes:       map[string]string{"events": "example.Event"},
	}
	application, err := New(t.Context(), cfg, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	defer func() {
		if err := application.Run(ctx); err != nil {
			t.Error(err)
		}
	}()
	recorder := httptest.NewRecorder()
	application.server.Handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/types", nil))
	if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), "example.Event") {
		t.Fatalf("types response: %d %s", recorder.Code, recorder.Body)
	}
	cfg.TopicTypes["events"] = "unknown.Type"
	if _, err := New(t.Context(), cfg, testLogger()); err == nil {
		t.Fatal("unknown configured type accepted")
	}
	if _, err := New(t.Context(), config.Config{}, testLogger()); err == nil {
		t.Fatal("empty configuration accepted")
	}
}

func TestRunClosesBrokerOnStartupFailure(t *testing.T) {
	closeError := errors.New("close failed")
	broker := &closerStub{
		closed: make(chan context.Context, 1),
		err:    closeError,
	}
	application := &App{
		logger: testLogger(),
		broker: broker,
		server: &http.Server{Addr: "127.0.0.1:-1"},
	}
	err := application.Run(t.Context())
	if !errors.Is(err, closeError) || !strings.Contains(err.Error(), "listen on") {
		t.Fatalf("error = %v", err)
	}
	select {
	case ctx := <-broker.closed:
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("close has no deadline")
		}
	default:
		t.Fatal("broker was not closed")
	}
	if err := application.Run(t.Context()); err == nil {
		t.Fatal("second Run accepted")
	}
}

// addressHandler извлекает адрес запущенного сервера из лога для HTTP-проверок.
type addressHandler struct {
	// Handler обрабатывает записи лога после извлечения адреса.
	slog.Handler
	// address передаёт тесту фактический адрес, включая автоматически выбранный порт.
	address chan string
}

func (h addressHandler) Handle(
	ctx context.Context,
	record slog.Record,
) error {
	if record.Message == "raf listening" {
		record.Attrs(func(attr slog.Attr) bool {
			if attr.Key == "address" {
				h.address <- attr.Value.String()
			}
			return true
		})
	}
	return h.Handler.Handle(ctx, record)
}

func TestRunDrainsActiveRequestsBeforeClosingBroker(t *testing.T) {
	addresses := make(chan string, 1)
	broker := &closerStub{closed: make(chan context.Context, 1)}
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	finishRequest := func() { releaseOnce.Do(func() { close(release) }) }
	shutdownStarted := make(chan struct{})
	requestContext := make(chan context.Context, 1)
	application := &App{
		logger: slog.New(addressHandler{
			Handler: testLogger().Handler(),
			address: addresses,
		}),
		broker: broker,
		server: &http.Server{
			Addr: "127.0.0.1:0",
			Handler: http.HandlerFunc(func(
				w http.ResponseWriter,
				r *http.Request,
			) {
				requestContext <- r.Context()
				close(entered)
				<-release
				w.WriteHeader(http.StatusNoContent)
			}),
		},
	}
	application.server.RegisterOnShutdown(func() { close(shutdownStarted) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer finishRequest()
	done := make(chan error, 1)
	go func() { done <- application.Run(ctx) }()
	var address string
	select {
	case address = <-addresses:
	case err := <-done:
		t.Fatalf("startup failed: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("startup timeout")
	}
	response := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 3 * time.Second}
		res, err := client.Get("http://" + address)
		if err == nil {
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				err = errors.New("unexpected status")
			}
		}
		response <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not arrive")
	}
	cancel()
	select {
	case <-shutdownStarted:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not start")
	}
	if reqCtx := <-requestContext; reqCtx.Err() != nil {
		t.Fatal("active request was cancelled before the grace period")
	}
	select {
	case <-broker.closed:
		t.Fatal("broker closed before active request finished")
	default:
	}
	finishRequest()
	if err := <-response; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	select {
	case <-broker.closed:
	default:
		t.Fatal("broker was not closed")
	}
}

func TestShutdownDeadlineCancelsRequestsAndReachesBroker(t *testing.T) {
	requestCtx, cancelRequests := context.WithCancel(t.Context())
	entered := make(chan struct{})
	finished := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		close(entered)
		<-requestCtx.Done()
		close(finished)
	}))
	server.Start()
	defer func() {
		cancelRequests()
		server.Close()
	}()
	responseDone := make(chan struct{})
	go func() {
		defer close(responseDone)
		response, err := server.Client().Get(server.URL)
		if err == nil {
			response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not arrive")
	}
	broker := &closerStub{closed: make(chan context.Context, 1)}
	application := &App{
		server: server.Config,
		broker: broker,
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	defer cancelRequests()
	if err := application.shutdown(ctx, cancelRequests); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	if got := <-broker.closed; got != ctx {
		t.Fatal("broker did not receive the shared deadline")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("request context was not cancelled")
	}
	<-responseDone
}
