package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/repzspb/raf/internal/config"
	httpapi "github.com/repzspb/raf/internal/message/adapter/http"
	"github.com/repzspb/raf/internal/message/adapter/kafka"
	"github.com/repzspb/raf/internal/message/adapter/protobuf"
	"github.com/repzspb/raf/internal/message/usecase"
)

// brokerCloser предоставляет приложению операцию завершения работы с брокером.
type brokerCloser interface {
	// Close ожидает завершения публикаций и освобождает ресурсы.
	// Отмена ctx ограничивает ожидание; повторный вызов позволяет дождаться завершения.
	Close(context.Context) error
}

// App владеет HTTP-сервером и ресурсами брокера.
// Кодек и сценарии хранятся в компонентах, которые их используют.
type App struct {
	// logger записывает события работы приложения.
	logger *slog.Logger
	// broker предоставляет закрытие ресурсов, которыми владеет приложение.
	broker brokerCloser
	// server принимает HTTP-запросы и управляет их плавным завершением.
	server *http.Server
	// started запрещает повторный или одновременный вызов Run.
	started atomic.Bool
}

func New(
	ctx context.Context,
	cfg config.Config,
	logger *slog.Logger,
) (*App, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	codec, err := protobuf.New(ctx, cfg.ProtoFiles, cfg.ProtoImportPaths)
	if err != nil {
		return nil, err
	}
	broker := kafka.New(cfg.KafkaBrokers)
	service, err := usecase.New(broker, broker, broker, codec, cfg.TopicTypes)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return nil, errors.Join(fmt.Errorf("topic configuration (RAF_TOPIC_TYPES / topics): %w", err), broker.Close(closeCtx))
	}
	return &App{
		logger: logger,
		broker: broker,
		server: &http.Server{
			Addr:              cfg.HTTPAddr,
			Handler:           httpapi.New(service, logger),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      20 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}, nil
}

// Run запускается один раз и закрывает ресурсы при любом исходе, включая ошибку запуска.
// Отмена ctx начинает плавную остановку; активным запросам даётся время завершиться.
func (a *App) Run(ctx context.Context) (err error) {
	if !a.started.CompareAndSwap(false, true) {
		return fmt.Errorf("application has already been run")
	}
	requestCtx, cancelRequests := context.WithCancel(context.Background())
	a.server.BaseContext = func(net.Listener) context.Context { return requestCtx }
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err = errors.Join(err, a.shutdown(shutdownCtx, cancelRequests))
	}()
	if ctx.Err() != nil {
		return nil
	}
	listener, err := net.Listen("tcp", a.server.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", a.server.Addr, err)
	}
	a.logger.Info("raf listening", "address", listener.Addr().String())
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- a.server.Serve(listener) }()
	select {
	case err := <-serveErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	case <-ctx.Done():
		return nil
	}
}

func (a *App) shutdown(
	ctx context.Context,
	cancelRequests context.CancelFunc,
) error {
	shutdownErr := a.server.Shutdown(ctx)
	cancelRequests()
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, a.server.Close())
	}
	return errors.Join(shutdownErr, a.broker.Close(ctx))
}
