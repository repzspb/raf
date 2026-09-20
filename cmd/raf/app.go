package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/repzspb/raf/internal/config"
	"github.com/repzspb/raf/internal/httpapi"
	"github.com/repzspb/raf/internal/kafka"
	"github.com/repzspb/raf/internal/protobuf"
)

func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, nil))
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	codec, err := protobuf.New(ctx, cfg.ProtoFiles, cfg.ProtoImportPaths)
	if err != nil {
		return err
	}
	for topic, messageType := range cfg.TopicTypes {
		if err := codec.ValidateType(messageType); err != nil {
			return fmt.Errorf("RAF_TOPIC_TYPES topic %q: %w", topic, err)
		}
	}
	broker := kafka.New(cfg.KafkaBrokers)
	requestCtx, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()
	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.New(broker, codec, logger, cfg.TopicTypes),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      20 * time.Second,
		IdleTimeout:       60 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return requestCtx },
	}
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = broker.Close(closeCtx)
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	logger.Info("raf listening", "address", cfg.HTTPAddr)
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	select {
	case err = <-serveErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			err = fmt.Errorf("serve HTTP: %w", err)
		}
	case <-ctx.Done():
		err = nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stop() // restore normal signal handling so another interrupt can force exit
	shutdownErr := server.Shutdown(shutdownCtx)
	cancelRequests()
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, server.Close())
	}
	return errors.Join(err, shutdownErr, broker.Close(shutdownCtx))
}
