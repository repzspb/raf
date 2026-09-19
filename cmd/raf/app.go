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
	broker := kafka.New(cfg.KafkaBrokers)
	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.New(broker, codec, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		_ = broker.Close()
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
	return errors.Join(err, server.Shutdown(shutdownCtx), broker.Close())
}
