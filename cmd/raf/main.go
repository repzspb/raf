package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/repzspb/raf/internal/app"
	"github.com/repzspb/raf/internal/config"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("raf stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		// После первого сигнала возвращаем стандартную обработку: второй завершит процесс.
		stop()
	}()
	application, err := app.New(ctx, cfg, logger)
	if err != nil {
		return err
	}
	return application.Run(ctx)
}
