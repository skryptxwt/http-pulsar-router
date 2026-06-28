package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sr-forwarder/internal/config"
	"sr-forwarder/internal/forwarder"
	"sr-forwarder/internal/pulsarpub"
)

func main() {
	configPath := flag.String("config", "config.json", "path to JSON config file")
	reloadInterval := flag.Duration("reload-interval", 2*time.Second, "config reload polling interval")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	store, err := config.NewStore(*configPath)
	if err != nil {
		logger.Fatalf("load config: %v", err)
	}

	cfg := store.Snapshot()
	publisher, err := pulsarpub.New(cfg.Pulsar)
	if err != nil {
		logger.Fatalf("create pulsar publisher: %v", err)
	}
	defer publisher.Close()

	mux := http.NewServeMux()
	handler := forwarder.NewHandler(store, publisher, cfg.Server, logger)
	handler.Register(mux)

	server := &http.Server{
		Addr:         cfg.Server.Addr,
		Handler:      mux,
		ReadTimeout:  cfg.Server.ReadTimeoutDuration(),
		WriteTimeout: cfg.Server.WriteTimeoutDuration(),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go store.Watch(ctx, *reloadInterval, logger)

	errCh := make(chan error, 1)
	go func() {
		logger.Printf("sr-forwarder listening on %s", cfg.Server.Addr)
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Println("shutdown signal received")
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			logger.Fatalf("http server: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeoutDuration())
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("graceful shutdown failed: %v", err)
	}
}
