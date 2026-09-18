package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ArtyomRytikov/posts-comments-service/internal/api"
	"github.com/ArtyomRytikov/posts-comments-service/internal/config"
	"github.com/ArtyomRytikov/posts-comments-service/internal/pubsub"
	"github.com/ArtyomRytikov/posts-comments-service/internal/repository"
	"github.com/ArtyomRytikov/posts-comments-service/internal/service"
	"github.com/ArtyomRytikov/posts-comments-service/internal/storage/memory"
	"github.com/ArtyomRytikov/posts-comments-service/internal/storage/postgres"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	migrateOnly := flag.Bool("migrate-only", false, "apply PostgreSQL migrations and exit")
	healthcheck := flag.Bool("healthcheck", false, "check local readiness and exit")
	flag.Parse()
	if *healthcheck {
		return checkHealth()
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var repo repository.Repository
	ready := func(context.Context) error { return nil }
	switch cfg.Storage {
	case "memory":
		if *migrateOnly {
			return fmt.Errorf("-migrate-only requires STORAGE=postgres")
		}
		repo = memory.New()
	case "postgres":
		store, err := postgres.New(startup, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer store.Close()
		if cfg.AutoMigrate || *migrateOnly {
			if err := store.Migrate(startup); err != nil {
				return err
			}
		}
		if *migrateOnly {
			logger.Info("migrations applied")
			return nil
		}
		repo, ready = store, store.Ping
	}
	cancel()
	broker := pubsub.New(64)
	defer broker.Close()
	mux := http.NewServeMux()
	mux.Handle("/", api.New(service.New(repo, broker), logger))
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		check, done := context.WithTimeout(r.Context(), 2*time.Second)
		defer done()
		if err := ready(check); err != nil {
			http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	srv := &http.Server{
		Addr: cfg.Address, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
		// Shutdown does not close hijacked WebSockets; cancellation propagates
		// to their operation contexts via BaseContext.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	errorsCh := make(chan error, 1)
	go func() { errorsCh <- srv.ListenAndServe() }()
	logger.Info("server starting", "address", cfg.Address, "storage", cfg.Storage)
	select {
	case err := <-errorsCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		broker.Close()
		shutdown, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if err := srv.Shutdown(shutdown); err != nil {
			_ = srv.Close()
			return err
		}
		logger.Info("server stopped gracefully")
		return nil
	}
}

func checkHealth() error {
	address := os.Getenv("HTTP_ADDR")
	if address == "" {
		address = ":8080"
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get("http://" + net.JoinHostPort(host, port) + "/readyz")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("readiness returned %d", response.StatusCode)
	}
	return nil
}
