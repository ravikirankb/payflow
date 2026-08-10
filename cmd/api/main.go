package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ravikirankb/payflow/internal/config"
	"github.com/ravikirankb/payflow/internal/database"
	"github.com/ravikirankb/payflow/internal/handlers"
	"github.com/ravikirankb/payflow/internal/logger"
	"github.com/ravikirankb/payflow/internal/middleware"
	"github.com/ravikirankb/payflow/internal/repository"
	"github.com/ravikirankb/payflow/internal/server"
	"github.com/ravikirankb/payflow/internal/service"
)

func main() {
	logger.Init()

	slog.Info("Payflow Starting..!")

	cfg := config.Load()

	db, err := database.Init(cfg.DATABASE_URL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	slog.Info("database connected")

	paymentRepo := repository.NewPaymentRepository(db)
	idempotencyRepo := repository.NewIdempotencyRepository(db)
	paymentService := service.NewPaymentService(db, paymentRepo, idempotencyRepo)

	repoCtx, repoCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer repoCancel()

	if err := paymentRepo.Ping(repoCtx); err != nil {
		slog.Error("repository ping failed", "error", err)
		os.Exit(1)
	}

	slog.Info("repository initialized")

	slog.Info("Payflow starting", "port", cfg.Port)

	mux := http.NewServeMux()
	healthHandler := middleware.Recovery(
		middleware.RequestID(
			middleware.Logging(http.HandlerFunc(healthHandler)),
		),
	)

	mux.Handle("/health", healthHandler)
	mux.HandleFunc("/ready", handlers.Ready(paymentRepo))
	mux.HandleFunc("/payments", handlers.CreatePayment(paymentService))

	srv := server.New(cfg.Port, mux)

	fmt.Printf("Server starting on port: %s\n", cfg.Port)

	go func() {
		err := srv.Start()
		if err != nil {
			slog.Error("server failed to start", "error", err)
		}
	}()

	// 1. Create a buffered channel to receive os.Signal notifications.
	// A buffer size of 1 is recommended to prevent missing signals.
	sigChan := make(chan os.Signal, 1)

	// 2. Register the channel to receive specific OS signals.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	slog.Info("Application started. Waiting for SIGINT (Ctrl+C) or SIGTERM...")

	// 3. Block until a signal is received.
	sig := <-sigChan

	// 4. Handle the received signal.
	slog.Info("\nReceived signal: %v. Initiating graceful shutdown...\n", "signal", sig)

	// Create a context with a 5-second timeout
	shutDownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)

	// Ensure resources are released when the main function exits
	defer shutdownCancel()

	if err := srv.ShutDown(shutDownCtx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}

	slog.Info("Shutdown complete. Exiting cleanly.")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
