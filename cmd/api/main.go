package main

import (
	"context"
	"fmt"
	"log"
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
	"github.com/ravikirankb/payflow/internal/messaging"
	"github.com/ravikirankb/payflow/internal/middleware"
	"github.com/ravikirankb/payflow/internal/repository"
	"github.com/ravikirankb/payflow/internal/server"
	"github.com/ravikirankb/payflow/internal/service"
	"github.com/ravikirankb/payflow/internal/worker"
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
	outboxRepo := repository.NewOutboxRepository(db)

	paymentService := service.NewPaymentService(
		db,
		paymentRepo,
		idempotencyRepo,
		outboxRepo,
	)

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

	shutdownCtx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	producer, err := messaging.NewKafkaProducer(
		[]string{"localhost:9092"},
	)
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	defer producer.Close()

	publisher := worker.NewOutboxPublisher(
		outboxRepo,
		producer,
	)

	go publisher.Start(shutdownCtx)

	slog.Info("Application started. Waiting for shutdown signal...")

	<-shutdownCtx.Done()

	slog.Info("Shutdown signal received. Stopping server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.ShutDown(ctx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}

	slog.Info("Shutdown complete. Exiting cleanly.")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
