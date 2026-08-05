package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ravikirankb/payflow/internal/config"
	"github.com/ravikirankb/payflow/internal/logger"

	"github.com/ravikirankb/payflow/internal/server"
)

func main() {
	logger.Init()

	slog.Info("Payflow Starting..!")

	cfg := config.Load()

	slog.Info("Payflow starting", "port", cfg.Port)

	srv := server.New(cfg.Port)

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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	// Ensure resources are released when the main function exits
	defer cancel()

	err := srv.ShutDown(ctx)
	if err != nil {
		slog.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}

	slog.Info("Shutdown complete. Exiting cleanly.")
}
