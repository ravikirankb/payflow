package main

import (
	"fmt"

	"github.com/ravikirankb/payflow/internal/config"

	"github.com/ravikirankb/payflow/internal/server"
)

func main() {
	fmt.Println("Payflow Starting..!")

	cfg := config.Load()

	fmt.Println("Payflow starting on port %s", cfg.Port)

	srv := server.New(cfg.Port)

	err := srv.Start()

	fmt.Println("Server has started successfully on port %s\n", cfg.Port)

	if err != nil {
		fmt.Println("There was an error starting the server %v\n", err)
	}
}
