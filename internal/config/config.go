package config

import "os"

type Config struct {
	Port         string
	DATABASE_URL string
	REDIS_ADDR   string
}

func Load() Config {
	port := os.Getenv("PORT")
	databaseURL := os.Getenv("DATABASE_URL")
	redisAddr := os.Getenv("REDIS_ADDR")

	if port == "" {
		port = "8080"
	}

	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/payflow?sslmode=disable"
	}

	redis_addr := os.Getenv("REDIS_ADDR")
	if redis_addr == "" {
		redis_addr = "localhost:6379"
	}

	return Config{
		Port:         port,
		DATABASE_URL: databaseURL,
		REDIS_ADDR:   redisAddr,
	}
}
