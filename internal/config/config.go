package config

import "os"

type Config struct {
	Port         string
	DATABASE_URL string
}

func Load() Config {
	port := os.Getenv("PORT")
	database_url := os.Getenv("DATABASE_URL")

	if port == "" {
		port = "8080"
	}

	if database_url == "" {
		database_url = "postgres://postgres:postgres@localhost:5432/payflow?sslmode=disable"
	}

	return Config{
		Port:         port,
		DATABASE_URL: database_url,
	}
}
