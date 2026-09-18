// Package config reads and validates process configuration.
package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Address     string
	Storage     string
	DatabaseURL string
	AutoMigrate bool
}

func Load() (Config, error) {
	c := Config{Address: value("HTTP_ADDR", ":8080"), Storage: value("STORAGE", "memory"), DatabaseURL: os.Getenv("DATABASE_URL")}
	var err error
	c.AutoMigrate, err = strconv.ParseBool(value("AUTO_MIGRATE", "true"))
	if err != nil {
		return Config{}, fmt.Errorf("AUTO_MIGRATE must be a boolean")
	}
	if c.Storage != "memory" && c.Storage != "postgres" {
		return Config{}, fmt.Errorf("STORAGE must be memory or postgres")
	}
	if c.Storage == "postgres" && c.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required for postgres storage")
	}
	return c, nil
}

func value(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
