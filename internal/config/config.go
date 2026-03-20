package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr             string
	MetadataDriver       string
	MetadataURL          string
	SecretKey            string
	MaxConcurrentBackups int
	BackupCommandTimeout time.Duration
	DefaultLocalBasePath string
}

func Load() Config {
	return Config{
		HTTPAddr:             getenv("HTTP_ADDR", ":8080"),
		MetadataDriver:       getenv("METADATA_DRIVER", "sqlite"),
		MetadataURL:          getenv("METADATA_DB_URL", "anchordb.db"),
		SecretKey:            getenv("SECRET_KEY", "anchordb-dev-secret"),
		MaxConcurrentBackups: getenvInt("MAX_CONCURRENT_BACKUPS", 2),
		BackupCommandTimeout: getenvDuration("BACKUP_COMMAND_TIMEOUT", 2*time.Hour),
		DefaultLocalBasePath: getenv("DEFAULT_LOCAL_BACKUP_PATH", "./backups"),
	}
}

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
