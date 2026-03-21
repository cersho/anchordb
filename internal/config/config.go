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
	CloudflareAccountID  string
	CloudflareDatabaseID string
	CloudflareAPIKey     string
	D1ExportLimit        int
	D1APIBaseURL         string
	MaxConcurrentBackups int
	BackupCommandTimeout time.Duration
	DefaultLocalBasePath string
	HealthPollInterval   time.Duration
	HealthDueBatchSize   int
	MaxConcurrentHealth  int
	DefaultHealthEvery   time.Duration
	DefaultHealthTimeout time.Duration
	DefaultFailThreshold int
	DefaultPassThreshold int
	HealthManualCooldown time.Duration
}

func Load() Config {
	return Config{
		HTTPAddr:             getenv("HTTP_ADDR", ":8080"),
		MetadataDriver:       getenv("METADATA_DRIVER", "sqlite"),
		MetadataURL:          getenv("METADATA_DB_URL", "anchordb.db"),
		SecretKey:            getenv("SECRET_KEY", "anchordb-dev-secret"),
		CloudflareAccountID:  getenv("CLOUDFLARE_ACCOUNT_ID", ""),
		CloudflareDatabaseID: getenv("CLOUDFLARE_DATABASE_ID", ""),
		CloudflareAPIKey:     getenv("CLOUDFLARE_API_KEY", ""),
		D1ExportLimit:        getenvInt("D1_EXPORT_LIMIT", 1000),
		D1APIBaseURL:         getenv("D1_API_BASE_URL", "https://api.cloudflare.com/client/v4"),
		MaxConcurrentBackups: getenvInt("MAX_CONCURRENT_BACKUPS", 2),
		BackupCommandTimeout: getenvDuration("BACKUP_COMMAND_TIMEOUT", 2*time.Hour),
		DefaultLocalBasePath: getenv("DEFAULT_LOCAL_BACKUP_PATH", "./backups"),
		HealthPollInterval:   getenvDuration("HEALTH_POLL_INTERVAL", 15*time.Second),
		HealthDueBatchSize:   getenvInt("HEALTH_DUE_BATCH_SIZE", 50),
		MaxConcurrentHealth:  getenvInt("MAX_CONCURRENT_HEALTH_CHECKS", 2),
		DefaultHealthEvery:   getenvDuration("DEFAULT_HEALTH_CHECK_INTERVAL", 1*time.Minute),
		DefaultHealthTimeout: getenvDuration("DEFAULT_HEALTH_CHECK_TIMEOUT", 5*time.Second),
		DefaultFailThreshold: getenvInt("DEFAULT_HEALTH_FAILURE_THRESHOLD", 3),
		DefaultPassThreshold: getenvInt("DEFAULT_HEALTH_SUCCESS_THRESHOLD", 1),
		HealthManualCooldown: getenvDuration("HEALTH_MANUAL_RUN_COOLDOWN", 10*time.Second),
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
