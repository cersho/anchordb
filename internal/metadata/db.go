package metadata

import (
	"fmt"

	"anchordb/internal/config"
	"anchordb/internal/models"

	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func Open(cfg config.Config) (*gorm.DB, error) {
	switch cfg.MetadataDriver {
	case "sqlite":
		return gorm.Open(sqlite.Open(cfg.MetadataURL), &gorm.Config{})
	case "postgres":
		return gorm.Open(postgres.Open(cfg.MetadataURL), &gorm.Config{})
	default:
		return nil, fmt.Errorf("unsupported metadata driver: %s", cfg.MetadataDriver)
	}
}

func Migrate(db *gorm.DB) error {
	return db.AutoMigrate(&models.Connection{}, &models.Remote{}, &models.Backup{}, &models.BackupRun{})
}
