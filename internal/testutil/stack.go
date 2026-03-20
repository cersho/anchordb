package testutil

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"anchordb/internal/config"
	"anchordb/internal/crypto"
	"anchordb/internal/metadata"
	"anchordb/internal/models"
	"anchordb/internal/repository"
	"anchordb/internal/scheduler"

	glebarezsqlite "github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type Stack struct {
	Config    config.Config
	DB        *gorm.DB
	Repo      *repository.Repository
	Scheduler *scheduler.Scheduler
}

func NewStack(t *testing.T) Stack {
	t.Helper()

	metadataPath := filepath.Join(t.TempDir(), "metadata.db")
	backupPath := filepath.Join(t.TempDir(), "backups")

	cfg := config.Config{
		HTTPAddr:             ":0",
		MetadataDriver:       "sqlite",
		MetadataURL:          metadataPath,
		SecretKey:            "test-secret",
		MaxConcurrentBackups: 2,
		BackupCommandTimeout: 5 * time.Second,
		DefaultLocalBasePath: backupPath,
	}

	db, err := gorm.Open(glebarezsqlite.Open(cfg.MetadataURL), &gorm.Config{})
	if err != nil {
		t.Fatalf("open metadata db: %v", err)
	}

	if err := metadata.Migrate(db); err != nil {
		t.Fatalf("migrate metadata db: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("open sql db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	repo := repository.New(db, crypto.New(cfg.SecretKey))
	exec := scheduler.NewExecutor(cfg)
	sch := scheduler.New(repo, exec, cfg)
	sch.Start()
	t.Cleanup(sch.Stop)

	return Stack{Config: cfg, DB: db, Repo: repo, Scheduler: sch}
}

func MustCreateConnection(t *testing.T, repo *repository.Repository, name string) models.Connection {
	t.Helper()

	item := models.Connection{
		Name:     name,
		Type:     "postgres",
		Host:     "localhost",
		Port:     5432,
		Database: "appdb",
		Username: "postgres",
		Password: "postgres",
		SSLMode:  "disable",
	}

	if err := repo.CreateConnection(context.Background(), &item); err != nil {
		t.Fatalf("create connection: %v", err)
	}

	return item
}

func MustCreateRemote(t *testing.T, repo *repository.Repository, name string) models.Remote {
	t.Helper()

	item := models.Remote{
		Name:      name,
		Provider:  "s3",
		Bucket:    "anchordb-backups",
		Region:    "us-east-1",
		AccessKey: "access",
		SecretKey: "secret",
	}

	if err := repo.CreateRemote(context.Background(), &item); err != nil {
		t.Fatalf("create remote: %v", err)
	}

	return item
}

func MustCreateLocalBackup(t *testing.T, repo *repository.Repository, name, connectionID, localPath string, enabled bool) models.Backup {
	t.Helper()

	item := models.Backup{
		Name:          name,
		ConnectionID:  connectionID,
		CronExpr:      "*/5 * * * *",
		Timezone:      "UTC",
		Enabled:       enabled,
		TargetType:    "local",
		LocalPath:     localPath,
		RetentionDays: 7,
		Compression:   "gzip",
	}

	if err := repo.CreateBackup(context.Background(), &item); err != nil {
		t.Fatalf("create local backup: %v", err)
	}

	fresh, err := repo.GetBackup(context.Background(), item.ID)
	if err != nil {
		t.Fatalf("read local backup: %v", err)
	}

	return fresh
}

func MustCreateS3Backup(t *testing.T, repo *repository.Repository, name, connectionID, remoteID string) models.Backup {
	t.Helper()

	item := models.Backup{
		Name:          name,
		ConnectionID:  connectionID,
		CronExpr:      "*/10 * * * *",
		Timezone:      "UTC",
		Enabled:       true,
		TargetType:    "s3",
		RemoteID:      &remoteID,
		RetentionDays: 7,
		Compression:   "gzip",
	}

	if err := repo.CreateBackup(context.Background(), &item); err != nil {
		t.Fatalf("create s3 backup: %v", err)
	}

	fresh, err := repo.GetBackup(context.Background(), item.ID)
	if err != nil {
		t.Fatalf("read s3 backup: %v", err)
	}

	return fresh
}

func MustCreateFinishedRun(t *testing.T, repo *repository.Repository, backupID, outputKey string) models.BackupRun {
	t.Helper()

	run, err := repo.StartBackupRun(context.Background(), backupID)
	if err != nil {
		t.Fatalf("start backup run: %v", err)
	}

	if err := repo.FinishBackupRun(context.Background(), run.ID, "success", "", outputKey); err != nil {
		t.Fatalf("finish backup run: %v", err)
	}

	fresh, err := repo.GetBackupRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("read backup run: %v", err)
	}

	if fresh.Status != "success" {
		t.Fatalf("run status mismatch: got %s", fresh.Status)
	}

	return fresh
}

func UniqueName(prefix string, i int) string {
	return fmt.Sprintf("%s-%d", prefix, i)
}
