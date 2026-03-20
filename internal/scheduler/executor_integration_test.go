package scheduler_test

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"anchordb/internal/config"
	"anchordb/internal/models"
	"anchordb/internal/scheduler"
)

func TestExecutorRunStoresCompressedLocalBackup(t *testing.T) {
	cmdDir := installFakeCommand(t, "pg_dump", "integration-dump")
	prependPath(t, cmdDir)

	root := t.TempDir()
	exec := scheduler.NewExecutor(config.Config{
		BackupCommandTimeout: 5 * time.Second,
		DefaultLocalBasePath: root,
	})

	backup := models.Backup{
		ID:          "backup-1",
		TargetType:  "local",
		Compression: "gzip",
		Connection: models.Connection{
			Name:     "Primary DB",
			Type:     "postgres",
			Host:     "localhost",
			Port:     5432,
			Database: "appdb",
			Username: "postgres",
			Password: "secret",
		},
	}

	key, err := exec.Run(context.Background(), backup)
	if err != nil {
		t.Fatalf("run executor: %v", err)
	}

	if !strings.HasSuffix(key, ".sql.gz") {
		t.Fatalf("expected gzip artifact key, got %q", key)
	}

	file, err := os.Open(filepath.Join(root, filepath.FromSlash(key)))
	if err != nil {
		t.Fatalf("open backup artifact: %v", err)
	}
	defer func() { _ = file.Close() }()

	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("open gzip stream: %v", err)
	}
	defer func() { _ = gz.Close() }()

	body, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("read gzip content: %v", err)
	}

	if !strings.Contains(string(body), "integration-dump") {
		t.Fatalf("unexpected dump content: %q", string(body))
	}
}

func TestExecutorCleanupRetentionRemovesOldFiles(t *testing.T) {
	root := t.TempDir()
	exec := scheduler.NewExecutor(config.Config{DefaultLocalBasePath: root})

	backup := models.Backup{
		TargetType:    "local",
		LocalPath:     root,
		RetentionDays: 1,
		Connection: models.Connection{
			Name: "Primary DB",
		},
	}

	oldPath := filepath.Join(root, "primary-db", "old.sql")
	newPath := filepath.Join(root, "primary-db", "new.sql")

	if err := os.MkdirAll(filepath.Dir(oldPath), 0o755); err != nil {
		t.Fatalf("create artifact directory: %v", err)
	}
	if err := os.WriteFile(oldPath, []byte("old"), 0o644); err != nil {
		t.Fatalf("write old artifact: %v", err)
	}
	if err := os.WriteFile(newPath, []byte("new"), 0o644); err != nil {
		t.Fatalf("write new artifact: %v", err)
	}

	oldTime := time.Now().Add(-72 * time.Hour)
	newTime := time.Now().Add(-1 * time.Hour)
	if err := os.Chtimes(oldPath, oldTime, oldTime); err != nil {
		t.Fatalf("set old artifact mtime: %v", err)
	}
	if err := os.Chtimes(newPath, newTime, newTime); err != nil {
		t.Fatalf("set new artifact mtime: %v", err)
	}

	if err := exec.CleanupRetention(context.Background(), backup); err != nil {
		t.Fatalf("cleanup retention: %v", err)
	}

	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("expected old artifact removed, stat err = %v", err)
	}
	if _, err := os.Stat(newPath); err != nil {
		t.Fatalf("expected new artifact to remain, got err %v", err)
	}
}

func TestExecutorRunStoresConvexExportArchive(t *testing.T) {
	cmdDir := installFakeNpxCommand(t, "convex-export")
	prependPath(t, cmdDir)

	root := t.TempDir()
	exec := scheduler.NewExecutor(config.Config{
		BackupCommandTimeout: 5 * time.Second,
		DefaultLocalBasePath: root,
	})

	backup := models.Backup{
		ID:                 "backup-convex",
		TargetType:         "local",
		Compression:        "gzip",
		IncludeFileStorage: true,
		Connection: models.Connection{
			Name:     "Convex Main",
			Type:     "convex",
			Host:     "https://convex.example",
			Password: "admin-key",
		},
	}

	key, err := exec.Run(context.Background(), backup)
	if err != nil {
		t.Fatalf("run convex executor: %v", err)
	}

	if !strings.HasSuffix(key, ".zip") {
		t.Fatalf("expected zip artifact key, got %q", key)
	}
	if strings.HasSuffix(key, ".gz") {
		t.Fatalf("did not expect gzip suffix for convex export key %q", key)
	}

	body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(key)))
	if err != nil {
		t.Fatalf("read convex backup artifact: %v", err)
	}
	if !strings.Contains(string(body), "convex-export") {
		t.Fatalf("unexpected convex export content: %q", string(body))
	}
}

func installFakeCommand(t *testing.T, name, output string) string {
	t.Helper()

	dir := t.TempDir()

	if runtime.GOOS == "windows" {
		path := filepath.Join(dir, name+".cmd")
		content := "@echo off\r\necho " + output + "\r\n"
		if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
			t.Fatalf("write fake windows command: %v", err)
		}
		return dir
	}

	path := filepath.Join(dir, name)
	content := "#!/bin/sh\necho " + output + "\n"
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write fake unix command: %v", err)
	}

	return dir
}

func installFakeNpxCommand(t *testing.T, output string) string {
	t.Helper()

	dir := t.TempDir()

	if runtime.GOOS == "windows" {
		path := filepath.Join(dir, "npx.cmd")
		content := "@echo off\r\nsetlocal\r\nset \"out=\"\r\n:loop\r\nif \"%~1\"==\"\" goto done\r\nif /I \"%~1\"==\"--path\" goto setpath\r\nshift\r\ngoto loop\r\n:setpath\r\nshift\r\nset \"out=%~1\"\r\nshift\r\ngoto loop\r\n:done\r\nif \"%out%\"==\"\" exit /b 1\r\n> \"%out%\" echo " + output + "\r\nexit /b 0\r\n"
		if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
			t.Fatalf("write fake windows npx command: %v", err)
		}
		return dir
	}

	path := filepath.Join(dir, "npx")
	content := "#!/bin/sh\nout=\"$4\"\nif [ -z \"$out\" ]; then\n  exit 1\nfi\nprintf '%s\n' \"" + output + "\" > \"$out\"\n"
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write fake unix npx command: %v", err)
	}

	return dir
}

func prependPath(t *testing.T, dir string) {
	t.Helper()

	original := os.Getenv("PATH")
	updated := dir + string(os.PathListSeparator) + original

	if err := os.Setenv("PATH", updated); err != nil {
		t.Fatalf("update PATH: %v", err)
	}
	t.Cleanup(func() { _ = os.Setenv("PATH", original) })
}
