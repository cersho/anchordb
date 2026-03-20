package scheduler_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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

func TestExecutorRunStoresD1ExportSQL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}

		var payload struct {
			SQL string `json:"sql"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode d1 request: %v", err)
		}

		sql := payload.SQL
		switch {
		case strings.Contains(sql, "type = 'table' ORDER BY rootpage DESC"):
			writeD1Response(w, []map[string]any{{
				"name": "users",
				"type": "table",
				"sql":  "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
			}})
		case strings.Contains(sql, "SELECT * FROM \"users\" LIMIT 1"):
			writeD1Response(w, []map[string]any{{"id": 1, "name": "alpha"}})
		case strings.Contains(sql, "SELECT COUNT(*) AS count FROM \"users\""):
			writeD1Response(w, []map[string]any{{"count": 1}})
		case strings.Contains(sql, "AS partial_command"):
			if strings.Contains(sql, "OFFSET 0") {
				writeD1Response(w, []map[string]any{{"partial_command": "1, 'alpha'"}})
				return
			}
			writeD1Response(w, []map[string]any{})
		case strings.Contains(sql, "type IN ('index', 'trigger', 'view')"):
			writeD1Response(w, []map[string]any{})
		default:
			t.Fatalf("unexpected d1 sql: %s", sql)
		}
	}))
	t.Cleanup(server.Close)

	root := t.TempDir()
	exec := scheduler.NewExecutor(config.Config{
		BackupCommandTimeout: 5 * time.Second,
		DefaultLocalBasePath: root,
		D1APIBaseURL:         server.URL,
	})

	backup := models.Backup{
		ID:          "backup-d1",
		TargetType:  "local",
		Compression: "gzip",
		Connection: models.Connection{
			Name:     "D1 Main",
			Type:     "d1",
			Host:     "account-123",
			Database: "database-456",
			Password: "token",
		},
	}

	key, err := exec.Run(context.Background(), backup)
	if err != nil {
		t.Fatalf("run d1 executor: %v", err)
	}

	if !strings.HasSuffix(key, ".sql.gz") {
		t.Fatalf("expected gzip sql artifact key, got %q", key)
	}

	file, err := os.Open(filepath.Join(root, filepath.FromSlash(key)))
	if err != nil {
		t.Fatalf("open d1 backup artifact: %v", err)
	}
	defer func() { _ = file.Close() }()

	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("open d1 gzip stream: %v", err)
	}
	defer func() { _ = gz.Close() }()

	body, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("read d1 gzip content: %v", err)
	}

	content := string(body)
	if !strings.Contains(content, "CREATE TABLE IF NOT EXISTS users") {
		t.Fatalf("expected create table statement, got %q", content)
	}
	if !strings.Contains(content, "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'alpha');") {
		t.Fatalf("expected insert statement, got %q", content)
	}
}

func writeD1Response(w http.ResponseWriter, rows []map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"errors":  []any{},
		"result": []map[string]any{{
			"success": true,
			"results": rows,
		}},
	})
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
	content := "#!/bin/sh\nout=\"\"\nwhile [ $# -gt 0 ]; do\n  if [ \"$1\" = \"--path\" ]; then\n    shift\n    out=\"$1\"\n    break\n  fi\n  shift\ndone\nif [ -z \"$out\" ]; then\n  exit 1\nfi\nprintf '%s\n' \"" + output + "\" > \"$out\"\n"
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
