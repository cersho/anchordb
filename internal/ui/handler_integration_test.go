package ui_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"anchordb/internal/testutil"
	"anchordb/internal/ui"
)

func TestDownloadRunFromLocalTarget(t *testing.T) {
	stack := testutil.NewStack(t)
	h := ui.NewHandler(stack.Repo, stack.Scheduler, stack.Config)
	server := httptest.NewServer(h.Router())
	t.Cleanup(server.Close)

	backupRoot := t.TempDir()
	conn := testutil.MustCreateConnection(t, stack.Repo, "ui-download")
	backup := testutil.MustCreateLocalBackup(t, stack.Repo, "ui-download-local", conn.ID, backupRoot, true)

	outputKey := "ui-download/2026/03/20/run.sql.gz"
	fullPath := filepath.Join(backupRoot, filepath.FromSlash(outputKey))
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatalf("create output directory: %v", err)
	}
	if err := os.WriteFile(fullPath, []byte("backup-bytes"), 0o644); err != nil {
		t.Fatalf("write output file: %v", err)
	}

	run := testutil.MustCreateFinishedRun(t, stack.Repo, backup.ID, outputKey)

	res, err := http.Get(server.URL + "/runs/" + run.ID + "/download")
	if err != nil {
		t.Fatalf("download run artifact: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}

	if got := res.Header.Get("Content-Disposition"); !strings.Contains(got, "run.sql.gz") {
		t.Fatalf("expected content-disposition filename, got %q", got)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if string(body) != "backup-bytes" {
		t.Fatalf("unexpected download content: %q", string(body))
	}
}

func TestDownloadRunRejectsPathTraversal(t *testing.T) {
	stack := testutil.NewStack(t)
	h := ui.NewHandler(stack.Repo, stack.Scheduler, stack.Config)
	server := httptest.NewServer(h.Router())
	t.Cleanup(server.Close)

	backupRoot := t.TempDir()
	conn := testutil.MustCreateConnection(t, stack.Repo, "ui-traversal")
	backup := testutil.MustCreateLocalBackup(t, stack.Repo, "ui-traversal-local", conn.ID, backupRoot, true)
	run := testutil.MustCreateFinishedRun(t, stack.Repo, backup.ID, "../escape.sql")

	res, err := http.Get(server.URL + "/runs/" + run.ID + "/download")
	if err != nil {
		t.Fatalf("download traversal run artifact: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if !strings.Contains(string(body), "invalid backup path") {
		t.Fatalf("expected invalid backup path error, got %q", string(body))
	}
}

func TestCreateConvexConnectionFromUI(t *testing.T) {
	stack := testutil.NewStack(t)
	h := ui.NewHandler(stack.Repo, stack.Scheduler, stack.Config)
	server := httptest.NewServer(h.Router())
	t.Cleanup(server.Close)

	form := url.Values{}
	form.Set("name", "convex-ui")
	form.Set("type", "convex")
	form.Set("host", "https://convex.example")
	form.Set("password", "admin-key")

	res, err := http.PostForm(server.URL+"/connections", form)
	if err != nil {
		t.Fatalf("post convex connection form: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if !strings.Contains(string(body), "Connection created") {
		t.Fatalf("expected success message, got %q", string(body))
	}
}

func TestCreateD1ConnectionFromUI(t *testing.T) {
	stack := testutil.NewStack(t)
	h := ui.NewHandler(stack.Repo, stack.Scheduler, stack.Config)
	server := httptest.NewServer(h.Router())
	t.Cleanup(server.Close)

	form := url.Values{}
	form.Set("name", "d1-ui")
	form.Set("type", "d1")
	form.Set("host", "account-123")
	form.Set("database", "db-456")
	form.Set("password", "api-token")

	res, err := http.PostForm(server.URL+"/connections", form)
	if err != nil {
		t.Fatalf("post d1 connection form: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if !strings.Contains(string(body), "Connection created") {
		t.Fatalf("expected success message, got %q", string(body))
	}
}
