package ui_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"anchordb/internal/models"
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

func TestCreateNotificationAndBindToScheduleFromUI(t *testing.T) {
	stack := testutil.NewStack(t)
	h := ui.NewHandler(stack.Repo, stack.Scheduler, stack.Config)
	server := httptest.NewServer(h.Router())
	t.Cleanup(server.Close)

	conn := testutil.MustCreateConnection(t, stack.Repo, "ui-notify-source")
	backup := testutil.MustCreateLocalBackup(t, stack.Repo, "ui-notify-backup", conn.ID, t.TempDir(), true)

	createForm := url.Values{}
	createForm.Set("backup_id", backup.ID)
	createForm.Set("name", "ops-discord")
	createForm.Set("type", "discord")
	createForm.Set("discord_webhook_url", "https://discord.com/api/webhooks/test-id/test-token")

	createRes, err := http.PostForm(server.URL+"/notifications", createForm)
	if err != nil {
		t.Fatalf("post notification form: %v", err)
	}
	defer func() { _ = createRes.Body.Close() }()
	if createRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", createRes.StatusCode)
	}
	body, err := io.ReadAll(createRes.Body)
	if err != nil {
		t.Fatalf("read create notification response body: %v", err)
	}
	if !strings.Contains(string(body), "Notification destination created") {
		t.Fatalf("expected create notification message, got %q", string(body))
	}

	notifications, err := stack.Repo.ListNotifications(context.Background())
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	if len(notifications) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(notifications))
	}

	bindingsForm := url.Values{}
	bindingsForm.Set("backup_id", backup.ID)
	bindingsForm.Set("binding_enabled_"+notifications[0].ID, "on")
	bindingsForm.Set("binding_success_"+notifications[0].ID, "on")
	bindingsForm.Set("binding_failure_"+notifications[0].ID, "on")

	bindRes, err := http.PostForm(server.URL+"/notifications/bindings", bindingsForm)
	if err != nil {
		t.Fatalf("post bindings form: %v", err)
	}
	defer func() { _ = bindRes.Body.Close() }()
	if bindRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", bindRes.StatusCode)
	}

	bindBody, err := io.ReadAll(bindRes.Body)
	if err != nil {
		t.Fatalf("read bind response body: %v", err)
	}
	if !strings.Contains(string(bindBody), "Schedule notifications updated") {
		t.Fatalf("expected schedule bindings updated message, got %q", string(bindBody))
	}

	bindings, err := stack.Repo.ListBackupNotifications(context.Background(), backup.ID)
	if err != nil {
		t.Fatalf("list backup bindings: %v", err)
	}
	if len(bindings) != 1 {
		t.Fatalf("expected 1 backup binding, got %d", len(bindings))
	}
}

func TestNotificationTestFromUI(t *testing.T) {
	stack := testutil.NewStack(t)
	h := ui.NewHandler(stack.Repo, stack.Scheduler, stack.Config)
	server := httptest.NewServer(h.Router())
	t.Cleanup(server.Close)

	called := false
	webhookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(webhookServer.Close)

	notification := models.NotificationDestination{
		Name:              "ui-discord-test",
		Type:              "discord",
		Enabled:           true,
		DiscordWebhookURL: webhookServer.URL,
	}
	if err := stack.Repo.CreateNotification(context.Background(), &notification); err != nil {
		t.Fatalf("create notification destination: %v", err)
	}

	res, err := http.Post(server.URL+"/notifications/"+notification.ID+"/test", "application/x-www-form-urlencoded", strings.NewReader(""))
	if err != nil {
		t.Fatalf("post notification test endpoint: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if !strings.Contains(string(body), "Test notification sent") {
		t.Fatalf("expected test notification success message, got %q", string(body))
	}
	if !called {
		t.Fatal("expected webhook to be called")
	}
}

func TestUpdateSMTPNotificationFromUIKeepsPasswordWhenBlank(t *testing.T) {
	stack := testutil.NewStack(t)
	h := ui.NewHandler(stack.Repo, stack.Scheduler, stack.Config)
	server := httptest.NewServer(h.Router())
	t.Cleanup(server.Close)

	createForm := url.Values{}
	createForm.Set("name", "ops-smtp")
	createForm.Set("type", "smtp")
	createForm.Set("smtp_host", "smtp.example.com")
	createForm.Set("smtp_port_mode", "587")
	createForm.Set("smtp_username", "alerts@example.com")
	createForm.Set("smtp_password", "initial-secret")
	createForm.Set("smtp_from", "alerts@example.com")
	createForm.Set("smtp_to", "team@example.com")
	createForm.Set("smtp_security", "starttls")

	createRes, err := http.PostForm(server.URL+"/notifications", createForm)
	if err != nil {
		t.Fatalf("post smtp notification form: %v", err)
	}
	defer func() { _ = createRes.Body.Close() }()
	if createRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", createRes.StatusCode)
	}

	notifications, err := stack.Repo.ListNotifications(context.Background())
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	if len(notifications) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(notifications))
	}

	updateForm := url.Values{}
	updateForm.Set("name", "ops-smtp-updated")
	updateForm.Set("type", "smtp")
	updateForm.Set("smtp_host", "smtp.example.com")
	updateForm.Set("smtp_port_mode", "587")
	updateForm.Set("smtp_username", "alerts@example.com")
	updateForm.Set("smtp_password", "")
	updateForm.Set("smtp_from", "alerts@example.com")
	updateForm.Set("smtp_to", "team@example.com")
	updateForm.Set("smtp_security", "starttls")

	updateRes, err := http.PostForm(server.URL+"/notifications/"+notifications[0].ID+"/update", updateForm)
	if err != nil {
		t.Fatalf("post update smtp notification form: %v", err)
	}
	defer func() { _ = updateRes.Body.Close() }()
	if updateRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", updateRes.StatusCode)
	}

	updated, err := stack.Repo.GetNotification(context.Background(), notifications[0].ID)
	if err != nil {
		t.Fatalf("get updated notification: %v", err)
	}
	if updated.Name != "ops-smtp-updated" {
		t.Fatalf("expected updated name, got %q", updated.Name)
	}
	if updated.SMTPPassword != "initial-secret" {
		t.Fatalf("expected smtp password to remain unchanged")
	}
}

func TestDeleteConnectionFromUI(t *testing.T) {
	stack := testutil.NewStack(t)
	h := ui.NewHandler(stack.Repo, stack.Scheduler, stack.Config)
	server := httptest.NewServer(h.Router())
	t.Cleanup(server.Close)

	conn := testutil.MustCreateConnection(t, stack.Repo, "ui-delete-connection")

	res, err := http.Post(server.URL+"/connections/"+conn.ID+"/delete", "application/x-www-form-urlencoded", strings.NewReader(""))
	if err != nil {
		t.Fatalf("post delete connection: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if !strings.Contains(string(body), "Connection deleted") {
		t.Fatalf("expected delete message, got %q", string(body))
	}

	items, err := stack.Repo.ListConnections(context.Background())
	if err != nil {
		t.Fatalf("list connections: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected 0 connections after delete, got %d", len(items))
	}
}

func TestRunsPaginationFromUI(t *testing.T) {
	stack := testutil.NewStack(t)
	h := ui.NewHandler(stack.Repo, stack.Scheduler, stack.Config)
	server := httptest.NewServer(h.Router())
	t.Cleanup(server.Close)

	conn := testutil.MustCreateConnection(t, stack.Repo, "ui-runs-paging-connection")
	backup := testutil.MustCreateLocalBackup(t, stack.Repo, "ui-runs-paging-backup", conn.ID, t.TempDir(), true)
	for i := 0; i < 20; i++ {
		testutil.MustCreateFinishedRun(t, stack.Repo, backup.ID, fmt.Sprintf("runs/%d.sql.gz", i))
	}

	res, err := http.Get(server.URL + "/runs/section?page=2")
	if err != nil {
		t.Fatalf("get runs section page 2: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if !strings.Contains(string(body), "Page 2") {
		t.Fatalf("expected pagination marker, got %q", string(body))
	}
}
