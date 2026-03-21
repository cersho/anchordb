package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"anchordb/internal/api"
	"anchordb/internal/models"
	"anchordb/internal/testutil"
)

func TestHealthEndpoint(t *testing.T) {
	stack := testutil.NewStack(t)
	server := httptest.NewServer(api.NewHandler(stack.Repo, stack.Scheduler).Router())
	t.Cleanup(server.Close)

	res, err := http.Get(server.URL + "/health")
	if err != nil {
		t.Fatalf("get /health: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}

	var body map[string]string
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode /health response: %v", err)
	}

	if body["status"] != "ok" {
		t.Fatalf("expected status ok, got %q", body["status"])
	}
	if body["time"] == "" {
		t.Fatal("expected time field")
	}
}

func TestConnectionsEndpointsRedactSecrets(t *testing.T) {
	stack := testutil.NewStack(t)
	server := httptest.NewServer(api.NewHandler(stack.Repo, stack.Scheduler).Router())
	t.Cleanup(server.Close)

	createdRes := doJSON(t, http.MethodPost, server.URL+"/connections", map[string]any{
		"name":     "postgres-main",
		"type":     "postgres",
		"host":     "localhost",
		"port":     5432,
		"database": "appdb",
		"username": "postgres",
		"password": "secret",
		"ssl_mode": "disable",
	})
	defer func() { _ = createdRes.Body.Close() }()

	if createdRes.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", createdRes.StatusCode)
	}

	var created models.Connection
	decodeJSON(t, createdRes.Body, &created)
	if created.ID == "" {
		t.Fatal("expected created connection id")
	}
	if created.Password != "" {
		t.Fatalf("expected redacted password, got %q", created.Password)
	}

	getRes := doJSON(t, http.MethodGet, server.URL+"/connections/"+created.ID, nil)
	defer func() { _ = getRes.Body.Close() }()
	if getRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from get connection, got %d", getRes.StatusCode)
	}

	var got models.Connection
	decodeJSON(t, getRes.Body, &got)
	if got.Password != "" {
		t.Fatalf("expected redacted password from get endpoint, got %q", got.Password)
	}

	listRes := doJSON(t, http.MethodGet, server.URL+"/connections", nil)
	defer func() { _ = listRes.Body.Close() }()
	if listRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from list connections, got %d", listRes.StatusCode)
	}

	var list []models.Connection
	decodeJSON(t, listRes.Body, &list)
	if len(list) != 1 {
		t.Fatalf("expected 1 connection, got %d", len(list))
	}
	if list[0].Password != "" {
		t.Fatalf("expected redacted password in list endpoint, got %q", list[0].Password)
	}
}

func TestBackupsCreateAndToggleIntegration(t *testing.T) {
	stack := testutil.NewStack(t)
	server := httptest.NewServer(api.NewHandler(stack.Repo, stack.Scheduler).Router())
	t.Cleanup(server.Close)

	conn := testutil.MustCreateConnection(t, stack.Repo, "api-backup-source")

	createRes := doJSON(t, http.MethodPost, server.URL+"/backups", map[string]any{
		"name":           "nightly",
		"connection_id":  conn.ID,
		"cron_expr":      "0 3 * * *",
		"timezone":       "UTC",
		"target_type":    "local",
		"local_path":     t.TempDir(),
		"retention_days": 14,
		"compression":    "gzip",
	})
	defer func() { _ = createRes.Body.Close() }()

	if createRes.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from create backup, got %d", createRes.StatusCode)
	}

	var created models.Backup
	decodeJSON(t, createRes.Body, &created)
	if created.ID == "" {
		t.Fatal("expected created backup id")
	}
	if created.Connection.Password != "" {
		t.Fatalf("expected connection password redacted, got %q", created.Connection.Password)
	}

	toggleRes := doJSON(t, http.MethodPatch, server.URL+"/backups/"+created.ID+"/enabled", map[string]bool{
		"enabled": false,
	})
	defer func() { _ = toggleRes.Body.Close() }()

	if toggleRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from toggle backup, got %d", toggleRes.StatusCode)
	}

	var toggled models.Backup
	decodeJSON(t, toggleRes.Body, &toggled)
	if toggled.Enabled {
		t.Fatal("expected backup to be disabled")
	}

	runsRes := doJSON(t, http.MethodGet, server.URL+"/backups/"+created.ID+"/runs?limit=20", nil)
	defer func() { _ = runsRes.Body.Close() }()

	if runsRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from list backup runs, got %d", runsRes.StatusCode)
	}

	var runs []models.BackupRun
	decodeJSON(t, runsRes.Body, &runs)
	if len(runs) != 0 {
		t.Fatalf("expected no runs yet, got %d", len(runs))
	}
}

func TestCreateBackupValidationError(t *testing.T) {
	stack := testutil.NewStack(t)
	server := httptest.NewServer(api.NewHandler(stack.Repo, stack.Scheduler).Router())
	t.Cleanup(server.Close)

	res := doJSON(t, http.MethodPost, server.URL+"/backups", map[string]any{
		"name":          "invalid-cron",
		"connection_id": "missing-id",
		"cron_expr":     "not-a-cron",
		"target_type":   "local",
		"local_path":    t.TempDir(),
	})
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}
}

func TestCreateConvexConnectionWithMinimalFields(t *testing.T) {
	stack := testutil.NewStack(t)
	server := httptest.NewServer(api.NewHandler(stack.Repo, stack.Scheduler).Router())
	t.Cleanup(server.Close)

	res := doJSON(t, http.MethodPost, server.URL+"/connections", map[string]any{
		"name":     "convex-main",
		"type":     "convex",
		"host":     "https://convex.example",
		"password": "admin-key",
	})
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res.StatusCode)
	}

	var created models.Connection
	decodeJSON(t, res.Body, &created)
	if created.ID == "" {
		t.Fatal("expected created convex connection id")
	}
	if created.Type != "convex" {
		t.Fatalf("expected convex type, got %q", created.Type)
	}
	if created.Password != "" {
		t.Fatalf("expected redacted password, got %q", created.Password)
	}
	if created.Port != 0 {
		t.Fatalf("expected port 0 for convex, got %d", created.Port)
	}
}

func TestCreateConvexBackupForcesNoCompression(t *testing.T) {
	stack := testutil.NewStack(t)
	server := httptest.NewServer(api.NewHandler(stack.Repo, stack.Scheduler).Router())
	t.Cleanup(server.Close)

	connRes := doJSON(t, http.MethodPost, server.URL+"/connections", map[string]any{
		"name":     "convex-main",
		"type":     "convex",
		"host":     "https://convex.example",
		"password": "admin-key",
	})
	defer func() { _ = connRes.Body.Close() }()
	if connRes.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from create connection, got %d", connRes.StatusCode)
	}

	var conn models.Connection
	decodeJSON(t, connRes.Body, &conn)

	createRes := doJSON(t, http.MethodPost, server.URL+"/backups", map[string]any{
		"name":                 "convex-nightly",
		"connection_id":        conn.ID,
		"cron_expr":            "0 3 * * *",
		"timezone":             "UTC",
		"target_type":          "local",
		"local_path":           t.TempDir(),
		"retention_days":       7,
		"compression":          "gzip",
		"include_file_storage": true,
	})
	defer func() { _ = createRes.Body.Close() }()

	if createRes.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from create backup, got %d", createRes.StatusCode)
	}

	var created models.Backup
	decodeJSON(t, createRes.Body, &created)
	if created.Compression != "none" {
		t.Fatalf("expected compression none for convex backup, got %q", created.Compression)
	}
	if !created.IncludeFileStorage {
		t.Fatal("expected include_file_storage=true for convex backup")
	}
}

func TestCreateD1ConnectionWithMinimalFields(t *testing.T) {
	stack := testutil.NewStack(t)
	server := httptest.NewServer(api.NewHandler(stack.Repo, stack.Scheduler).Router())
	t.Cleanup(server.Close)

	res := doJSON(t, http.MethodPost, server.URL+"/connections", map[string]any{
		"name":     "d1-main",
		"type":     "d1",
		"host":     "account-123",
		"database": "db-456",
		"password": "api-token",
	})
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res.StatusCode)
	}

	var created models.Connection
	decodeJSON(t, res.Body, &created)
	if created.ID == "" {
		t.Fatal("expected created d1 connection id")
	}
	if created.Type != "d1" {
		t.Fatalf("expected d1 type, got %q", created.Type)
	}
	if created.Password != "" {
		t.Fatalf("expected redacted password, got %q", created.Password)
	}
	if created.Port != 0 {
		t.Fatalf("expected port 0 for d1, got %d", created.Port)
	}
}

func TestNotificationsEndpointsAndBackupBindings(t *testing.T) {
	stack := testutil.NewStack(t)
	server := httptest.NewServer(api.NewHandler(stack.Repo, stack.Scheduler).Router())
	t.Cleanup(server.Close)

	createNotificationRes := doJSON(t, http.MethodPost, server.URL+"/notifications", map[string]any{
		"name":                "discord-ops",
		"type":                "discord",
		"discord_webhook_url": "https://discord.com/api/webhooks/test-id/test-token",
	})
	defer func() { _ = createNotificationRes.Body.Close() }()
	if createNotificationRes.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from create notification, got %d", createNotificationRes.StatusCode)
	}

	var destination models.NotificationDestination
	decodeJSON(t, createNotificationRes.Body, &destination)
	if destination.ID == "" {
		t.Fatal("expected created notification id")
	}
	if destination.DiscordWebhookURL != "" {
		t.Fatalf("expected redacted webhook URL, got %q", destination.DiscordWebhookURL)
	}

	conn := testutil.MustCreateConnection(t, stack.Repo, "api-notify-source")
	backup := testutil.MustCreateLocalBackup(t, stack.Repo, "api-notify-backup", conn.ID, t.TempDir(), true)

	setBindingsRes := doJSON(t, http.MethodPut, server.URL+"/backups/"+backup.ID+"/notifications", map[string]any{
		"notifications": []map[string]any{{
			"notification_id": destination.ID,
			"on_success":      true,
			"on_failure":      true,
			"enabled":         true,
		}},
	})
	defer func() { _ = setBindingsRes.Body.Close() }()
	if setBindingsRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from set backup notifications, got %d", setBindingsRes.StatusCode)
	}

	listBindingsRes := doJSON(t, http.MethodGet, server.URL+"/backups/"+backup.ID+"/notifications", nil)
	defer func() { _ = listBindingsRes.Body.Close() }()
	if listBindingsRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from list backup notifications, got %d", listBindingsRes.StatusCode)
	}

	var bindings []models.BackupNotification
	decodeJSON(t, listBindingsRes.Body, &bindings)
	if len(bindings) != 1 {
		t.Fatalf("expected 1 backup notification binding, got %d", len(bindings))
	}
	if bindings[0].Notification.DiscordWebhookURL != "" {
		t.Fatalf("expected redacted webhook URL in bindings, got %q", bindings[0].Notification.DiscordWebhookURL)
	}
}

func TestNotificationTestEndpointSendsDiscordWebhook(t *testing.T) {
	stack := testutil.NewStack(t)
	apiServer := httptest.NewServer(api.NewHandler(stack.Repo, stack.Scheduler).Router())
	t.Cleanup(apiServer.Close)

	called := false
	webhookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST webhook request, got %s", r.Method)
		}
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(webhookServer.Close)

	createRes := doJSON(t, http.MethodPost, apiServer.URL+"/notifications", map[string]any{
		"name":                "discord-test",
		"type":                "discord",
		"discord_webhook_url": webhookServer.URL,
	})
	defer func() { _ = createRes.Body.Close() }()
	if createRes.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from create notification, got %d", createRes.StatusCode)
	}

	var destination models.NotificationDestination
	decodeJSON(t, createRes.Body, &destination)

	testRes := doJSON(t, http.MethodPost, apiServer.URL+"/notifications/"+destination.ID+"/test", nil)
	defer func() { _ = testRes.Body.Close() }()
	if testRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from test notification endpoint, got %d", testRes.StatusCode)
	}

	if !called {
		t.Fatal("expected webhook endpoint to be called")
	}
}

func doJSON(t *testing.T, method, url string, payload any) *http.Response {
	t.Helper()

	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal request payload: %v", err)
		}
		body = bytes.NewReader(raw)
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("execute request %s %s: %v", method, url, err)
	}

	return res
}

func decodeJSON(t *testing.T, r io.Reader, target any) {
	t.Helper()
	if err := json.NewDecoder(r).Decode(target); err != nil {
		t.Fatalf("decode json response: %v", err)
	}
}
