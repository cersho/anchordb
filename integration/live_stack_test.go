//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

type connection struct {
	ID string `json:"id"`
}

type remote struct {
	ID string `json:"id"`
}

type backup struct {
	ID string `json:"id"`
}

func TestLiveStackCRUDFlow(t *testing.T) {
	baseURL := strings.TrimRight(getenv("ANCHORDB_URL", "http://localhost:8080"), "/")
	client := &http.Client{Timeout: 10 * time.Second}

	if err := waitForHealth(client, baseURL); err != nil {
		t.Skipf("live stack unavailable at %s: %v", baseURL, err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())

	conn := mustCreateConnection(t, client, baseURL, suffix)
	rem := mustCreateRemote(t, client, baseURL, suffix)
	b := mustCreateBackup(t, client, baseURL, conn.ID, rem.ID, suffix)

	mustDelete(t, client, baseURL+"/backups/"+b.ID)
	mustDelete(t, client, baseURL+"/remotes/"+rem.ID)
	mustDelete(t, client, baseURL+"/connections/"+conn.ID)
}

func waitForHealth(client *http.Client, baseURL string) error {
	deadline := time.Now().Add(90 * time.Second)
	var lastErr error

	for time.Now().Before(deadline) {
		res, err := client.Get(baseURL + "/health")
		if err == nil {
			body, readErr := io.ReadAll(res.Body)
			_ = res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return nil
			}
			if readErr != nil {
				lastErr = fmt.Errorf("status %d", res.StatusCode)
			} else {
				lastErr = fmt.Errorf("status %d: %s", res.StatusCode, string(body))
			}
		} else {
			lastErr = err
		}

		time.Sleep(2 * time.Second)
	}

	if lastErr == nil {
		return fmt.Errorf("health check failed without detailed error")
	}

	return lastErr
}

func mustCreateConnection(t *testing.T, client *http.Client, baseURL, suffix string) connection {
	t.Helper()

	body := map[string]any{
		"name":     "it-postgres-" + suffix,
		"type":     "postgres",
		"host":     "postgres-source",
		"port":     5432,
		"database": "appdb",
		"username": "postgres",
		"password": "postgres",
		"ssl_mode": "disable",
	}

	res := mustJSON(t, client, http.MethodPost, baseURL+"/connections", body, http.StatusCreated)
	defer func() { _ = res.Body.Close() }()

	var item connection
	decodeJSON(t, res.Body, &item)
	if item.ID == "" {
		t.Fatal("created connection id is empty")
	}
	return item
}

func mustCreateRemote(t *testing.T, client *http.Client, baseURL, suffix string) remote {
	t.Helper()

	body := map[string]any{
		"name":       "it-minio-" + suffix,
		"provider":   "s3",
		"bucket":     "anchordb-backups",
		"region":     "us-east-1",
		"endpoint":   "http://minio:9000",
		"access_key": "minio",
		"secret_key": "minio123",
	}

	res := mustJSON(t, client, http.MethodPost, baseURL+"/remotes", body, http.StatusCreated)
	defer func() { _ = res.Body.Close() }()

	var item remote
	decodeJSON(t, res.Body, &item)
	if item.ID == "" {
		t.Fatal("created remote id is empty")
	}
	return item
}

func mustCreateBackup(t *testing.T, client *http.Client, baseURL, connID, remoteID, suffix string) backup {
	t.Helper()

	body := map[string]any{
		"name":           "it-backup-" + suffix,
		"connection_id":  connID,
		"cron_expr":      "*/15 * * * *",
		"timezone":       "UTC",
		"target_type":    "s3",
		"remote_id":      remoteID,
		"retention_days": 2,
		"compression":    "gzip",
	}

	res := mustJSON(t, client, http.MethodPost, baseURL+"/backups", body, http.StatusCreated)
	defer func() { _ = res.Body.Close() }()

	var item backup
	decodeJSON(t, res.Body, &item)
	if item.ID == "" {
		t.Fatal("created backup id is empty")
	}
	return item
}

func mustDelete(t *testing.T, client *http.Client, url string) {
	t.Helper()

	res := mustJSON(t, client, http.MethodDelete, url, nil, http.StatusOK)
	defer func() { _ = res.Body.Close() }()
}

func mustJSON(t *testing.T, client *http.Client, method, url string, payload any, expected int) *http.Response {
	t.Helper()

	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
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

	res, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s failed: %v", method, url, err)
	}

	if res.StatusCode != expected {
		defer func() { _ = res.Body.Close() }()
		respBody, _ := io.ReadAll(res.Body)
		t.Fatalf("%s %s expected %d, got %d: %s", method, url, expected, res.StatusCode, string(respBody))
	}

	return res
}

func decodeJSON(t *testing.T, r io.Reader, target any) {
	t.Helper()
	if err := json.NewDecoder(r).Decode(target); err != nil {
		t.Fatalf("decode json: %v", err)
	}
}

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}
