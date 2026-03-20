package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type connection struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
	SSLMode  string `json:"ssl_mode"`
}

type remote struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Provider   string `json:"provider"`
	Bucket     string `json:"bucket"`
	Region     string `json:"region"`
	Endpoint   string `json:"endpoint"`
	AccessKey  string `json:"access_key"`
	SecretKey  string `json:"secret_key"`
	PathPrefix string `json:"path_prefix"`
}

type apiError struct {
	Error string `json:"error"`
}

func main() {
	baseURL := strings.TrimRight(getenv("ANCHORDB_URL", "http://localhost:8080"), "/")

	client := &http.Client{Timeout: 10 * time.Second}

	if err := ping(client, baseURL); err != nil {
		fatal("health check failed", err)
	}

	existingConnections, err := listConnections(client, baseURL)
	if err != nil {
		fatal("list connections", err)
	}

	existingRemotes, err := listRemotes(client, baseURL)
	if err != nil {
		fatal("list remotes", err)
	}

	if _, err := ensureConnection(client, baseURL, existingConnections, connection{
		Name:     "compose-postgres",
		Type:     "postgres",
		Host:     "postgres-source",
		Port:     5432,
		Database: "appdb",
		Username: "postgres",
		Password: "postgres",
		SSLMode:  "disable",
	}); err != nil {
		fatal("ensure postgres connection", err)
	}

	if _, err := ensureConnection(client, baseURL, existingConnections, connection{
		Name:     "compose-mysql",
		Type:     "mysql",
		Host:     "mysql-source",
		Port:     3306,
		Database: "appdb",
		Username: "app",
		Password: "app",
	}); err != nil {
		fatal("ensure mysql connection", err)
	}

	if _, err := ensureRemote(client, baseURL, existingRemotes, remote{
		Name:       "compose-minio",
		Provider:   "s3",
		Bucket:     "anchordb-backups",
		Region:     "us-east-1",
		Endpoint:   "http://minio:9000",
		AccessKey:  "minio",
		SecretKey:  "minio123",
		PathPrefix: "",
	}); err != nil {
		fatal("ensure minio remote", err)
	}

	fmt.Println("Seeding complete.")
}

func ping(client *http.Client, baseURL string) error {
	req, err := http.NewRequest(http.MethodGet, baseURL+"/health", nil)
	if err != nil {
		return err
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("status %d: %s", res.StatusCode, string(body))
	}
	return nil
}

func listConnections(client *http.Client, baseURL string) ([]connection, error) {
	req, err := http.NewRequest(http.MethodGet, baseURL+"/connections", nil)
	if err != nil {
		return nil, err
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return nil, readAPIError(res)
	}
	var items []connection
	if err := json.NewDecoder(res.Body).Decode(&items); err != nil {
		return nil, err
	}
	return items, nil
}

func listRemotes(client *http.Client, baseURL string) ([]remote, error) {
	req, err := http.NewRequest(http.MethodGet, baseURL+"/remotes", nil)
	if err != nil {
		return nil, err
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return nil, readAPIError(res)
	}
	var items []remote
	if err := json.NewDecoder(res.Body).Decode(&items); err != nil {
		return nil, err
	}
	return items, nil
}

func ensureConnection(client *http.Client, baseURL string, existing []connection, payload connection) (string, error) {
	for _, item := range existing {
		if item.Name == payload.Name {
			fmt.Printf("Connection exists: %s (%s)\n", item.Name, item.ID)
			return item.ID, nil
		}
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/connections", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusCreated {
		return "", readAPIError(res)
	}

	var created connection
	if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
		return "", err
	}
	fmt.Printf("Created connection: %s (%s)\n", created.Name, created.ID)
	return created.ID, nil
}

func ensureRemote(client *http.Client, baseURL string, existing []remote, payload remote) (string, error) {
	for _, item := range existing {
		if item.Name == payload.Name {
			fmt.Printf("Remote exists: %s (%s)\n", item.Name, item.ID)
			return item.ID, nil
		}
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/remotes", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusCreated {
		return "", readAPIError(res)
	}

	var created remote
	if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
		return "", err
	}
	fmt.Printf("Created remote: %s (%s)\n", created.Name, created.ID)
	return created.ID, nil
}

func readAPIError(res *http.Response) error {
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("status %d", res.StatusCode)
	}
	var parsed apiError
	if err := json.Unmarshal(body, &parsed); err == nil && parsed.Error != "" {
		return fmt.Errorf("status %d: %s", res.StatusCode, parsed.Error)
	}
	if len(body) == 0 {
		return fmt.Errorf("status %d", res.StatusCode)
	}
	return fmt.Errorf("status %d: %s", res.StatusCode, string(body))
}

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func fatal(label string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", label, err)
	os.Exit(1)
}
