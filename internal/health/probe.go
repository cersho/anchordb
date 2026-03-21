package health

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"anchordb/internal/config"
	"anchordb/internal/models"
)

func ProbeConnection(ctx context.Context, item models.Connection, cfg config.Config, timeout time.Duration) (string, error) {
	typeName := strings.ToLower(strings.TrimSpace(item.Type))
	host := strings.TrimSpace(item.Host)
	if host == "" {
		return "", errors.New("host is required")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	dialTimeout := timeout
	if dialTimeout > time.Second {
		dialTimeout -= time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	switch typeName {
	case "postgres", "postgresql", "mysql":
		applyConnectionDefaults(&item)
		if item.Port <= 0 {
			return "", errors.New("port is required")
		}
		addr := host
		if _, _, err := net.SplitHostPort(host); err != nil {
			addr = net.JoinHostPort(host, strconv.Itoa(item.Port))
		}
		dialer := net.Dialer{Timeout: dialTimeout}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return "", err
		}
		_ = conn.Close()
		return "TCP reachability OK (" + addr + ")", nil
	case "convex":
		raw := strings.TrimSpace(item.Host)
		if !strings.Contains(raw, "://") {
			raw = "https://" + raw
		}
		u, err := url.Parse(raw)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return "", errors.New("invalid Convex URL")
		}
		port := u.Port()
		if port == "" {
			if strings.EqualFold(u.Scheme, "http") {
				port = "80"
			} else {
				port = "443"
			}
		}
		addr := net.JoinHostPort(u.Hostname(), port)
		dialer := net.Dialer{Timeout: dialTimeout}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return "", err
		}
		_ = conn.Close()
		return "Convex endpoint reachable (" + addr + ")", nil
	case "d1":
		accountID := strings.TrimSpace(item.Host)
		if accountID == "" {
			accountID = strings.TrimSpace(cfg.CloudflareAccountID)
		}
		databaseID := strings.TrimSpace(item.Database)
		if databaseID == "" {
			databaseID = strings.TrimSpace(cfg.CloudflareDatabaseID)
		}
		apiKey := strings.TrimSpace(item.Password)
		if apiKey == "" {
			apiKey = strings.TrimSpace(cfg.CloudflareAPIKey)
		}
		if accountID == "" || databaseID == "" || apiKey == "" {
			return "", errors.New("d1 test requires account id, database id, and api key")
		}
		if err := probeD1Query(ctx, cfg, accountID, databaseID, apiKey); err != nil {
			return "", err
		}
		return "Cloudflare D1 API reachable", nil
	default:
		return "", fmt.Errorf("unsupported connection type: %s", item.Type)
	}
}

func applyConnectionDefaults(item *models.Connection) {
	if item.Port != 0 {
		return
	}

	switch strings.ToLower(strings.TrimSpace(item.Type)) {
	case "postgres", "postgresql":
		item.Port = 5432
	case "mysql":
		item.Port = 3306
	}
}

func probeD1Query(ctx context.Context, cfg config.Config, accountID, databaseID, apiKey string) error {
	endpoint := strings.TrimSuffix(strings.TrimSpace(cfg.D1APIBaseURL), "/")
	if endpoint == "" {
		endpoint = "https://api.cloudflare.com/client/v4"
	}
	endpoint = endpoint + "/accounts/" + url.PathEscape(accountID) + "/d1/database/" + url.PathEscape(databaseID) + "/query"

	payload, err := json.Marshal(map[string]any{"sql": "SELECT 1 AS ok"})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	var envelope struct {
		Success bool `json:"success"`
		Errors  []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		if res.StatusCode >= http.StatusBadRequest {
			return fmt.Errorf("d1 query failed (status %d)", res.StatusCode)
		}
		return err
	}

	if res.StatusCode >= http.StatusBadRequest {
		if len(envelope.Errors) > 0 && strings.TrimSpace(envelope.Errors[0].Message) != "" {
			return errors.New(envelope.Errors[0].Message)
		}
		return fmt.Errorf("d1 query failed (status %d)", res.StatusCode)
	}
	if !envelope.Success {
		if len(envelope.Errors) > 0 && strings.TrimSpace(envelope.Errors[0].Message) != "" {
			return errors.New(envelope.Errors[0].Message)
		}
		return errors.New("d1 query returned unsuccessful response")
	}

	return nil
}
