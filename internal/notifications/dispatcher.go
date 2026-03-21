package notifications

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/smtp"
	"strconv"
	"strings"
	"time"

	"anchordb/internal/models"
	"anchordb/internal/repository"
)

type Dispatcher struct {
	repo       *repository.Repository
	httpClient *http.Client
}

func NewDispatcher(repo *repository.Repository) *Dispatcher {
	return &Dispatcher{
		repo:       repo,
		httpClient: &http.Client{Timeout: 8 * time.Second},
	}
}

func (d *Dispatcher) NotifyBackupRun(ctx context.Context, backup models.Backup, run models.BackupRun) error {
	event := "failed"
	if strings.EqualFold(strings.TrimSpace(run.Status), "success") {
		event = "success"
	}

	destinations, err := d.repo.ListNotificationDestinationsForEvent(ctx, backup.ID, event)
	if err != nil {
		return err
	}
	if len(destinations) == 0 {
		return nil
	}

	errs := make([]string, 0)
	for _, destination := range destinations {
		if strings.TrimSpace(destination.Type) == "" {
			continue
		}

		var sendErr error
		switch strings.ToLower(strings.TrimSpace(destination.Type)) {
		case "discord":
			sendErr = d.sendDiscordMessage(ctx, destination, formatDiscordMessage(backup, run))
		case "smtp":
			sendErr = d.sendSMTPMessage(ctx, destination, buildSMTPSubject(backup, run), formatDiscordMessage(backup, run))
		default:
			sendErr = fmt.Errorf("unsupported notification type: %s", destination.Type)
		}

		if sendErr != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", destination.Name, sendErr))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, "; "))
	}

	return nil
}

func (d *Dispatcher) SendTestNotification(ctx context.Context, destination models.NotificationDestination) error {
	kind := strings.ToLower(strings.TrimSpace(destination.Type))
	message := "AnchorDB test notification\n- Result: Delivery channel is configured correctly"
	switch kind {
	case "discord":
		return d.sendDiscordMessage(ctx, destination, message)
	case "smtp":
		return d.sendSMTPMessage(ctx, destination, "[AnchorDB] Test notification", message)
	default:
		return fmt.Errorf("unsupported notification type: %s", destination.Type)
	}
}

func (d *Dispatcher) sendDiscordMessage(ctx context.Context, destination models.NotificationDestination, content string) error {
	webhookURL := strings.TrimSpace(destination.DiscordWebhookURL)
	if webhookURL == "" {
		return fmt.Errorf("discord_webhook_url is empty")
	}

	body, err := json.Marshal(map[string]string{
		"content": content,
	})
	if err != nil {
		return err
	}

	requestCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode >= http.StatusBadRequest {
		responseBody, _ := io.ReadAll(res.Body)
		if trimmed := strings.TrimSpace(string(responseBody)); trimmed != "" {
			return fmt.Errorf("discord webhook status %d: %s", res.StatusCode, trimmed)
		}
		return fmt.Errorf("discord webhook status %d", res.StatusCode)
	}

	return nil
}

func (d *Dispatcher) sendSMTPMessage(ctx context.Context, destination models.NotificationDestination, subject, body string) error {
	host := strings.TrimSpace(destination.SMTPHost)
	port := destination.SMTPPort
	if host == "" || port <= 0 {
		return fmt.Errorf("smtp_host and smtp_port are required")
	}
	security := normalizeSMTPSecurity(destination.SMTPSecurity)

	from := strings.TrimSpace(destination.SMTPFrom)
	if from == "" {
		return fmt.Errorf("smtp_from is required")
	}
	recipients := parseRecipients(destination.SMTPTo)
	if len(recipients) == 0 {
		return fmt.Errorf("smtp_to must include at least one recipient")
	}

	client, err := dialSMTPClient(ctx, host, port, security)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	if security == "starttls" {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return fmt.Errorf("smtp server does not support STARTTLS")
		}
		tlsCfg := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
		if err := client.StartTLS(tlsCfg); err != nil {
			return fmt.Errorf("starttls failed: %w", err)
		}
	}

	username := strings.TrimSpace(destination.SMTPUsername)
	if username != "" {
		auth := smtp.PlainAuth("", username, destination.SMTPPassword, host)
		if err := client.Auth(auth); err != nil {
			return fmt.Errorf("smtp auth failed: %w", err)
		}
	}

	if err := client.Mail(from); err != nil {
		return err
	}
	for _, recipient := range recipients {
		if err := client.Rcpt(recipient); err != nil {
			return err
		}
	}

	bodyWriter, err := client.Data()
	if err != nil {
		return err
	}

	message := buildSMTPMessage(from, recipients, subject, body)
	if _, err := bodyWriter.Write([]byte(message)); err != nil {
		_ = bodyWriter.Close()
		return err
	}
	if err := bodyWriter.Close(); err != nil {
		return err
	}

	return client.Quit()
}

func formatDiscordMessage(backup models.Backup, run models.BackupRun) string {
	status := strings.ToUpper(strings.TrimSpace(run.Status))
	if status == "" {
		status = "UNKNOWN"
	}
	startedAt := run.StartedAt.UTC()
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}

	builder := strings.Builder{}
	builder.WriteString("AnchorDB backup ")
	builder.WriteString(status)
	builder.WriteString("\n")
	builder.WriteString("- Schedule: ")
	builder.WriteString(backup.Name)
	builder.WriteString("\n")
	builder.WriteString("- Connection: ")
	builder.WriteString(backup.Connection.Name)
	builder.WriteString("\n")
	builder.WriteString("- Run ID: ")
	builder.WriteString(run.ID)
	builder.WriteString("\n")
	builder.WriteString("- Started: ")
	builder.WriteString(startedAt.Format(time.RFC3339))

	if run.FinishedAt != nil {
		builder.WriteString("\n- Finished: ")
		builder.WriteString(run.FinishedAt.UTC().Format(time.RFC3339))
	}
	if strings.TrimSpace(run.OutputKey) != "" {
		builder.WriteString("\n- Artifact: ")
		builder.WriteString(run.OutputKey)
	}
	if strings.TrimSpace(run.ErrorText) != "" {
		builder.WriteString("\n- Error: ")
		builder.WriteString(strings.TrimSpace(run.ErrorText))
	}

	return builder.String()
}

func buildSMTPSubject(backup models.Backup, run models.BackupRun) string {
	status := strings.ToUpper(strings.TrimSpace(run.Status))
	if status == "" {
		status = "UNKNOWN"
	}
	return fmt.Sprintf("[AnchorDB] %s - %s", status, backup.Name)
}

func buildSMTPMessage(from string, to []string, subject, body string) string {
	headers := []string{
		"From: " + from,
		"To: " + strings.Join(to, ", "),
		"Subject: " + subject,
		"Date: " + time.Now().UTC().Format(time.RFC1123Z),
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=UTF-8",
	}

	return strings.Join(headers, "\r\n") + "\r\n\r\n" + body + "\r\n"
}

func normalizeSMTPSecurity(raw string) string {
	security := strings.ToLower(strings.TrimSpace(raw))
	switch security {
	case "", "starttls":
		return "starttls"
	case "ssl", "tls", "ssl_tls", "smtps", "implicit_tls", "implicit-tls":
		return "ssl_tls"
	case "none", "plain", "insecure":
		return "none"
	default:
		return security
	}
}

func parseRecipients(input string) []string {
	replacer := strings.NewReplacer("\n", ",", ";", ",")
	raw := replacer.Replace(input)
	parts := strings.Split(raw, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		items = append(items, trimmed)
	}
	return items
}

func dialSMTPClient(ctx context.Context, host string, port int, security string) (*smtp.Client, error) {
	address := host + ":" + strconv.Itoa(port)
	if security == "ssl_tls" {
		tlsConfig := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
		dialer := &net.Dialer{Timeout: 8 * time.Second}
		conn, err := tls.DialWithDialer(dialer, "tcp", address, tlsConfig)
		if err != nil {
			return nil, err
		}
		client, err := smtp.NewClient(conn, host)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		return client, nil
	}

	dialer := &net.Dialer{Timeout: 8 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	client, err := smtp.NewClient(conn, host)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return client, nil
}
