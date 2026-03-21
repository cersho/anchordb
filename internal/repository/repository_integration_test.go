package repository_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"anchordb/internal/models"
	"anchordb/internal/repository"
	"anchordb/internal/testutil"
)

func TestConnectionLifecycleEncryptsAtRest(t *testing.T) {
	stack := testutil.NewStack(t)
	ctx := context.Background()

	input := models.Connection{
		Name:     "primary-db",
		Type:     "postgres",
		Host:     "localhost",
		Port:     5432,
		Database: "appdb",
		Username: "postgres",
		Password: "super-secret",
	}

	if err := stack.Repo.CreateConnection(ctx, &input); err != nil {
		t.Fatalf("create connection: %v", err)
	}

	if input.Password != "super-secret" {
		t.Fatalf("expected decrypted password after create, got %q", input.Password)
	}

	var stored models.Connection
	if err := stack.DB.WithContext(ctx).First(&stored, "id = ?", input.ID).Error; err != nil {
		t.Fatalf("read stored connection: %v", err)
	}

	if !strings.HasPrefix(stored.Password, "enc:v1:") {
		t.Fatalf("expected encrypted password at rest, got %q", stored.Password)
	}

	fresh, err := stack.Repo.GetConnection(ctx, input.ID)
	if err != nil {
		t.Fatalf("get connection: %v", err)
	}

	if fresh.Password != "super-secret" {
		t.Fatalf("expected decrypted password from repo, got %q", fresh.Password)
	}
}

func TestCreateBackupValidationAndDefaults(t *testing.T) {
	stack := testutil.NewStack(t)
	ctx := context.Background()

	conn := testutil.MustCreateConnection(t, stack.Repo, "backup-source")

	missingLocal := models.Backup{
		Name:         "invalid-local",
		ConnectionID: conn.ID,
		CronExpr:     "0 2 * * *",
		TargetType:   "local",
	}
	if err := stack.Repo.CreateBackup(ctx, &missingLocal); err == nil {
		t.Fatal("expected local_path validation error")
	}

	missingRemote := models.Backup{
		Name:         "invalid-s3",
		ConnectionID: conn.ID,
		CronExpr:     "0 2 * * *",
		TargetType:   "s3",
	}
	if err := stack.Repo.CreateBackup(ctx, &missingRemote); err == nil {
		t.Fatal("expected remote_id validation error")
	}

	valid := models.Backup{
		Name:         "daily-local",
		ConnectionID: conn.ID,
		CronExpr:     "0 2 * * *",
		TargetType:   "local",
		LocalPath:    t.TempDir(),
	}
	if err := stack.Repo.CreateBackup(ctx, &valid); err != nil {
		t.Fatalf("create valid backup: %v", err)
	}

	fresh, err := stack.Repo.GetBackup(ctx, valid.ID)
	if err != nil {
		t.Fatalf("get valid backup: %v", err)
	}

	if fresh.Timezone != "UTC" {
		t.Fatalf("expected default timezone UTC, got %q", fresh.Timezone)
	}
	if fresh.Compression != "gzip" {
		t.Fatalf("expected default compression gzip, got %q", fresh.Compression)
	}
	if fresh.RetentionDays != 7 {
		t.Fatalf("expected default retention 7, got %d", fresh.RetentionDays)
	}
	if !fresh.Enabled {
		t.Fatal("expected backup to be enabled by default")
	}
}

func TestBackupRunLifecycle(t *testing.T) {
	stack := testutil.NewStack(t)
	ctx := context.Background()

	conn := testutil.MustCreateConnection(t, stack.Repo, "run-source")
	backup := testutil.MustCreateLocalBackup(t, stack.Repo, "hourly", conn.ID, t.TempDir(), true)

	run, err := stack.Repo.StartBackupRun(ctx, backup.ID)
	if err != nil {
		t.Fatalf("start backup run: %v", err)
	}

	if run.Status != "running" {
		t.Fatalf("expected running status, got %q", run.Status)
	}

	if err := stack.Repo.FinishBackupRun(ctx, run.ID, "success", "", "hourly/output.sql.gz"); err != nil {
		t.Fatalf("finish backup run: %v", err)
	}

	fresh, err := stack.Repo.GetBackupRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get backup run: %v", err)
	}

	if fresh.Status != "success" {
		t.Fatalf("expected success status, got %q", fresh.Status)
	}
	if fresh.OutputKey != "hourly/output.sql.gz" {
		t.Fatalf("unexpected output key: %q", fresh.OutputKey)
	}
	if fresh.FinishedAt == nil {
		t.Fatal("expected finished_at to be set")
	}

	runs, err := stack.Repo.ListBackupRuns(ctx, backup.ID, 10)
	if err != nil {
		t.Fatalf("list backup runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}
}

func TestNotificationLifecycleAndBindings(t *testing.T) {
	stack := testutil.NewStack(t)
	ctx := context.Background()

	notification := models.NotificationDestination{
		Name:              "discord-alerts",
		Type:              "discord",
		Enabled:           true,
		DiscordWebhookURL: "https://discord.com/api/webhooks/test-id/test-token",
	}

	if err := stack.Repo.CreateNotification(ctx, &notification); err != nil {
		t.Fatalf("create notification: %v", err)
	}

	if notification.DiscordWebhookURL == "" {
		t.Fatal("expected decrypted webhook URL in create response")
	}

	var stored models.NotificationDestination
	if err := stack.DB.WithContext(ctx).First(&stored, "id = ?", notification.ID).Error; err != nil {
		t.Fatalf("read stored notification: %v", err)
	}
	if !strings.HasPrefix(stored.DiscordWebhookURL, "enc:v1:") {
		t.Fatalf("expected encrypted webhook URL at rest, got %q", stored.DiscordWebhookURL)
	}

	updated, err := stack.Repo.UpdateNotification(ctx, notification.ID, repository.NotificationPatch{
		Name: ptrString("discord-alerts-renamed"),
	})
	if err != nil {
		t.Fatalf("update notification: %v", err)
	}
	if updated.DiscordWebhookURL != notification.DiscordWebhookURL {
		t.Fatalf("expected webhook URL to remain decryptable after non-secret update, got %q", updated.DiscordWebhookURL)
	}

	var storedAfterUpdate models.NotificationDestination
	if err := stack.DB.WithContext(ctx).First(&storedAfterUpdate, "id = ?", notification.ID).Error; err != nil {
		t.Fatalf("read stored notification after update: %v", err)
	}
	if strings.Count(storedAfterUpdate.DiscordWebhookURL, "enc:v1:") != 1 {
		t.Fatalf("expected single encryption prefix, got %q", storedAfterUpdate.DiscordWebhookURL)
	}

	conn := testutil.MustCreateConnection(t, stack.Repo, "notif-source")
	backup := testutil.MustCreateLocalBackup(t, stack.Repo, "notif-backup", conn.ID, t.TempDir(), true)

	bindings, err := stack.Repo.SetBackupNotifications(ctx, backup.ID, []models.BackupNotification{{
		NotificationID: notification.ID,
		Enabled:        true,
		OnSuccess:      true,
		OnFailure:      false,
	}})
	if err != nil {
		t.Fatalf("set backup notifications: %v", err)
	}
	if len(bindings) != 1 {
		t.Fatalf("expected 1 binding, got %d", len(bindings))
	}
	if bindings[0].CreatedAt.IsZero() || bindings[0].UpdatedAt.IsZero() {
		t.Fatalf("expected binding timestamps to be set, got created_at=%v updated_at=%v", bindings[0].CreatedAt, bindings[0].UpdatedAt)
	}

	successDestinations, err := stack.Repo.ListNotificationDestinationsForEvent(ctx, backup.ID, "success")
	if err != nil {
		t.Fatalf("list success destinations: %v", err)
	}
	if len(successDestinations) != 1 {
		t.Fatalf("expected 1 success destination, got %d", len(successDestinations))
	}

	failureDestinations, err := stack.Repo.ListNotificationDestinationsForEvent(ctx, backup.ID, "failed")
	if err != nil {
		t.Fatalf("list failure destinations: %v", err)
	}
	if len(failureDestinations) != 0 {
		t.Fatalf("expected 0 failure destinations, got %d", len(failureDestinations))
	}
}

func ptrString(v string) *string {
	return &v
}

func TestDeleteConnectionRemovesHealthChecksAndBindings(t *testing.T) {
	stack := testutil.NewStack(t)
	ctx := context.Background()

	notification := models.NotificationDestination{
		Name:              "health-alerts",
		Type:              "discord",
		Enabled:           true,
		DiscordWebhookURL: "https://discord.com/api/webhooks/test-id/test-token",
	}
	if err := stack.Repo.CreateNotification(ctx, &notification); err != nil {
		t.Fatalf("create notification: %v", err)
	}

	conn := testutil.MustCreateConnection(t, stack.Repo, "health-source")
	check, err := stack.Repo.UpsertHealthCheckForConnection(ctx, conn.ID, repository.HealthCheckDefaults{
		IntervalSecond:   60,
		TimeoutSecond:    5,
		FailureThreshold: 3,
		SuccessThreshold: 1,
	})
	if err != nil {
		t.Fatalf("upsert health check: %v", err)
	}

	bindings, err := stack.Repo.ListHealthCheckNotifications(ctx, check.ID)
	if err != nil {
		t.Fatalf("list health check bindings: %v", err)
	}
	if len(bindings) == 0 {
		t.Fatal("expected seeded health check notification bindings")
	}

	if err := stack.Repo.DeleteConnection(ctx, conn.ID); err != nil {
		t.Fatalf("delete connection: %v", err)
	}

	var checkCount int64
	if err := stack.DB.WithContext(ctx).Model(&models.HealthCheck{}).Where("connection_id = ?", conn.ID).Count(&checkCount).Error; err != nil {
		t.Fatalf("count health checks: %v", err)
	}
	if checkCount != 0 {
		t.Fatalf("expected health checks deleted, got %d", checkCount)
	}

	var bindingCount int64
	if err := stack.DB.WithContext(ctx).Model(&models.HealthCheckNotification{}).Where("health_check_id = ?", check.ID).Count(&bindingCount).Error; err != nil {
		t.Fatalf("count health check bindings: %v", err)
	}
	if bindingCount != 0 {
		t.Fatalf("expected health check bindings deleted, got %d", bindingCount)
	}
}

func TestDeleteNotificationRemovesHealthBindings(t *testing.T) {
	stack := testutil.NewStack(t)
	ctx := context.Background()

	conn := testutil.MustCreateConnection(t, stack.Repo, "health-binding-source")
	if _, err := stack.Repo.UpsertHealthCheckForConnection(ctx, conn.ID, repository.HealthCheckDefaults{
		IntervalSecond:   60,
		TimeoutSecond:    5,
		FailureThreshold: 3,
		SuccessThreshold: 1,
	}); err != nil {
		t.Fatalf("upsert health check: %v", err)
	}

	notification := models.NotificationDestination{
		Name:              "health-alerts-delete",
		Type:              "discord",
		Enabled:           true,
		DiscordWebhookURL: "https://discord.com/api/webhooks/test-id/test-token",
	}
	if err := stack.Repo.CreateNotification(ctx, &notification); err != nil {
		t.Fatalf("create notification: %v", err)
	}

	checks, err := stack.Repo.ListHealthChecks(ctx)
	if err != nil {
		t.Fatalf("list health checks: %v", err)
	}
	if len(checks) == 0 {
		t.Fatal("expected at least one health check")
	}

	var beforeCount int64
	if err := stack.DB.WithContext(ctx).Model(&models.HealthCheckNotification{}).Where("notification_id = ?", notification.ID).Count(&beforeCount).Error; err != nil {
		t.Fatalf("count bindings before delete: %v", err)
	}
	if beforeCount == 0 {
		t.Fatal("expected health notification bindings before delete")
	}

	if err := stack.Repo.DeleteNotification(ctx, notification.ID); err != nil {
		t.Fatalf("delete notification: %v", err)
	}

	var afterCount int64
	if err := stack.DB.WithContext(ctx).Model(&models.HealthCheckNotification{}).Where("notification_id = ?", notification.ID).Count(&afterCount).Error; err != nil {
		t.Fatalf("count bindings after delete: %v", err)
	}
	if afterCount != 0 {
		t.Fatalf("expected health notification bindings deleted, got %d", afterCount)
	}
}

func TestClaimHealthCheckRunPreventsDuplicateClaims(t *testing.T) {
	stack := testutil.NewStack(t)
	ctx := context.Background()

	conn := testutil.MustCreateConnection(t, stack.Repo, "claim-source")
	check, err := stack.Repo.UpsertHealthCheckForConnection(ctx, conn.ID, repository.HealthCheckDefaults{
		IntervalSecond:   60,
		TimeoutSecond:    5,
		FailureThreshold: 3,
		SuccessThreshold: 1,
	})
	if err != nil {
		t.Fatalf("upsert health check: %v", err)
	}

	now := time.Now().UTC()
	claimed, err := stack.Repo.ClaimHealthCheckRun(ctx, check.ID, now, 5*time.Second)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !claimed {
		t.Fatal("expected first claim to succeed")
	}

	claimed, err = stack.Repo.ClaimHealthCheckRun(ctx, check.ID, now.Add(1*time.Second), 5*time.Second)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if claimed {
		t.Fatal("expected second claim to be rejected while lease is active")
	}

	past := time.Now().UTC().Add(-1 * time.Second)
	if err := stack.DB.WithContext(ctx).Model(&models.HealthCheck{}).Where("id = ?", check.ID).Update("next_check_at", &past).Error; err != nil {
		t.Fatalf("set next_check_at to past: %v", err)
	}

	claimed, err = stack.Repo.ClaimHealthCheckRun(ctx, check.ID, time.Now().UTC(), 5*time.Second)
	if err != nil {
		t.Fatalf("third claim: %v", err)
	}
	if !claimed {
		t.Fatal("expected claim to succeed after lease expires")
	}
}
