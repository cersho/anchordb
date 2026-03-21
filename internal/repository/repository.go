package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"anchordb/internal/crypto"
	"anchordb/internal/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Repository struct {
	db     *gorm.DB
	crypto *crypto.Service
}

type NotificationPatch struct {
	Name              *string
	Type              *string
	Enabled           *bool
	DiscordWebhookURL *string
	SMTPHost          *string
	SMTPPort          *int
	SMTPUsername      *string
	SMTPPassword      *string
	SMTPFrom          *string
	SMTPTo            *string
	SMTPSecurity      *string
}

type HealthCheckDefaults struct {
	IntervalSecond   int
	TimeoutSecond    int
	FailureThreshold int
	SuccessThreshold int
}

type HealthCheckPatch struct {
	Enabled             *bool
	CheckIntervalSecond *int
	TimeoutSecond       *int
	FailureThreshold    *int
	SuccessThreshold    *int
}

func New(db *gorm.DB, cryptoSvc *crypto.Service) *Repository {
	return &Repository{db: db, crypto: cryptoSvc}
}

func (r *Repository) CreateConnection(ctx context.Context, c *models.Connection) error {
	if c.ID == "" {
		c.ID = uuid.NewString()
	}
	if c.SSLMode == "" {
		c.SSLMode = "disable"
	}

	enc, err := r.crypto.EncryptString(c.Password)
	if err != nil {
		return err
	}
	c.Password = enc

	if err := r.db.WithContext(ctx).Create(c).Error; err != nil {
		return err
	}

	return r.decryptConnection(c)
}

func (r *Repository) UpdateConnection(ctx context.Context, id string, c *models.Connection) (models.Connection, error) {
	existing, err := r.getConnectionRaw(ctx, id)
	if err != nil {
		return models.Connection{}, err
	}

	if c.Name != "" {
		existing.Name = c.Name
	}
	if c.Type != "" {
		existing.Type = strings.ToLower(c.Type)
	}
	if c.Host != "" {
		existing.Host = c.Host
	}
	if c.Port != 0 {
		existing.Port = c.Port
	}
	if c.Database != "" {
		existing.Database = c.Database
	}
	if c.Username != "" {
		existing.Username = c.Username
	}
	if c.Password != "" {
		encrypted, encErr := r.crypto.EncryptString(c.Password)
		if encErr != nil {
			return models.Connection{}, encErr
		}
		existing.Password = encrypted
	}
	if c.SSLMode != "" {
		existing.SSLMode = c.SSLMode
	}

	if err := r.db.WithContext(ctx).Save(&existing).Error; err != nil {
		return models.Connection{}, err
	}
	if err := r.decryptConnection(&existing); err != nil {
		return models.Connection{}, err
	}
	return existing, nil
}

func (r *Repository) DeleteConnection(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var checkIDs []string
		if err := tx.Model(&models.HealthCheck{}).Where("connection_id = ?", id).Pluck("id", &checkIDs).Error; err != nil {
			return err
		}
		if len(checkIDs) > 0 {
			if err := tx.Where("health_check_id IN ?", checkIDs).Delete(&models.HealthCheckNotification{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("connection_id = ?", id).Delete(&models.HealthCheck{}).Error; err != nil {
			return err
		}

		res := tx.Delete(&models.Connection{}, "id = ?", id)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
}

func (r *Repository) ListConnections(ctx context.Context) ([]models.Connection, error) {
	var items []models.Connection
	err := r.db.WithContext(ctx).Order("created_at desc").Find(&items).Error
	if err != nil {
		return nil, err
	}
	for i := range items {
		if decErr := r.decryptConnection(&items[i]); decErr != nil {
			return nil, decErr
		}
	}
	return items, nil
}

func (r *Repository) GetConnection(ctx context.Context, id string) (models.Connection, error) {
	c, err := r.getConnectionRaw(ctx, id)
	if err != nil {
		return models.Connection{}, err
	}
	if err := r.decryptConnection(&c); err != nil {
		return models.Connection{}, err
	}
	return c, nil
}

func (r *Repository) getConnectionRaw(ctx context.Context, id string) (models.Connection, error) {
	var c models.Connection
	err := r.db.WithContext(ctx).First(&c, "id = ?", id).Error
	return c, err
}

func (r *Repository) CreateRemote(ctx context.Context, rem *models.Remote) error {
	if rem.ID == "" {
		rem.ID = uuid.NewString()
	}
	if rem.Provider == "" {
		rem.Provider = "s3"
	}

	access, err := r.crypto.EncryptString(rem.AccessKey)
	if err != nil {
		return err
	}
	secret, err := r.crypto.EncryptString(rem.SecretKey)
	if err != nil {
		return err
	}
	rem.AccessKey = access
	rem.SecretKey = secret

	if err := r.db.WithContext(ctx).Create(rem).Error; err != nil {
		return err
	}

	return r.decryptRemote(rem)
}

func (r *Repository) UpdateRemote(ctx context.Context, id string, rem *models.Remote) (models.Remote, error) {
	existing, err := r.getRemoteRaw(ctx, id)
	if err != nil {
		return models.Remote{}, err
	}

	if rem.Name != "" {
		existing.Name = rem.Name
	}
	if rem.Provider != "" {
		existing.Provider = strings.ToLower(rem.Provider)
	}
	if rem.Bucket != "" {
		existing.Bucket = rem.Bucket
	}
	if rem.Region != "" {
		existing.Region = rem.Region
	}
	if rem.Endpoint != "" {
		existing.Endpoint = rem.Endpoint
	}
	if rem.PathPrefix != "" {
		existing.PathPrefix = rem.PathPrefix
	}
	if rem.AccessKey != "" {
		enc, encErr := r.crypto.EncryptString(rem.AccessKey)
		if encErr != nil {
			return models.Remote{}, encErr
		}
		existing.AccessKey = enc
	}
	if rem.SecretKey != "" {
		enc, encErr := r.crypto.EncryptString(rem.SecretKey)
		if encErr != nil {
			return models.Remote{}, encErr
		}
		existing.SecretKey = enc
	}

	if err := r.db.WithContext(ctx).Save(&existing).Error; err != nil {
		return models.Remote{}, err
	}
	if err := r.decryptRemote(&existing); err != nil {
		return models.Remote{}, err
	}
	return existing, nil
}

func (r *Repository) DeleteRemote(ctx context.Context, id string) error {
	res := r.db.WithContext(ctx).Delete(&models.Remote{}, "id = ?", id)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func (r *Repository) GetRemote(ctx context.Context, id string) (models.Remote, error) {
	rem, err := r.getRemoteRaw(ctx, id)
	if err != nil {
		return models.Remote{}, err
	}
	if err := r.decryptRemote(&rem); err != nil {
		return models.Remote{}, err
	}
	return rem, nil
}

func (r *Repository) getRemoteRaw(ctx context.Context, id string) (models.Remote, error) {
	var rem models.Remote
	err := r.db.WithContext(ctx).First(&rem, "id = ?", id).Error
	return rem, err
}

func (r *Repository) ListRemotes(ctx context.Context) ([]models.Remote, error) {
	var items []models.Remote
	err := r.db.WithContext(ctx).Order("created_at desc").Find(&items).Error
	if err != nil {
		return nil, err
	}
	for i := range items {
		if decErr := r.decryptRemote(&items[i]); decErr != nil {
			return nil, decErr
		}
	}
	return items, nil
}

func (r *Repository) CreateNotification(ctx context.Context, n *models.NotificationDestination) error {
	if n.ID == "" {
		n.ID = uuid.NewString()
	}
	n.Type = strings.ToLower(strings.TrimSpace(n.Type))
	if n.SMTPPort == 0 {
		n.SMTPPort = 587
	}
	n.SMTPSecurity = normalizeSMTPSecurityValue(n.SMTPSecurity)
	if err := validateNotificationDestination(*n); err != nil {
		return err
	}

	copy := *n
	if err := r.encryptNotificationSecrets(&copy); err != nil {
		return err
	}
	if err := r.db.WithContext(ctx).Create(&copy).Error; err != nil {
		return err
	}
	if err := r.attachNotificationToAllHealthChecks(ctx, copy.ID); err != nil {
		return err
	}
	*n = copy
	return r.decryptNotification(n)
}

func (r *Repository) UpdateNotification(ctx context.Context, id string, patch NotificationPatch) (models.NotificationDestination, error) {
	existing, err := r.getNotificationRaw(ctx, id)
	if err != nil {
		return models.NotificationDestination{}, err
	}
	if err := r.decryptNotification(&existing); err != nil {
		return models.NotificationDestination{}, err
	}

	if patch.Name != nil {
		existing.Name = strings.TrimSpace(*patch.Name)
	}
	if patch.Type != nil {
		existing.Type = strings.ToLower(strings.TrimSpace(*patch.Type))
	}
	if patch.Enabled != nil {
		existing.Enabled = *patch.Enabled
	}
	if patch.DiscordWebhookURL != nil {
		existing.DiscordWebhookURL = strings.TrimSpace(*patch.DiscordWebhookURL)
	}
	if patch.SMTPHost != nil {
		existing.SMTPHost = strings.TrimSpace(*patch.SMTPHost)
	}
	if patch.SMTPPort != nil {
		existing.SMTPPort = *patch.SMTPPort
	}
	if patch.SMTPUsername != nil {
		existing.SMTPUsername = strings.TrimSpace(*patch.SMTPUsername)
	}
	if patch.SMTPPassword != nil {
		existing.SMTPPassword = *patch.SMTPPassword
	}
	if patch.SMTPFrom != nil {
		existing.SMTPFrom = strings.TrimSpace(*patch.SMTPFrom)
	}
	if patch.SMTPTo != nil {
		existing.SMTPTo = strings.TrimSpace(*patch.SMTPTo)
	}
	if patch.SMTPSecurity != nil {
		existing.SMTPSecurity = normalizeSMTPSecurityValue(*patch.SMTPSecurity)
	}

	if existing.SMTPPort == 0 {
		existing.SMTPPort = 587
	}
	existing.SMTPSecurity = normalizeSMTPSecurityValue(existing.SMTPSecurity)
	if err := validateNotificationDestination(existing); err != nil {
		return models.NotificationDestination{}, err
	}

	saved := existing
	if err := r.encryptNotificationSecrets(&saved); err != nil {
		return models.NotificationDestination{}, err
	}
	if err := r.db.WithContext(ctx).Save(&saved).Error; err != nil {
		return models.NotificationDestination{}, err
	}
	if err := r.decryptNotification(&saved); err != nil {
		return models.NotificationDestination{}, err
	}
	return saved, nil
}

func (r *Repository) DeleteNotification(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(&models.BackupNotification{}, "notification_id = ?", id).Error; err != nil {
			return err
		}
		if err := tx.Delete(&models.HealthCheckNotification{}, "notification_id = ?", id).Error; err != nil {
			return err
		}
		res := tx.Delete(&models.NotificationDestination{}, "id = ?", id)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
}

func (r *Repository) GetNotification(ctx context.Context, id string) (models.NotificationDestination, error) {
	n, err := r.getNotificationRaw(ctx, id)
	if err != nil {
		return models.NotificationDestination{}, err
	}
	if err := r.decryptNotification(&n); err != nil {
		return models.NotificationDestination{}, err
	}
	return n, nil
}

func (r *Repository) ListNotifications(ctx context.Context) ([]models.NotificationDestination, error) {
	var items []models.NotificationDestination
	err := r.db.WithContext(ctx).Order("created_at desc").Find(&items).Error
	if err != nil {
		return nil, err
	}
	for i := range items {
		if decErr := r.decryptNotification(&items[i]); decErr != nil {
			return nil, decErr
		}
	}
	return items, nil
}

func (r *Repository) getNotificationRaw(ctx context.Context, id string) (models.NotificationDestination, error) {
	var n models.NotificationDestination
	err := r.db.WithContext(ctx).First(&n, "id = ?", id).Error
	return n, err
}

func (r *Repository) ListBackupNotifications(ctx context.Context, backupID string) ([]models.BackupNotification, error) {
	var items []models.BackupNotification
	err := r.db.WithContext(ctx).
		Where("backup_id = ?", backupID).
		Preload("Notification").
		Order("created_at asc").
		Find(&items).Error
	if err != nil {
		return nil, err
	}
	for i := range items {
		if items[i].Notification.ID != "" {
			if decErr := r.decryptNotification(&items[i].Notification); decErr != nil {
				return nil, decErr
			}
		}
	}
	return items, nil
}

func (r *Repository) SetBackupNotifications(ctx context.Context, backupID string, bindings []models.BackupNotification) ([]models.BackupNotification, error) {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var backup models.Backup
		if err := tx.First(&backup, "id = ?", backupID).Error; err != nil {
			return err
		}

		if err := tx.Delete(&models.BackupNotification{}, "backup_id = ?", backupID).Error; err != nil {
			return err
		}

		seen := make(map[string]struct{}, len(bindings))
		for _, binding := range bindings {
			notificationID := strings.TrimSpace(binding.NotificationID)
			if notificationID == "" {
				return errors.New("notification_id is required")
			}
			if _, ok := seen[notificationID]; ok {
				return fmt.Errorf("duplicate notification_id: %s", notificationID)
			}
			seen[notificationID] = struct{}{}
			if !binding.OnSuccess && !binding.OnFailure {
				return fmt.Errorf("notification %s must enable on_success or on_failure", notificationID)
			}

			var destination models.NotificationDestination
			if err := tx.First(&destination, "id = ?", notificationID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return fmt.Errorf("notification_id not found: %s", notificationID)
				}
				return err
			}

			now := time.Now().UTC()
			item := map[string]any{
				"id":              uuid.NewString(),
				"backup_id":       backupID,
				"notification_id": notificationID,
				"on_success":      binding.OnSuccess,
				"on_failure":      binding.OnFailure,
				"enabled":         binding.Enabled,
				"created_at":      now,
				"updated_at":      now,
			}

			if err := tx.Table("backup_notifications").Create(item).Error; err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return r.ListBackupNotifications(ctx, backupID)
}

func (r *Repository) ListNotificationDestinationsForEvent(ctx context.Context, backupID, event string) ([]models.NotificationDestination, error) {
	column := "on_failure"
	if strings.EqualFold(strings.TrimSpace(event), "success") {
		column = "on_success"
	}

	var bindings []models.BackupNotification
	err := r.db.WithContext(ctx).
		Where("backup_id = ?", backupID).
		Where("enabled = ?", true).
		Where(column+" = ?", true).
		Preload("Notification", "enabled = ?", true).
		Find(&bindings).Error
	if err != nil {
		return nil, err
	}

	items := make([]models.NotificationDestination, 0, len(bindings))
	for _, binding := range bindings {
		if binding.Notification.ID == "" {
			continue
		}
		if decErr := r.decryptNotification(&binding.Notification); decErr != nil {
			return nil, decErr
		}
		items = append(items, binding.Notification)
	}
	return items, nil
}

func (r *Repository) EnsureHealthChecksForConnections(ctx context.Context, defaults HealthCheckDefaults) error {
	conns, err := r.ListConnections(ctx)
	if err != nil {
		return err
	}
	for _, conn := range conns {
		if _, err := r.UpsertHealthCheckForConnection(ctx, conn.ID, defaults); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) UpsertHealthCheckForConnection(ctx context.Context, connectionID string, defaults HealthCheckDefaults) (models.HealthCheck, error) {
	var item models.HealthCheck
	err := r.db.WithContext(ctx).Where("connection_id = ?", strings.TrimSpace(connectionID)).First(&item).Error
	if err == nil {
		if seedErr := r.seedDefaultHealthCheckNotifications(ctx, item.ID); seedErr != nil {
			return models.HealthCheck{}, seedErr
		}
		return r.GetHealthCheck(ctx, item.ID)
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return models.HealthCheck{}, err
	}

	now := time.Now().UTC()
	item = models.HealthCheck{
		ID:                  uuid.NewString(),
		ConnectionID:        strings.TrimSpace(connectionID),
		Enabled:             true,
		CheckIntervalSecond: normalizePositive(defaults.IntervalSecond, 60),
		TimeoutSecond:       normalizePositive(defaults.TimeoutSecond, 5),
		FailureThreshold:    normalizePositive(defaults.FailureThreshold, 3),
		SuccessThreshold:    normalizePositive(defaults.SuccessThreshold, 1),
		Status:              "unknown",
		NextCheckAt:         &now,
	}
	if err := r.db.WithContext(ctx).Create(&item).Error; err != nil {
		return models.HealthCheck{}, err
	}
	if err := r.seedDefaultHealthCheckNotifications(ctx, item.ID); err != nil {
		return models.HealthCheck{}, err
	}
	return r.GetHealthCheck(ctx, item.ID)
}

func (r *Repository) ListHealthChecks(ctx context.Context) ([]models.HealthCheck, error) {
	var items []models.HealthCheck
	err := r.db.WithContext(ctx).
		Preload("Connection").
		Order("created_at desc").
		Find(&items).Error
	if err != nil {
		return nil, err
	}
	for i := range items {
		if decErr := r.decryptConnection(&items[i].Connection); decErr != nil {
			return nil, decErr
		}
	}
	return items, nil
}

func (r *Repository) GetHealthCheck(ctx context.Context, id string) (models.HealthCheck, error) {
	var item models.HealthCheck
	err := r.db.WithContext(ctx).
		Preload("Connection").
		First(&item, "id = ?", id).Error
	if err != nil {
		return models.HealthCheck{}, err
	}
	if err := r.decryptConnection(&item.Connection); err != nil {
		return models.HealthCheck{}, err
	}
	return item, nil
}

func (r *Repository) UpdateHealthCheck(ctx context.Context, id string, patch HealthCheckPatch) (models.HealthCheck, error) {
	var item models.HealthCheck
	if err := r.db.WithContext(ctx).First(&item, "id = ?", id).Error; err != nil {
		return models.HealthCheck{}, err
	}

	if patch.Enabled != nil {
		item.Enabled = *patch.Enabled
	}
	if patch.CheckIntervalSecond != nil {
		item.CheckIntervalSecond = normalizePositive(*patch.CheckIntervalSecond, item.CheckIntervalSecond)
	}
	if patch.TimeoutSecond != nil {
		item.TimeoutSecond = normalizePositive(*patch.TimeoutSecond, item.TimeoutSecond)
	}
	if patch.FailureThreshold != nil {
		item.FailureThreshold = normalizePositive(*patch.FailureThreshold, item.FailureThreshold)
	}
	if patch.SuccessThreshold != nil {
		item.SuccessThreshold = normalizePositive(*patch.SuccessThreshold, item.SuccessThreshold)
	}
	if item.CheckIntervalSecond <= 0 {
		item.CheckIntervalSecond = 60
	}
	if item.TimeoutSecond <= 0 {
		item.TimeoutSecond = 5
	}
	if item.FailureThreshold <= 0 {
		item.FailureThreshold = 3
	}
	if item.SuccessThreshold <= 0 {
		item.SuccessThreshold = 1
	}
	if err := r.db.WithContext(ctx).Save(&item).Error; err != nil {
		return models.HealthCheck{}, err
	}
	return r.GetHealthCheck(ctx, item.ID)
}

func (r *Repository) ListDueHealthChecks(ctx context.Context, now time.Time, limit int) ([]models.HealthCheck, error) {
	if limit <= 0 {
		limit = 50
	}
	var items []models.HealthCheck
	err := r.db.WithContext(ctx).
		Where("enabled = ?", true).
		Where("next_check_at IS NULL OR next_check_at <= ?", now.UTC()).
		Preload("Connection").
		Order("next_check_at asc").
		Limit(limit).
		Find(&items).Error
	if err != nil {
		return nil, err
	}
	for i := range items {
		if decErr := r.decryptConnection(&items[i].Connection); decErr != nil {
			return nil, decErr
		}
	}
	return items, nil
}

func (r *Repository) ClaimHealthCheckRun(ctx context.Context, id string, now time.Time, lease time.Duration) (bool, error) {
	if lease <= 0 {
		lease = 10 * time.Second
	}
	nowUTC := now.UTC()
	next := nowUTC.Add(lease)

	res := r.db.WithContext(ctx).
		Model(&models.HealthCheck{}).
		Where("id = ?", id).
		Where("enabled = ?", true).
		Where("next_check_at IS NULL OR next_check_at <= ?", nowUTC).
		Updates(map[string]any{"next_check_at": &next})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}

func (r *Repository) SaveHealthCheckProbeResult(ctx context.Context, id string, checkedAt time.Time, healthy bool, errorText string) (models.HealthCheck, string, error) {
	var item models.HealthCheck
	if err := r.db.WithContext(ctx).First(&item, "id = ?", id).Error; err != nil {
		return models.HealthCheck{}, "", err
	}

	oldStatus := strings.ToLower(strings.TrimSpace(item.Status))
	if oldStatus == "" {
		oldStatus = "unknown"
	}

	if healthy {
		item.ConsecutiveSuccess++
		item.ConsecutiveFailures = 0
		item.LastError = ""
		if item.ConsecutiveSuccess >= normalizePositive(item.SuccessThreshold, 1) {
			item.Status = "up"
		}
	} else {
		item.ConsecutiveFailures++
		item.ConsecutiveSuccess = 0
		item.LastError = strings.TrimSpace(errorText)
		if item.ConsecutiveFailures >= normalizePositive(item.FailureThreshold, 3) {
			item.Status = "down"
		}
	}

	if strings.TrimSpace(item.Status) == "" {
		item.Status = "unknown"
	}

	checked := checkedAt.UTC()
	item.LastCheckedAt = &checked
	next := checked.Add(time.Duration(normalizePositive(item.CheckIntervalSecond, 60)) * time.Second)
	item.NextCheckAt = &next

	if err := r.db.WithContext(ctx).Save(&item).Error; err != nil {
		return models.HealthCheck{}, "", err
	}

	newStatus := strings.ToLower(strings.TrimSpace(item.Status))
	event := ""
	if oldStatus != "down" && newStatus == "down" {
		event = "down"
	}
	if oldStatus == "down" && newStatus == "up" {
		event = "recovered"
	}

	fresh, err := r.GetHealthCheck(ctx, item.ID)
	if err != nil {
		return models.HealthCheck{}, "", err
	}
	return fresh, event, nil
}

func (r *Repository) MarkHealthCheckNotified(ctx context.Context, id string, at time.Time) error {
	t := at.UTC()
	return r.db.WithContext(ctx).
		Model(&models.HealthCheck{}).
		Where("id = ?", id).
		Update("last_notified_at", &t).Error
}

func (r *Repository) ListHealthCheckNotifications(ctx context.Context, healthCheckID string) ([]models.HealthCheckNotification, error) {
	var items []models.HealthCheckNotification
	err := r.db.WithContext(ctx).
		Where("health_check_id = ?", healthCheckID).
		Preload("Notification").
		Order("created_at asc").
		Find(&items).Error
	if err != nil {
		return nil, err
	}
	for i := range items {
		if items[i].Notification.ID != "" {
			if decErr := r.decryptNotification(&items[i].Notification); decErr != nil {
				return nil, decErr
			}
		}
	}
	return items, nil
}

func (r *Repository) SetHealthCheckNotifications(ctx context.Context, healthCheckID string, bindings []models.HealthCheckNotification) ([]models.HealthCheckNotification, error) {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var check models.HealthCheck
		if err := tx.First(&check, "id = ?", healthCheckID).Error; err != nil {
			return err
		}

		if err := tx.Delete(&models.HealthCheckNotification{}, "health_check_id = ?", healthCheckID).Error; err != nil {
			return err
		}

		seen := make(map[string]struct{}, len(bindings))
		for _, binding := range bindings {
			notificationID := strings.TrimSpace(binding.NotificationID)
			if notificationID == "" {
				return errors.New("notification_id is required")
			}
			if _, ok := seen[notificationID]; ok {
				return fmt.Errorf("duplicate notification_id: %s", notificationID)
			}
			seen[notificationID] = struct{}{}
			if !binding.OnDown && !binding.OnRecovered {
				return fmt.Errorf("notification %s must enable on_down or on_recovered", notificationID)
			}

			var destination models.NotificationDestination
			if err := tx.First(&destination, "id = ?", notificationID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return fmt.Errorf("notification_id not found: %s", notificationID)
				}
				return err
			}

			now := time.Now().UTC()
			item := map[string]any{
				"id":              uuid.NewString(),
				"health_check_id": healthCheckID,
				"notification_id": notificationID,
				"on_down":         binding.OnDown,
				"on_recovered":    binding.OnRecovered,
				"enabled":         binding.Enabled,
				"created_at":      now,
				"updated_at":      now,
			}
			if err := tx.Table("health_check_notifications").Create(item).Error; err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return r.ListHealthCheckNotifications(ctx, healthCheckID)
}

func (r *Repository) ListHealthNotificationDestinationsForEvent(ctx context.Context, healthCheckID, event string) ([]models.NotificationDestination, error) {
	column := "on_down"
	if strings.EqualFold(strings.TrimSpace(event), "recovered") {
		column = "on_recovered"
	}

	var bindings []models.HealthCheckNotification
	err := r.db.WithContext(ctx).
		Where("health_check_id = ?", healthCheckID).
		Where("enabled = ?", true).
		Where(column+" = ?", true).
		Preload("Notification", "enabled = ?", true).
		Find(&bindings).Error
	if err != nil {
		return nil, err
	}

	items := make([]models.NotificationDestination, 0, len(bindings))
	for _, binding := range bindings {
		if binding.Notification.ID == "" {
			continue
		}
		if decErr := r.decryptNotification(&binding.Notification); decErr != nil {
			return nil, decErr
		}
		items = append(items, binding.Notification)
	}
	return items, nil
}

func normalizePositive(value, fallback int) int {
	if value > 0 {
		return value
	}
	if fallback > 0 {
		return fallback
	}
	return 1
}

func (r *Repository) seedDefaultHealthCheckNotifications(ctx context.Context, healthCheckID string) error {
	var count int64
	if err := r.db.WithContext(ctx).Model(&models.HealthCheckNotification{}).Where("health_check_id = ?", healthCheckID).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	var destinations []models.NotificationDestination
	if err := r.db.WithContext(ctx).Where("enabled = ?", true).Find(&destinations).Error; err != nil {
		return err
	}
	if len(destinations) == 0 {
		return nil
	}

	now := time.Now().UTC()
	items := make([]map[string]any, 0, len(destinations))
	for _, destination := range destinations {
		items = append(items, map[string]any{
			"id":              uuid.NewString(),
			"health_check_id": healthCheckID,
			"notification_id": destination.ID,
			"on_down":         true,
			"on_recovered":    true,
			"enabled":         true,
			"created_at":      now,
			"updated_at":      now,
		})
	}
	return r.db.WithContext(ctx).Table("health_check_notifications").Create(items).Error
}

func (r *Repository) attachNotificationToAllHealthChecks(ctx context.Context, notificationID string) error {
	var checks []models.HealthCheck
	if err := r.db.WithContext(ctx).Select("id").Find(&checks).Error; err != nil {
		return err
	}
	if len(checks) == 0 {
		return nil
	}
	now := time.Now().UTC()
	items := make([]map[string]any, 0, len(checks))
	for _, check := range checks {
		items = append(items, map[string]any{
			"id":              uuid.NewString(),
			"health_check_id": check.ID,
			"notification_id": notificationID,
			"on_down":         true,
			"on_recovered":    true,
			"enabled":         true,
			"created_at":      now,
			"updated_at":      now,
		})
	}
	return r.db.WithContext(ctx).Table("health_check_notifications").Create(items).Error
}

func validateNotificationDestination(n models.NotificationDestination) error {
	if strings.TrimSpace(n.Name) == "" {
		return errors.New("name is required")
	}
	kind := strings.ToLower(strings.TrimSpace(n.Type))
	if kind != "discord" && kind != "smtp" {
		return errors.New("type must be discord or smtp")
	}
	if kind == "discord" {
		if strings.TrimSpace(n.DiscordWebhookURL) == "" {
			return errors.New("discord_webhook_url is required for discord notifications")
		}
		return nil
	}

	if strings.TrimSpace(n.SMTPHost) == "" || strings.TrimSpace(n.SMTPFrom) == "" || strings.TrimSpace(n.SMTPTo) == "" {
		return errors.New("smtp_host, smtp_from, and smtp_to are required for smtp notifications")
	}
	if n.SMTPPort <= 0 || n.SMTPPort > 65535 {
		return errors.New("smtp_port must be between 1 and 65535")
	}
	security := strings.ToLower(strings.TrimSpace(n.SMTPSecurity))
	if security != "starttls" && security != "ssl_tls" && security != "none" {
		return errors.New("smtp_security must be starttls, ssl_tls, or none")
	}
	if strings.TrimSpace(n.SMTPUsername) != "" && strings.TrimSpace(n.SMTPPassword) == "" {
		return errors.New("smtp_password is required when smtp_username is set")
	}
	return nil
}

func normalizeSMTPSecurityValue(raw string) string {
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

func (r *Repository) CreateBackup(ctx context.Context, b *models.Backup) error {
	if b.ID == "" {
		b.ID = uuid.NewString()
	}
	if b.Timezone == "" {
		b.Timezone = "UTC"
	}
	if b.Compression == "" {
		b.Compression = "gzip"
	}
	if b.RetentionDays == 0 {
		b.RetentionDays = 7
	}
	if !b.Enabled {
		b.Enabled = true
	}
	if b.TargetType == "local" && b.LocalPath == "" {
		return errors.New("local_path is required for local target")
	}
	if b.TargetType == "s3" && b.RemoteID == nil {
		return errors.New("remote_id is required for s3 target")
	}
	return r.db.WithContext(ctx).Create(b).Error
}

func (r *Repository) UpdateBackup(ctx context.Context, id string, in *models.Backup) (models.Backup, error) {
	existing, err := r.GetBackup(ctx, id)
	if err != nil {
		return models.Backup{}, err
	}

	if in.Name != "" {
		existing.Name = in.Name
	}
	if in.ConnectionID != "" {
		existing.ConnectionID = in.ConnectionID
	}
	if in.CronExpr != "" {
		existing.CronExpr = in.CronExpr
	}
	if in.Timezone != "" {
		existing.Timezone = in.Timezone
	}
	if in.TargetType != "" {
		existing.TargetType = in.TargetType
	}
	if in.LocalPath != "" {
		existing.LocalPath = in.LocalPath
	}
	if in.RemoteID != nil {
		existing.RemoteID = in.RemoteID
	}
	if in.RetentionDays > 0 {
		existing.RetentionDays = in.RetentionDays
	}
	if in.Compression != "" {
		existing.Compression = in.Compression
	}
	if in.IncludeFileStorage {
		existing.IncludeFileStorage = true
	}
	if err := r.db.WithContext(ctx).Model(&models.Backup{}).Where("id = ?", id).Updates(map[string]any{
		"name":                 existing.Name,
		"connection_id":        existing.ConnectionID,
		"cron_expr":            existing.CronExpr,
		"timezone":             existing.Timezone,
		"target_type":          existing.TargetType,
		"local_path":           existing.LocalPath,
		"remote_id":            existing.RemoteID,
		"retention_days":       existing.RetentionDays,
		"compression":          existing.Compression,
		"include_file_storage": existing.IncludeFileStorage,
	}).Error; err != nil {
		return models.Backup{}, err
	}

	return r.GetBackup(ctx, id)
}

func (r *Repository) DeleteBackup(ctx context.Context, id string) error {
	res := r.db.WithContext(ctx).Delete(&models.Backup{}, "id = ?", id)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func (r *Repository) ListBackups(ctx context.Context) ([]models.Backup, error) {
	var items []models.Backup
	err := r.db.WithContext(ctx).
		Preload("Connection").
		Preload("Remote").
		Order("created_at desc").
		Find(&items).Error
	if err != nil {
		return nil, err
	}
	for i := range items {
		if decErr := r.decryptConnection(&items[i].Connection); decErr != nil {
			return nil, decErr
		}
		if items[i].Remote != nil {
			if decErr := r.decryptRemote(items[i].Remote); decErr != nil {
				return nil, decErr
			}
		}
	}
	return items, err
}

func (r *Repository) ListEnabledBackups(ctx context.Context) ([]models.Backup, error) {
	var items []models.Backup
	err := r.db.WithContext(ctx).
		Where("enabled = ?", true).
		Preload("Connection").
		Preload("Remote").
		Find(&items).Error
	if err != nil {
		return nil, err
	}
	for i := range items {
		if decErr := r.decryptConnection(&items[i].Connection); decErr != nil {
			return nil, decErr
		}
		if items[i].Remote != nil {
			if decErr := r.decryptRemote(items[i].Remote); decErr != nil {
				return nil, decErr
			}
		}
	}
	return items, nil
}

func (r *Repository) GetBackup(ctx context.Context, id string) (models.Backup, error) {
	var b models.Backup
	err := r.db.WithContext(ctx).
		Preload("Connection").
		Preload("Remote").
		First(&b, "id = ?", id).Error
	if err != nil {
		return b, err
	}
	if err := r.decryptConnection(&b.Connection); err != nil {
		return models.Backup{}, err
	}
	if b.Remote != nil {
		if err := r.decryptRemote(b.Remote); err != nil {
			return models.Backup{}, err
		}
	}
	return b, nil
}

func (r *Repository) SetBackupEnabled(ctx context.Context, id string, enabled bool) error {
	res := r.db.WithContext(ctx).
		Model(&models.Backup{}).
		Where("id = ?", id).
		Update("enabled", enabled)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func (r *Repository) TouchBackupRun(ctx context.Context, id string, lastRun time.Time, nextRun *time.Time) error {
	updates := map[string]any{
		"last_run_at": lastRun,
		"next_run_at": nextRun,
	}
	return r.db.WithContext(ctx).
		Model(&models.Backup{}).
		Where("id = ?", id).
		Updates(updates).Error
}

func (r *Repository) UpdateBackupNextRun(ctx context.Context, id string, nextRun *time.Time) error {
	return r.db.WithContext(ctx).
		Model(&models.Backup{}).
		Where("id = ?", id).
		Update("next_run_at", nextRun).Error
}

func (r *Repository) StartBackupRun(ctx context.Context, backupID string) (models.BackupRun, error) {
	run := models.BackupRun{
		ID:        uuid.NewString(),
		BackupID:  backupID,
		Status:    "running",
		StartedAt: time.Now().UTC(),
	}
	if err := r.db.WithContext(ctx).Create(&run).Error; err != nil {
		return models.BackupRun{}, err
	}
	return run, nil
}

func (r *Repository) FinishBackupRun(ctx context.Context, runID, status, errorText, outputKey string) error {
	now := time.Now().UTC()
	return r.db.WithContext(ctx).
		Model(&models.BackupRun{}).
		Where("id = ?", runID).
		Updates(map[string]any{
			"status":      status,
			"error_text":  errorText,
			"output_key":  outputKey,
			"finished_at": &now,
		}).Error
}

func (r *Repository) ListBackupRuns(ctx context.Context, backupID string, limit int) ([]models.BackupRun, error) {
	if limit <= 0 {
		limit = 50
	}
	var items []models.BackupRun
	err := r.db.WithContext(ctx).
		Where("backup_id = ?", backupID).
		Order("started_at desc").
		Limit(limit).
		Find(&items).Error
	return items, err
}

func (r *Repository) ListRecentRuns(ctx context.Context, limit int) ([]models.BackupRun, error) {
	if limit <= 0 {
		limit = 50
	}
	var items []models.BackupRun
	err := r.db.WithContext(ctx).
		Order("started_at desc").
		Limit(limit).
		Find(&items).Error
	return items, err
}

func (r *Repository) ListRunsSince(ctx context.Context, since time.Time) ([]models.BackupRun, error) {
	var items []models.BackupRun
	err := r.db.WithContext(ctx).
		Where("started_at >= ?", since).
		Order("started_at desc").
		Find(&items).Error
	return items, err
}

func (r *Repository) GetBackupRun(ctx context.Context, id string) (models.BackupRun, error) {
	var run models.BackupRun
	err := r.db.WithContext(ctx).First(&run, "id = ?", id).Error
	return run, err
}

func (r *Repository) decryptConnection(c *models.Connection) error {
	plain, err := r.crypto.DecryptString(c.Password)
	if err != nil {
		return err
	}
	c.Password = plain
	return nil
}

func (r *Repository) decryptRemote(rem *models.Remote) error {
	access, err := r.crypto.DecryptString(rem.AccessKey)
	if err != nil {
		return err
	}
	secret, err := r.crypto.DecryptString(rem.SecretKey)
	if err != nil {
		return err
	}
	rem.AccessKey = access
	rem.SecretKey = secret
	return nil
}

func (r *Repository) encryptNotificationSecrets(n *models.NotificationDestination) error {
	if strings.TrimSpace(n.DiscordWebhookURL) != "" {
		enc, err := r.crypto.EncryptString(n.DiscordWebhookURL)
		if err != nil {
			return err
		}
		n.DiscordWebhookURL = enc
	}
	if strings.TrimSpace(n.SMTPPassword) != "" {
		enc, err := r.crypto.EncryptString(n.SMTPPassword)
		if err != nil {
			return err
		}
		n.SMTPPassword = enc
	}
	return nil
}

func (r *Repository) decryptNotification(n *models.NotificationDestination) error {
	if strings.TrimSpace(n.DiscordWebhookURL) != "" {
		plain, err := r.crypto.DecryptString(n.DiscordWebhookURL)
		if err != nil {
			return err
		}
		n.DiscordWebhookURL = plain
	}
	if strings.TrimSpace(n.SMTPPassword) != "" {
		plain, err := r.crypto.DecryptString(n.SMTPPassword)
		if err != nil {
			return err
		}
		n.SMTPPassword = plain
	}
	return nil
}
