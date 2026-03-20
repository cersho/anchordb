package repository

import (
	"context"
	"errors"
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
	res := r.db.WithContext(ctx).Delete(&models.Connection{}, "id = ?", id)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
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
	if err := r.db.WithContext(ctx).Model(&models.Backup{}).Where("id = ?", id).Updates(map[string]any{
		"name":           existing.Name,
		"connection_id":  existing.ConnectionID,
		"cron_expr":      existing.CronExpr,
		"timezone":       existing.Timezone,
		"target_type":    existing.TargetType,
		"local_path":     existing.LocalPath,
		"remote_id":      existing.RemoteID,
		"retention_days": existing.RetentionDays,
		"compression":    existing.Compression,
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
