package models

import "time"

type Connection struct {
	ID        string    `gorm:"primaryKey;size:36" json:"id"`
	Name      string    `gorm:"size:255;not null" json:"name"`
	Type      string    `gorm:"size:32;not null" json:"type"`
	Host      string    `gorm:"size:255;not null" json:"host"`
	Port      int       `gorm:"not null" json:"port"`
	Database  string    `gorm:"size:255;not null" json:"database"`
	Username  string    `gorm:"size:255;not null" json:"username"`
	Password  string    `gorm:"size:1024;not null" json:"password"`
	SSLMode   string    `gorm:"size:32" json:"ssl_mode"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Remote struct {
	ID         string    `gorm:"primaryKey;size:36" json:"id"`
	Name       string    `gorm:"size:255;not null" json:"name"`
	Provider   string    `gorm:"size:32;not null" json:"provider"`
	Bucket     string    `gorm:"size:255;not null" json:"bucket"`
	Region     string    `gorm:"size:255;not null" json:"region"`
	Endpoint   string    `gorm:"size:512" json:"endpoint"`
	AccessKey  string    `gorm:"size:1024;not null" json:"access_key"`
	SecretKey  string    `gorm:"size:1024;not null" json:"secret_key"`
	PathPrefix string    `gorm:"size:512" json:"path_prefix"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type Backup struct {
	ID                 string     `gorm:"primaryKey;size:36" json:"id"`
	Name               string     `gorm:"size:255;not null" json:"name"`
	ConnectionID       string     `gorm:"size:36;not null;index" json:"connection_id"`
	CronExpr           string     `gorm:"size:255;not null" json:"cron_expr"`
	Timezone           string     `gorm:"size:128;not null;default:UTC" json:"timezone"`
	Enabled            bool       `gorm:"not null;default:true" json:"enabled"`
	TargetType         string     `gorm:"size:32;not null" json:"target_type"`
	LocalPath          string     `gorm:"size:1024" json:"local_path"`
	RemoteID           *string    `gorm:"size:36;index" json:"remote_id"`
	RetentionDays      int        `gorm:"not null;default:7" json:"retention_days"`
	Compression        string     `gorm:"size:32;not null;default:gzip" json:"compression"`
	IncludeFileStorage bool       `gorm:"not null;default:false" json:"include_file_storage"`
	LastRunAt          *time.Time `json:"last_run_at"`
	NextRunAt          *time.Time `json:"next_run_at"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`

	Connection Connection `gorm:"foreignKey:ConnectionID" json:"connection,omitempty"`
	Remote     *Remote    `gorm:"foreignKey:RemoteID" json:"remote,omitempty"`
}

type BackupRun struct {
	ID         string     `gorm:"primaryKey;size:36" json:"id"`
	BackupID   string     `gorm:"size:36;not null;index" json:"backup_id"`
	Status     string     `gorm:"size:32;not null" json:"status"`
	ErrorText  string     `gorm:"size:2048" json:"error_text"`
	OutputKey  string     `gorm:"size:1024" json:"output_key"`
	StartedAt  time.Time  `gorm:"not null" json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

type NotificationDestination struct {
	ID                string    `gorm:"primaryKey;size:36" json:"id"`
	Name              string    `gorm:"size:255;not null" json:"name"`
	Type              string    `gorm:"size:32;not null" json:"type"`
	Enabled           bool      `gorm:"not null;default:true" json:"enabled"`
	DiscordWebhookURL string    `gorm:"size:2048" json:"discord_webhook_url"`
	SMTPHost          string    `gorm:"size:255" json:"smtp_host"`
	SMTPPort          int       `gorm:"not null;default:587" json:"smtp_port"`
	SMTPUsername      string    `gorm:"size:255" json:"smtp_username"`
	SMTPPassword      string    `gorm:"size:1024" json:"smtp_password"`
	SMTPFrom          string    `gorm:"size:255" json:"smtp_from"`
	SMTPTo            string    `gorm:"size:1024" json:"smtp_to"`
	SMTPSecurity      string    `gorm:"size:32;not null;default:starttls" json:"smtp_security"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type BackupNotification struct {
	ID             string                  `gorm:"primaryKey;size:36" json:"id"`
	BackupID       string                  `gorm:"size:36;not null;index:idx_backup_notification_unique,unique" json:"backup_id"`
	NotificationID string                  `gorm:"size:36;not null;index:idx_backup_notification_unique,unique;index" json:"notification_id"`
	OnSuccess      bool                    `gorm:"not null;default:true" json:"on_success"`
	OnFailure      bool                    `gorm:"not null;default:true" json:"on_failure"`
	Enabled        bool                    `gorm:"not null;default:true" json:"enabled"`
	CreatedAt      time.Time               `json:"created_at"`
	UpdatedAt      time.Time               `json:"updated_at"`
	Backup         Backup                  `gorm:"foreignKey:BackupID" json:"backup,omitempty"`
	Notification   NotificationDestination `gorm:"foreignKey:NotificationID" json:"notification,omitempty"`
}
