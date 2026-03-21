package ui

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"anchordb/internal/config"
	"anchordb/internal/health"
	"anchordb/internal/models"
	"anchordb/internal/notifications"
	"anchordb/internal/repository"
	"anchordb/internal/scheduler"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-chi/chi/v5"
	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
)

//go:embed templates/*.html
var templateFS embed.FS

type Handler struct {
	repo      *repository.Repository
	scheduler *scheduler.Scheduler
	notifier  *notifications.Dispatcher
	cfg       config.Config
	tmpl      *template.Template
}

type pageData struct {
	Connections              []models.Connection
	Remotes                  []models.Remote
	Backups                  []models.Backup
	HealthChecks             []models.HealthCheck
	DownHealthChecks         []models.HealthCheck
	Notifications            []models.NotificationDestination
	BackupBindings           []models.BackupNotification
	HealthCheckBindings      []models.HealthCheckNotification
	Runs                     []models.BackupRun
	RunItems                 []runListItem
	RunLog                   []runLogLine
	RunsPage                 int
	RunsPageSize             int
	RunsHasPrev              bool
	RunsHasNext              bool
	SelectedRunID            string
	SelectedRun              runListItem
	HasSelectedRun           bool
	SelectedBackupID         string
	SelectedHealthCheckID    string
	BackupNotificationCounts map[string]int
	HealthNotificationCounts map[string]int
	TotalHealthChecks        int
	DownHealthCheckCount     int
	HealthSummaryLabel       string
	CurrentPage              string
	Dashboard                dashboardStats

	ConnectionsMsg   string
	ConnectionsErr   string
	RemotesMsg       string
	RemotesErr       string
	BackupsMsg       string
	BackupsErr       string
	HealthChecksMsg  string
	HealthChecksErr  string
	NotificationsMsg string
	NotificationsErr string
	RunsErr          string
}

type dashboardStats struct {
	TotalConnections   int
	HealthyConnections int
	FailedConnections  int
	TotalSchedules     int
	EnabledSchedules   int
	RunsLast30Days     int
	SuccessRate        int
	NextRunLabel       string
}

type runListItem struct {
	ID             string
	BackupID       string
	BackupName     string
	ConnectionName string
	Status         string
	ErrorText      string
	OutputKey      string
	StartedAt      time.Time
	FinishedAt     *time.Time
}

type runLogLine struct {
	Time    string
	Level   string
	Message string
}

func NewHandler(repo *repository.Repository, scheduler *scheduler.Scheduler, cfg config.Config) *Handler {
	tmpl := template.Must(template.New("app").Funcs(template.FuncMap{
		"inc": func(v int) int {
			return v + 1
		},
		"dec": func(v int) int {
			if v <= 1 {
				return 1
			}
			return v - 1
		},
		"isEnabled": func(v bool) string {
			if v {
				return "enabled"
			}
			return "disabled"
		},
		"typeLabel": connectionTypeLabel,
		"dbIcon":    connectionTypeIconName,
		"connectionAddress": func(c models.Connection) string {
			if strings.EqualFold(c.Type, "convex") {
				return c.Host
			}
			if strings.EqualFold(c.Type, "d1") {
				return c.Host
			}
			return c.Host + ":" + strconv.Itoa(c.Port)
		},
		"ago":      ago,
		"duration": runDuration,
		"runTone": func(status string) string {
			switch status {
			case "success":
				return "ok"
			case "failed":
				return "err"
			default:
				return "muted"
			}
		},
		"notificationTypeLabel": func(kind string) string {
			switch strings.ToLower(strings.TrimSpace(kind)) {
			case "discord":
				return "Discord"
			case "smtp":
				return "SMTP"
			default:
				return strings.ToUpper(strings.TrimSpace(kind))
			}
		},
		"bindingEnabled": func(bindings []models.BackupNotification, notificationID string) bool {
			for _, item := range bindings {
				if item.NotificationID == notificationID && item.Enabled {
					return true
				}
			}
			return false
		},
		"bindingOnSuccess": func(bindings []models.BackupNotification, notificationID string) bool {
			for _, item := range bindings {
				if item.NotificationID == notificationID && item.Enabled {
					return item.OnSuccess
				}
			}
			return false
		},
		"bindingOnFailure": func(bindings []models.BackupNotification, notificationID string) bool {
			for _, item := range bindings {
				if item.NotificationID == notificationID && item.Enabled {
					return item.OnFailure
				}
			}
			return false
		},
		"healthBindingEnabled": func(bindings []models.HealthCheckNotification, notificationID string) bool {
			for _, item := range bindings {
				if item.NotificationID == notificationID && item.Enabled {
					return true
				}
			}
			return false
		},
		"healthBindingOnDown": func(bindings []models.HealthCheckNotification, notificationID string) bool {
			for _, item := range bindings {
				if item.NotificationID == notificationID && item.Enabled {
					return item.OnDown
				}
			}
			return false
		},
		"healthBindingOnRecovered": func(bindings []models.HealthCheckNotification, notificationID string) bool {
			for _, item := range bindings {
				if item.NotificationID == notificationID && item.Enabled {
					return item.OnRecovered
				}
			}
			return false
		},
	}).ParseFS(templateFS, "templates/*.html"))

	return &Handler{repo: repo, scheduler: scheduler, notifier: notifications.NewDispatcher(repo), cfg: cfg, tmpl: tmpl}
}

func (h *Handler) Router() http.Handler {
	r := chi.NewRouter()

	r.Get("/", h.home)
	r.Get("/dashboard", h.dashboardPage)
	r.Get("/connections", h.connectionsPage)
	r.Get("/connections/section", h.connectionsSection)
	r.Post("/connections", h.createConnection)
	r.Post("/connections/{id}/update", h.updateConnection)
	r.Post("/connections/{id}/delete", h.deleteConnection)
	r.Post("/connections/test", h.testConnection)
	r.Post("/connections/test-all", h.testAllConnections)
	r.Post("/connections/{id}/test", h.testSavedConnection)
	r.Get("/remotes", h.remotesPage)
	r.Get("/remotes/section", h.remotesSection)
	r.Post("/remotes", h.createRemote)
	r.Post("/remotes/{id}/update", h.updateRemote)
	r.Post("/remotes/{id}/delete", h.deleteRemote)
	r.Get("/notifications", h.notificationsPage)
	r.Get("/notifications/section", h.notificationsSection)
	r.Post("/notifications", h.createNotification)
	r.Post("/notifications/{id}/update", h.updateNotification)
	r.Post("/notifications/{id}/test", h.testNotification)
	r.Post("/notifications/{id}/delete", h.deleteNotification)
	r.Post("/notifications/bindings", h.saveNotificationBindings)
	r.Get("/health-checks", h.healthChecksPage)
	r.Get("/health-checks/section", h.healthChecksSection)
	r.Post("/health-checks/{id}/settings", h.updateHealthCheckSettings)
	r.Post("/health-checks/{id}/archive", h.archiveHealthCheck)
	r.Post("/health-checks/{id}/run", h.runHealthCheckNow)
	r.Post("/health-checks/bindings", h.saveHealthCheckBindings)
	r.Get("/backups", h.backupsPage)
	r.Get("/schedules", h.backupsPage)
	r.Get("/backups/section", h.backupsSection)
	r.Get("/schedules/section", h.backupsSection)
	r.Post("/backups", h.createBackup)
	r.Post("/backups/{id}/update", h.updateBackup)
	r.Post("/backups/{id}/run", h.runBackupNow)
	r.Post("/backups/{id}/toggle", h.toggleBackup)
	r.Post("/backups/{id}/delete", h.deleteBackup)
	r.Get("/backups/{id}/runs", h.backupRuns)
	r.Get("/runs", h.runsPage)
	r.Get("/runs/section", h.runsSection)
	r.Get("/runs/{id}/download", h.downloadRun)

	return r
}

func (h *Handler) home(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/app/dashboard", http.StatusFound)
}

func (h *Handler) dashboardPage(w http.ResponseWriter, r *http.Request) {
	data, err := h.loadDashboardData(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.CurrentPage = "dashboard"
	h.renderPage(w, r, data)
}

func (h *Handler) connectionsPage(w http.ResponseWriter, r *http.Request) {
	items, err := h.repo.ListConnections(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redactConnections(items)
	h.renderPage(w, r, pageData{CurrentPage: "connections", Connections: items})
}

func (h *Handler) connectionsSection(w http.ResponseWriter, r *http.Request) {
	items, err := h.repo.ListConnections(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redactConnections(items)
	h.render(w, "connections_section", pageData{Connections: items})
}

func (h *Handler) remotesSection(w http.ResponseWriter, r *http.Request) {
	items, err := h.repo.ListRemotes(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redactRemotes(items)
	h.render(w, "remotes_section", pageData{Remotes: items})
}

func (h *Handler) remotesPage(w http.ResponseWriter, r *http.Request) {
	items, err := h.repo.ListRemotes(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redactRemotes(items)
	h.renderPage(w, r, pageData{CurrentPage: "remotes", Remotes: items})
}

func (h *Handler) notificationsPage(w http.ResponseWriter, r *http.Request) {
	selectedBackupID := strings.TrimSpace(r.URL.Query().Get("backup_id"))
	data, err := h.loadNotificationsData(r.Context(), selectedBackupID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.CurrentPage = "notifications"
	h.renderPage(w, r, data)
}

func (h *Handler) notificationsSection(w http.ResponseWriter, r *http.Request) {
	selectedBackupID := strings.TrimSpace(r.URL.Query().Get("backup_id"))
	data, err := h.loadNotificationsData(r.Context(), selectedBackupID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.render(w, "notifications_section", data)
}

func (h *Handler) healthChecksPage(w http.ResponseWriter, r *http.Request) {
	selectedHealthCheckID := strings.TrimSpace(r.URL.Query().Get("health_check_id"))
	data, err := h.loadHealthChecksData(r.Context(), selectedHealthCheckID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.CurrentPage = "health-checks"
	h.renderPage(w, r, data)
}

func (h *Handler) healthChecksSection(w http.ResponseWriter, r *http.Request) {
	selectedHealthCheckID := strings.TrimSpace(r.URL.Query().Get("health_check_id"))
	data, err := h.loadHealthChecksData(r.Context(), selectedHealthCheckID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.render(w, "health_checks_section", data)
}

func (h *Handler) backupsSection(w http.ResponseWriter, r *http.Request) {
	data, err := h.loadBackupsData(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.CurrentPage = "schedules"
	h.render(w, "backups_section", data)
}

func (h *Handler) backupsPage(w http.ResponseWriter, r *http.Request) {
	data, err := h.loadBackupsData(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.CurrentPage = "schedules"
	h.renderPage(w, r, data)
}

func (h *Handler) backupRuns(w http.ResponseWriter, r *http.Request) {
	backupID := chi.URLParam(r, "id")
	http.Redirect(w, r, "/app/runs?backup_id="+backupID, http.StatusFound)
}

func (h *Handler) runsPage(w http.ResponseWriter, r *http.Request) {
	data, err := h.loadRunsData(
		r.Context(),
		strings.TrimSpace(r.URL.Query().Get("run_id")),
		parsePositiveInt(r.URL.Query().Get("page"), 1),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.CurrentPage = "runs"
	h.renderPage(w, r, data)
}

func (h *Handler) runsSection(w http.ResponseWriter, r *http.Request) {
	data, err := h.loadRunsData(
		r.Context(),
		strings.TrimSpace(r.URL.Query().Get("run_id")),
		parsePositiveInt(r.URL.Query().Get("page"), 1),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.render(w, "runs_section", data)
}

func (h *Handler) loadRunsData(ctx context.Context, selectedRunID string, page int) (pageData, error) {
	if page < 1 {
		page = 1
	}
	const pageSize = 15
	offset := (page - 1) * pageSize

	backups, err := h.repo.ListBackups(ctx)
	if err != nil {
		return pageData{}, err
	}
	redactBackups(backups)

	backupByID := make(map[string]models.Backup, len(backups))
	for _, item := range backups {
		backupByID[item.ID] = item
	}

	runs, err := h.repo.ListRecentRunsPage(ctx, offset, pageSize+1)
	if err != nil {
		return pageData{}, err
	}

	hasNext := len(runs) > pageSize
	if hasNext {
		runs = runs[:pageSize]
	}

	runItems := make([]runListItem, 0, len(runs))
	for _, run := range runs {
		backup := backupByID[run.BackupID]
		runItems = append(runItems, runListItem{
			ID:             run.ID,
			BackupID:       run.BackupID,
			BackupName:     backup.Name,
			ConnectionName: backup.Connection.Name,
			Status:         run.Status,
			ErrorText:      run.ErrorText,
			OutputKey:      run.OutputKey,
			StartedAt:      run.StartedAt,
			FinishedAt:     run.FinishedAt,
		})
	}

	data := pageData{
		Backups:      backups,
		RunItems:     runItems,
		RunsPage:     page,
		RunsPageSize: pageSize,
		RunsHasPrev:  page > 1,
		RunsHasNext:  hasNext,
	}
	if len(runItems) == 0 {
		return data, nil
	}

	selected := runItems[0]
	if selectedRunID != "" {
		for _, item := range runItems {
			if item.ID == selectedRunID {
				selected = item
				break
			}
		}
	}

	data.SelectedRunID = selected.ID
	data.SelectedRun = selected
	data.HasSelectedRun = true
	data.RunLog = buildRunLog(selected)
	return data, nil
}

func (h *Handler) updateConnection(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	if id == "" {
		h.renderConnections(w, "", "connection id is required")
		return
	}
	if err := r.ParseForm(); err != nil {
		h.renderConnections(w, "", "Invalid form data")
		return
	}

	port, err := parsePort(r.FormValue("port"))
	if err != nil {
		h.renderConnections(w, "", err.Error())
		return
	}

	item := models.Connection{
		Name:     strings.TrimSpace(r.FormValue("name")),
		Type:     strings.ToLower(strings.TrimSpace(r.FormValue("type"))),
		Host:     strings.TrimSpace(r.FormValue("host")),
		Port:     port,
		Database: strings.TrimSpace(r.FormValue("database")),
		Username: strings.TrimSpace(r.FormValue("username")),
		Password: r.FormValue("password"),
		SSLMode:  strings.TrimSpace(r.FormValue("ssl_mode")),
	}

	if item.Name == "" || item.Type == "" || item.Host == "" {
		h.renderConnections(w, "", "name, type, and host are required")
		return
	}
	if item.Type != "postgres" && item.Type != "postgresql" && item.Type != "mysql" && item.Type != "convex" && item.Type != "d1" {
		h.renderConnections(w, "", "type must be mysql, postgres, convex, or d1")
		return
	}
	if item.Type == "d1" && item.Database == "" {
		h.renderConnections(w, "", "database is required for d1")
		return
	}
	if item.Type != "convex" && item.Type != "d1" && (item.Database == "" || item.Username == "") {
		h.renderConnections(w, "", "database and username are required for mysql/postgres")
		return
	}
	if item.Port == 0 {
		switch item.Type {
		case "postgres", "postgresql":
			item.Port = 5432
		case "mysql":
			item.Port = 3306
		}
	}
	if item.Type == "convex" {
		if item.Database == "" {
			item.Database = "convex"
		}
		if item.Username == "" {
			item.Username = "convex"
		}
		item.SSLMode = ""
	}
	if item.Type == "d1" {
		if item.Username == "" {
			item.Username = "d1"
		}
		item.Port = 0
		item.SSLMode = ""
	}

	if _, err := h.repo.UpdateConnection(r.Context(), id, &item); err != nil {
		h.renderConnections(w, "", err.Error())
		return
	}

	h.renderConnections(w, "Connection updated", "")
}

func (h *Handler) deleteConnection(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	if id == "" {
		h.renderConnections(w, "", "connection id is required")
		return
	}
	if err := h.repo.DeleteConnection(r.Context(), id); err != nil {
		h.renderConnections(w, "", "connection not found")
		return
	}
	h.renderConnections(w, "Connection deleted", "")
}

func (h *Handler) downloadRun(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(chi.URLParam(r, "id"))
	if runID == "" {
		http.Error(w, "run id is required", http.StatusBadRequest)
		return
	}

	run, err := h.repo.GetBackupRun(r.Context(), runID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if run.Status != "success" || strings.TrimSpace(run.OutputKey) == "" {
		http.Error(w, "backup artifact not available for this run", http.StatusBadRequest)
		return
	}

	backup, err := h.repo.GetBackup(r.Context(), run.BackupID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			http.Error(w, "backup not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	filename := filepath.Base(strings.ReplaceAll(run.OutputKey, "\\", "/"))
	if filename == "" || filename == "." || filename == "/" {
		filename = run.ID + ".sql"
	}

	switch backup.TargetType {
	case "local":
		h.downloadLocalRun(w, r, backup, run.OutputKey, filename)
		return
	case "s3":
		h.downloadS3Run(w, r, backup, run.OutputKey, filename)
		return
	default:
		http.Error(w, "unsupported backup target", http.StatusBadRequest)
		return
	}
}

func (h *Handler) downloadLocalRun(w http.ResponseWriter, r *http.Request, backup models.Backup, outputKey, filename string) {
	root := strings.TrimSpace(backup.LocalPath)
	if root == "" {
		root = h.cfg.DefaultLocalBasePath
	}

	rootAbs, err := filepath.Abs(root)
	if err != nil {
		http.Error(w, "invalid backup path", http.StatusInternalServerError)
		return
	}

	fullPath := filepath.Join(rootAbs, filepath.FromSlash(outputKey))
	fullAbs, err := filepath.Abs(fullPath)
	if err != nil {
		http.Error(w, "invalid backup path", http.StatusInternalServerError)
		return
	}

	if fullAbs != rootAbs && !strings.HasPrefix(fullAbs, rootAbs+string(filepath.Separator)) {
		http.Error(w, "invalid backup path", http.StatusBadRequest)
		return
	}

	f, err := os.Open(fullAbs)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "backup file not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to open backup file", http.StatusInternalServerError)
		return
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		http.Error(w, "failed to read backup file metadata", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+filename+"\"")
	http.ServeContent(w, r, filename, info.ModTime(), f)
}

func (h *Handler) downloadS3Run(w http.ResponseWriter, r *http.Request, backup models.Backup, outputKey, filename string) {
	if backup.Remote == nil {
		http.Error(w, "remote target is missing", http.StatusBadRequest)
		return
	}

	client, err := s3ClientFromRemote(r.Context(), *backup.Remote)
	if err != nil {
		http.Error(w, "failed to initialize remote client", http.StatusInternalServerError)
		return
	}

	obj, err := client.GetObject(r.Context(), &s3.GetObjectInput{
		Bucket: &backup.Remote.Bucket,
		Key:    &outputKey,
	})
	if err != nil {
		http.Error(w, "backup object not found", http.StatusNotFound)
		return
	}
	defer func() { _ = obj.Body.Close() }()

	contentType := "application/octet-stream"
	if obj.ContentType != nil && strings.TrimSpace(*obj.ContentType) != "" {
		contentType = *obj.ContentType
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", "attachment; filename=\""+filename+"\"")
	if obj.ContentLength != nil {
		w.Header().Set("Content-Length", strconv.FormatInt(*obj.ContentLength, 10))
	}

	if _, err := io.Copy(w, obj.Body); err != nil {
		return
	}
}

func s3ClientFromRemote(ctx context.Context, rem models.Remote) (*s3.Client, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(rem.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(rem.AccessKey, rem.SecretKey, "")),
	}

	usePathStyle := false
	if rem.Endpoint != "" {
		usePathStyle = true
		resolver := s3.EndpointResolverFromURL(rem.Endpoint)
		loadOpts = append(loadOpts, awsconfig.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				if service == s3.ServiceID {
					return resolver.ResolveEndpoint(region, s3.EndpointResolverOptions{})
				}
				return aws.Endpoint{}, errors.New("unknown endpoint requested")
			},
		)))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
	}), nil
}

func (h *Handler) createConnection(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.renderConnections(w, "", "Invalid form data")
		return
	}

	port, err := parsePort(r.FormValue("port"))
	if err != nil {
		h.renderConnections(w, "", err.Error())
		return
	}

	item := models.Connection{
		Name:     strings.TrimSpace(r.FormValue("name")),
		Type:     strings.ToLower(strings.TrimSpace(r.FormValue("type"))),
		Host:     strings.TrimSpace(r.FormValue("host")),
		Port:     port,
		Database: strings.TrimSpace(r.FormValue("database")),
		Username: strings.TrimSpace(r.FormValue("username")),
		Password: r.FormValue("password"),
		SSLMode:  strings.TrimSpace(r.FormValue("ssl_mode")),
	}

	if item.Name == "" || item.Type == "" || item.Host == "" || item.Password == "" {
		h.renderConnections(w, "", "name, type, host, and password are required")
		return
	}
	if item.Type != "postgres" && item.Type != "postgresql" && item.Type != "mysql" && item.Type != "convex" && item.Type != "d1" {
		h.renderConnections(w, "", "type must be mysql, postgres, convex, or d1")
		return
	}
	if item.Type == "d1" && item.Database == "" {
		h.renderConnections(w, "", "database is required for d1")
		return
	}
	if item.Type != "convex" && item.Type != "d1" && (item.Database == "" || item.Username == "") {
		h.renderConnections(w, "", "database and username are required for mysql/postgres")
		return
	}
	if item.Port == 0 {
		switch item.Type {
		case "postgres", "postgresql":
			item.Port = 5432
		case "mysql":
			item.Port = 3306
		}
	}
	if item.Type == "convex" {
		if item.Database == "" {
			item.Database = "convex"
		}
		if item.Username == "" {
			item.Username = "convex"
		}
		item.SSLMode = ""
	}
	if item.Type == "d1" {
		if item.Username == "" {
			item.Username = "d1"
		}
		item.Port = 0
		item.SSLMode = ""
	}

	if err := h.repo.CreateConnection(r.Context(), &item); err != nil {
		h.renderConnections(w, "", err.Error())
		return
	}
	if _, err := h.repo.UpsertHealthCheckForConnection(r.Context(), item.ID, repository.HealthCheckDefaults{
		IntervalSecond:   int(h.cfg.DefaultHealthEvery / time.Second),
		TimeoutSecond:    int(h.cfg.DefaultHealthTimeout / time.Second),
		FailureThreshold: h.cfg.DefaultFailThreshold,
		SuccessThreshold: h.cfg.DefaultPassThreshold,
	}); err != nil {
		h.renderConnections(w, "", err.Error())
		return
	}

	h.renderConnections(w, "Connection created", "")
}

func (h *Handler) testConnection(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.renderConnections(w, "", "Invalid form data")
		return
	}

	port, err := parsePort(r.FormValue("port"))
	if err != nil {
		h.renderConnections(w, "", err.Error())
		return
	}

	item := models.Connection{
		Name:     strings.TrimSpace(r.FormValue("name")),
		Type:     strings.ToLower(strings.TrimSpace(r.FormValue("type"))),
		Host:     strings.TrimSpace(r.FormValue("host")),
		Port:     port,
		Database: strings.TrimSpace(r.FormValue("database")),
		Username: strings.TrimSpace(r.FormValue("username")),
		Password: r.FormValue("password"),
		SSLMode:  strings.TrimSpace(r.FormValue("ssl_mode")),
	}

	if item.Type == "" || item.Host == "" {
		h.renderConnections(w, "", "type and host are required for connection test")
		return
	}

	result, err := h.probeConnection(r.Context(), item)
	if err != nil {
		h.renderConnections(w, "", "Connection test failed: "+err.Error())
		return
	}

	h.renderConnections(w, "Connection test passed: "+result, "")
}

func (h *Handler) testSavedConnection(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	if id == "" {
		h.renderConnections(w, "", "connection id is required")
		return
	}

	item, err := h.repo.GetConnection(r.Context(), id)
	if err != nil {
		h.renderConnections(w, "", "connection not found")
		return
	}

	result, err := h.probeConnection(r.Context(), item)
	if err != nil {
		h.renderConnections(w, "", "Connection test failed for "+item.Name+": "+err.Error())
		return
	}

	h.renderConnections(w, "Connection test passed for "+item.Name+": "+result, "")
}

func (h *Handler) testAllConnections(w http.ResponseWriter, r *http.Request) {
	items, err := h.repo.ListConnections(r.Context())
	if err != nil {
		h.renderConnections(w, "", err.Error())
		return
	}

	if len(items) == 0 {
		h.renderConnections(w, "", "No connections to test")
		return
	}

	passed := 0
	failures := make([]string, 0)
	for _, item := range items {
		if _, err := h.probeConnection(r.Context(), item); err != nil {
			failures = append(failures, item.Name+": "+err.Error())
			continue
		}
		passed++
	}

	if len(failures) == 0 {
		h.renderConnections(w, fmt.Sprintf("All %d connections passed connectivity checks", passed), "")
		return
	}

	h.renderConnections(w, fmt.Sprintf("%d/%d connections passed", passed, len(items)), strings.Join(failures, " | "))
}

func (h *Handler) probeConnection(ctx context.Context, item models.Connection) (string, error) {
	return health.ProbeConnection(ctx, item, h.cfg, 5*time.Second)
}

func (h *Handler) createRemote(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.renderRemotes(w, "", "Invalid form data")
		return
	}

	provider := strings.ToLower(strings.TrimSpace(r.FormValue("provider")))
	if provider == "" {
		provider = "s3"
	}

	item := models.Remote{
		Name:       strings.TrimSpace(r.FormValue("name")),
		Provider:   provider,
		Bucket:     strings.TrimSpace(r.FormValue("bucket")),
		Region:     strings.TrimSpace(r.FormValue("region")),
		Endpoint:   strings.TrimSpace(r.FormValue("endpoint")),
		AccessKey:  strings.TrimSpace(r.FormValue("access_key")),
		SecretKey:  strings.TrimSpace(r.FormValue("secret_key")),
		PathPrefix: strings.TrimSpace(r.FormValue("path_prefix")),
	}

	if item.Name == "" || item.Bucket == "" || item.Region == "" || item.AccessKey == "" || item.SecretKey == "" {
		h.renderRemotes(w, "", "name, bucket, region, access_key, secret_key are required")
		return
	}
	if item.Provider != "s3" {
		h.renderRemotes(w, "", "provider must be s3")
		return
	}

	if err := h.repo.CreateRemote(r.Context(), &item); err != nil {
		h.renderRemotes(w, "", err.Error())
		return
	}

	h.renderRemotes(w, "Remote created", "")
}

func (h *Handler) updateRemote(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	if id == "" {
		h.renderRemotes(w, "", "remote id is required")
		return
	}
	if err := r.ParseForm(); err != nil {
		h.renderRemotes(w, "", "Invalid form data")
		return
	}

	provider := strings.ToLower(strings.TrimSpace(r.FormValue("provider")))
	if provider == "" {
		provider = "s3"
	}

	item := models.Remote{
		Name:       strings.TrimSpace(r.FormValue("name")),
		Provider:   provider,
		Bucket:     strings.TrimSpace(r.FormValue("bucket")),
		Region:     strings.TrimSpace(r.FormValue("region")),
		Endpoint:   strings.TrimSpace(r.FormValue("endpoint")),
		AccessKey:  strings.TrimSpace(r.FormValue("access_key")),
		SecretKey:  strings.TrimSpace(r.FormValue("secret_key")),
		PathPrefix: strings.TrimSpace(r.FormValue("path_prefix")),
	}

	if item.Name == "" || item.Bucket == "" || item.Region == "" {
		h.renderRemotes(w, "", "name, bucket, and region are required")
		return
	}
	if item.Provider != "s3" {
		h.renderRemotes(w, "", "provider must be s3")
		return
	}

	if _, err := h.repo.UpdateRemote(r.Context(), id, &item); err != nil {
		h.renderRemotes(w, "", err.Error())
		return
	}

	h.renderRemotes(w, "Remote updated", "")
}

func (h *Handler) deleteRemote(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	if id == "" {
		h.renderRemotes(w, "", "remote id is required")
		return
	}
	if err := h.repo.DeleteRemote(r.Context(), id); err != nil {
		h.renderRemotes(w, "", "remote not found")
		return
	}
	h.renderRemotes(w, "Remote deleted", "")
}

func (h *Handler) createNotification(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.renderNotifications(w, "", "Invalid form data", strings.TrimSpace(r.FormValue("backup_id")))
		return
	}

	selectedBackupID := strings.TrimSpace(r.FormValue("backup_id"))
	notificationType := strings.ToLower(strings.TrimSpace(r.FormValue("type")))
	enabled := r.FormValue("enabled") == "on" || r.FormValue("enabled") == "true"

	item := models.NotificationDestination{
		Name:              strings.TrimSpace(r.FormValue("name")),
		Type:              notificationType,
		Enabled:           enabled,
		DiscordWebhookURL: strings.TrimSpace(r.FormValue("discord_webhook_url")),
		SMTPHost:          strings.TrimSpace(r.FormValue("smtp_host")),
		SMTPPort:          587,
		SMTPUsername:      strings.TrimSpace(r.FormValue("smtp_username")),
		SMTPPassword:      r.FormValue("smtp_password"),
		SMTPFrom:          strings.TrimSpace(r.FormValue("smtp_from")),
		SMTPTo:            strings.TrimSpace(r.FormValue("smtp_to")),
		SMTPSecurity:      strings.ToLower(strings.TrimSpace(r.FormValue("smtp_security"))),
	}

	portRaw := strings.TrimSpace(r.FormValue("smtp_port"))
	portMode := strings.TrimSpace(r.FormValue("smtp_port_mode"))
	if portMode != "" && portMode != "other" {
		portRaw = portMode
	}
	if portMode == "other" {
		portRaw = strings.TrimSpace(r.FormValue("smtp_port_custom"))
	}
	if portRaw != "" {
		port, err := strconv.Atoi(portRaw)
		if err != nil || port <= 0 || port > 65535 {
			h.renderNotifications(w, "", "smtp_port must be between 1 and 65535", selectedBackupID)
			return
		}
		item.SMTPPort = port
	}

	if err := h.repo.CreateNotification(r.Context(), &item); err != nil {
		h.renderNotifications(w, "", err.Error(), selectedBackupID)
		return
	}

	h.renderNotifications(w, "Notification destination created", "", selectedBackupID)
}

func (h *Handler) updateNotification(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	if id == "" {
		h.renderNotifications(w, "", "notification id is required", strings.TrimSpace(r.FormValue("backup_id")))
		return
	}
	if err := r.ParseForm(); err != nil {
		h.renderNotifications(w, "", "Invalid form data", strings.TrimSpace(r.FormValue("backup_id")))
		return
	}

	selectedBackupID := strings.TrimSpace(r.FormValue("backup_id"))
	notificationType := strings.ToLower(strings.TrimSpace(r.FormValue("type")))
	enabled := r.FormValue("enabled") == "on" || r.FormValue("enabled") == "true"

	name := strings.TrimSpace(r.FormValue("name"))
	discordWebhookURL := strings.TrimSpace(r.FormValue("discord_webhook_url"))
	smtpHost := strings.TrimSpace(r.FormValue("smtp_host"))
	smtpUsername := strings.TrimSpace(r.FormValue("smtp_username"))
	smtpPassword := r.FormValue("smtp_password")
	smtpFrom := strings.TrimSpace(r.FormValue("smtp_from"))
	smtpTo := strings.TrimSpace(r.FormValue("smtp_to"))
	smtpSecurity := strings.ToLower(strings.TrimSpace(r.FormValue("smtp_security")))

	smtpPort := 587
	portRaw := strings.TrimSpace(r.FormValue("smtp_port"))
	portMode := strings.TrimSpace(r.FormValue("smtp_port_mode"))
	if portMode != "" && portMode != "other" {
		portRaw = portMode
	}
	if portMode == "other" {
		portRaw = strings.TrimSpace(r.FormValue("smtp_port_custom"))
	}
	if portRaw != "" {
		port, err := strconv.Atoi(portRaw)
		if err != nil || port <= 0 || port > 65535 {
			h.renderNotifications(w, "", "smtp_port must be between 1 and 65535", selectedBackupID)
			return
		}
		smtpPort = port
	}

	if _, err := h.repo.UpdateNotification(r.Context(), id, repository.NotificationPatch{
		Name:              stringPtr(name),
		Type:              stringPtr(notificationType),
		Enabled:           boolPtr(enabled),
		DiscordWebhookURL: stringPtr(discordWebhookURL),
		SMTPHost:          stringPtr(smtpHost),
		SMTPPort:          intPtr(smtpPort),
		SMTPUsername:      stringPtr(smtpUsername),
		SMTPPassword:      stringPtr(smtpPassword),
		SMTPFrom:          stringPtr(smtpFrom),
		SMTPTo:            stringPtr(smtpTo),
		SMTPSecurity:      stringPtr(smtpSecurity),
	}); err != nil {
		h.renderNotifications(w, "", err.Error(), selectedBackupID)
		return
	}

	h.renderNotifications(w, "Notification destination updated", "", selectedBackupID)
}

func (h *Handler) testNotification(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	selectedBackupID := strings.TrimSpace(r.URL.Query().Get("backup_id"))

	item, err := h.repo.GetNotification(r.Context(), id)
	if err != nil {
		h.renderNotifications(w, "", "notification not found", selectedBackupID)
		return
	}
	if !item.Enabled {
		h.renderNotifications(w, "", "notification is disabled", selectedBackupID)
		return
	}
	if err := h.notifier.SendTestNotification(r.Context(), item); err != nil {
		h.renderNotifications(w, "", "Test notification failed: "+err.Error(), selectedBackupID)
		return
	}

	h.renderNotifications(w, "Test notification sent", "", selectedBackupID)
}

func (h *Handler) deleteNotification(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	selectedBackupID := strings.TrimSpace(r.URL.Query().Get("backup_id"))
	if err := h.repo.DeleteNotification(r.Context(), id); err != nil {
		h.renderNotifications(w, "", "notification not found", selectedBackupID)
		return
	}
	h.renderNotifications(w, "Notification destination deleted", "", selectedBackupID)
}

func (h *Handler) saveNotificationBindings(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.renderNotifications(w, "", "Invalid form data", strings.TrimSpace(r.FormValue("backup_id")))
		return
	}

	selectedBackupID := strings.TrimSpace(r.FormValue("backup_id"))
	if selectedBackupID == "" {
		h.renderNotifications(w, "", "backup_id is required", selectedBackupID)
		return
	}

	notifications, err := h.repo.ListNotifications(r.Context())
	if err != nil {
		h.renderNotifications(w, "", err.Error(), selectedBackupID)
		return
	}

	bindings := make([]models.BackupNotification, 0, len(notifications))
	for _, notification := range notifications {
		key := notification.ID
		enabled := r.FormValue("binding_enabled_"+key) == "on"
		if !enabled {
			continue
		}

		onSuccess := r.FormValue("binding_success_"+key) == "on"
		onFailure := r.FormValue("binding_failure_"+key) == "on"
		if !onSuccess && !onFailure {
			h.renderNotifications(w, "", "each enabled notification must have success or failure selected", selectedBackupID)
			return
		}

		bindings = append(bindings, models.BackupNotification{
			NotificationID: notification.ID,
			Enabled:        true,
			OnSuccess:      onSuccess,
			OnFailure:      onFailure,
		})
	}

	if _, err := h.repo.SetBackupNotifications(r.Context(), selectedBackupID, bindings); err != nil {
		h.renderNotifications(w, "", err.Error(), selectedBackupID)
		return
	}

	h.renderNotifications(w, "Schedule notifications updated", "", selectedBackupID)
}

func (h *Handler) updateHealthCheckSettings(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.renderHealthChecks(w, "", "Invalid form data", strings.TrimSpace(r.FormValue("health_check_id")))
		return
	}

	id := strings.TrimSpace(chi.URLParam(r, "id"))
	selectedHealthCheckID := strings.TrimSpace(r.FormValue("health_check_id"))
	if selectedHealthCheckID == "" {
		selectedHealthCheckID = id
	}
	if id == "" {
		h.renderHealthChecks(w, "", "health check id is required", selectedHealthCheckID)
		return
	}

	intervalSecond, err := strconv.Atoi(strings.TrimSpace(r.FormValue("check_interval_second")))
	if err != nil || intervalSecond <= 0 {
		h.renderHealthChecks(w, "", "check_interval_second must be a positive number", selectedHealthCheckID)
		return
	}
	timeoutSecond, err := strconv.Atoi(strings.TrimSpace(r.FormValue("timeout_second")))
	if err != nil || timeoutSecond <= 0 {
		h.renderHealthChecks(w, "", "timeout_second must be a positive number", selectedHealthCheckID)
		return
	}
	failureThreshold, err := strconv.Atoi(strings.TrimSpace(r.FormValue("failure_threshold")))
	if err != nil || failureThreshold <= 0 {
		h.renderHealthChecks(w, "", "failure_threshold must be a positive number", selectedHealthCheckID)
		return
	}
	successThreshold, err := strconv.Atoi(strings.TrimSpace(r.FormValue("success_threshold")))
	if err != nil || successThreshold <= 0 {
		h.renderHealthChecks(w, "", "success_threshold must be a positive number", selectedHealthCheckID)
		return
	}

	enabled := r.FormValue("enabled") == "on"
	if _, err := h.repo.UpdateHealthCheck(r.Context(), id, repository.HealthCheckPatch{
		Enabled:             &enabled,
		CheckIntervalSecond: &intervalSecond,
		TimeoutSecond:       &timeoutSecond,
		FailureThreshold:    &failureThreshold,
		SuccessThreshold:    &successThreshold,
	}); err != nil {
		h.renderHealthChecks(w, "", err.Error(), selectedHealthCheckID)
		return
	}

	h.renderHealthChecks(w, "Health check settings updated", "", selectedHealthCheckID)
}

func (h *Handler) runHealthCheckNow(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	selectedHealthCheckID := strings.TrimSpace(r.URL.Query().Get("health_check_id"))
	if selectedHealthCheckID == "" {
		selectedHealthCheckID = id
	}
	if id == "" {
		h.renderHealthChecks(w, "", "health check id is required", selectedHealthCheckID)
		return
	}

	checkItem, err := h.repo.GetHealthCheck(r.Context(), id)
	if err != nil {
		h.renderHealthChecks(w, "", "health check not found", selectedHealthCheckID)
		return
	}
	if checkItem.LastCheckedAt != nil && h.cfg.HealthManualCooldown > 0 {
		nextAllowedAt := checkItem.LastCheckedAt.UTC().Add(h.cfg.HealthManualCooldown)
		if time.Now().UTC().Before(nextAllowedAt) {
			retryAfter := int(time.Until(nextAllowedAt).Seconds())
			if retryAfter < 1 {
				retryAfter = 1
			}
			h.renderHealthChecks(w, "", "Manual health check is on cooldown. Retry in "+strconv.Itoa(retryAfter)+"s", selectedHealthCheckID)
			return
		}
	}

	timeoutSecond := checkItem.TimeoutSecond
	if timeoutSecond <= 0 {
		timeoutSecond = int(h.cfg.DefaultHealthTimeout / time.Second)
	}
	if timeoutSecond <= 0 {
		timeoutSecond = 5
	}
	timeout := time.Duration(timeoutSecond) * time.Second

	result, probeErr := health.ProbeConnection(r.Context(), checkItem.Connection, h.cfg, timeout)
	healthy := probeErr == nil
	errText := ""
	if probeErr != nil {
		errText = strings.TrimSpace(probeErr.Error())
	}

	updated, event, saveErr := h.repo.SaveHealthCheckProbeResult(r.Context(), checkItem.ID, time.Now().UTC(), healthy, errText)
	if saveErr != nil {
		h.renderHealthChecks(w, "", saveErr.Error(), selectedHealthCheckID)
		return
	}
	if event != "" {
		if notifyErr := h.notifier.NotifyHealthCheckEvent(r.Context(), updated, event); notifyErr != nil {
			h.renderHealthChecks(w, "", "health check updated but notification failed: "+notifyErr.Error(), selectedHealthCheckID)
			return
		}
		_ = h.repo.MarkHealthCheckNotified(r.Context(), updated.ID, time.Now().UTC())
	}

	if probeErr != nil {
		h.renderHealthChecks(w, "", "Health check failed: "+probeErr.Error(), selectedHealthCheckID)
		return
	}
	h.renderHealthChecks(w, "Health check passed: "+result, "", selectedHealthCheckID)
}

func (h *Handler) saveHealthCheckBindings(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.renderHealthChecks(w, "", "Invalid form data", strings.TrimSpace(r.FormValue("health_check_id")))
		return
	}

	selectedHealthCheckID := strings.TrimSpace(r.FormValue("health_check_id"))
	if selectedHealthCheckID == "" {
		h.renderHealthChecks(w, "", "health_check_id is required", selectedHealthCheckID)
		return
	}

	notifications, err := h.repo.ListNotifications(r.Context())
	if err != nil {
		h.renderHealthChecks(w, "", err.Error(), selectedHealthCheckID)
		return
	}

	bindings := make([]models.HealthCheckNotification, 0, len(notifications))
	for _, notification := range notifications {
		key := notification.ID
		enabled := r.FormValue("binding_enabled_"+key) == "on"
		if !enabled {
			continue
		}

		onDown := r.FormValue("binding_down_"+key) == "on"
		onRecovered := r.FormValue("binding_recovered_"+key) == "on"
		if !onDown && !onRecovered {
			h.renderHealthChecks(w, "", "each enabled notification must have down or recovered selected", selectedHealthCheckID)
			return
		}

		bindings = append(bindings, models.HealthCheckNotification{
			NotificationID: notification.ID,
			Enabled:        true,
			OnDown:         onDown,
			OnRecovered:    onRecovered,
		})
	}

	if _, err := h.repo.SetHealthCheckNotifications(r.Context(), selectedHealthCheckID, bindings); err != nil {
		h.renderHealthChecks(w, "", err.Error(), selectedHealthCheckID)
		return
	}

	h.renderHealthChecks(w, "Health check notifications updated", "", selectedHealthCheckID)
}

func (h *Handler) archiveHealthCheck(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	selectedHealthCheckID := strings.TrimSpace(r.URL.Query().Get("health_check_id"))
	if id == "" {
		h.renderHealthChecks(w, "", "health check id is required", selectedHealthCheckID)
		return
	}
	enabled := false
	if _, err := h.repo.UpdateHealthCheck(r.Context(), id, repository.HealthCheckPatch{Enabled: &enabled}); err != nil {
		h.renderHealthChecks(w, "", err.Error(), selectedHealthCheckID)
		return
	}
	h.renderHealthChecks(w, "Health check archived", "", selectedHealthCheckID)
}

func (h *Handler) createBackup(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.renderBackups(w, "", "Invalid form data")
		return
	}

	retention, err := parseRetention(r.FormValue("retention_days"))
	if err != nil {
		h.renderBackups(w, "", err.Error())
		return
	}

	targetType := strings.TrimSpace(r.FormValue("target_type"))
	compression := strings.TrimSpace(r.FormValue("compression"))
	if compression == "" {
		compression = "gzip"
	}
	includeFileStorage := r.FormValue("include_file_storage") == "true" || r.FormValue("include_file_storage") == "on"
	if targetType != "local" && targetType != "s3" {
		h.renderBackups(w, "", "target_type must be local or s3")
		return
	}

	cronExpr := strings.TrimSpace(r.FormValue("cron_expr"))
	if _, err := cron.ParseStandard(cronExpr); err != nil {
		h.renderBackups(w, "", "invalid cron_expr")
		return
	}

	timezone := strings.TrimSpace(r.FormValue("timezone"))
	if timezone == "" {
		timezone = "UTC"
	}

	item := models.Backup{
		Name:               strings.TrimSpace(r.FormValue("name")),
		ConnectionID:       strings.TrimSpace(r.FormValue("connection_id")),
		CronExpr:           cronExpr,
		Timezone:           timezone,
		Enabled:            true,
		TargetType:         targetType,
		LocalPath:          strings.TrimSpace(r.FormValue("local_path")),
		RetentionDays:      retention,
		Compression:        compression,
		IncludeFileStorage: includeFileStorage,
	}

	if item.Name == "" || item.ConnectionID == "" {
		h.renderBackups(w, "", "name and connection are required")
		return
	}
	conn, err := h.repo.GetConnection(r.Context(), item.ConnectionID)
	if err != nil {
		h.renderBackups(w, "", "connection_id not found")
		return
	}
	if strings.EqualFold(conn.Type, "convex") {
		item.Compression = "none"
	} else {
		item.IncludeFileStorage = false
		if item.Compression != "gzip" && item.Compression != "none" {
			h.renderBackups(w, "", "compression must be gzip or none")
			return
		}
	}
	if item.TargetType == "local" && item.LocalPath == "" {
		h.renderBackups(w, "", "local_path is required for local target")
		return
	}
	if item.TargetType == "s3" {
		id := strings.TrimSpace(r.FormValue("remote_id"))
		if id == "" {
			h.renderBackups(w, "", "remote_id is required for s3 target")
			return
		}
		if _, err := h.repo.GetRemote(r.Context(), id); err != nil {
			h.renderBackups(w, "", "remote_id not found")
			return
		}
		item.RemoteID = &id
	}

	if err := h.repo.CreateBackup(r.Context(), &item); err != nil {
		h.renderBackups(w, "", err.Error())
		return
	}
	if err := h.scheduler.Upsert(r.Context(), item.ID); err != nil {
		h.renderBackups(w, "", err.Error())
		return
	}

	h.renderBackups(w, "Backup created", "")
}

func (h *Handler) updateBackup(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	if id == "" {
		h.renderBackups(w, "", "backup id is required")
		return
	}
	if err := r.ParseForm(); err != nil {
		h.renderBackups(w, "", "Invalid form data")
		return
	}

	retention, err := parseRetention(r.FormValue("retention_days"))
	if err != nil {
		h.renderBackups(w, "", err.Error())
		return
	}

	targetType := strings.TrimSpace(r.FormValue("target_type"))
	compression := strings.TrimSpace(r.FormValue("compression"))
	if compression == "" {
		compression = "gzip"
	}
	includeFileStorage := r.FormValue("include_file_storage") == "true" || r.FormValue("include_file_storage") == "on"
	if targetType != "local" && targetType != "s3" {
		h.renderBackups(w, "", "target_type must be local or s3")
		return
	}

	cronExpr := strings.TrimSpace(r.FormValue("cron_expr"))
	if _, err := cron.ParseStandard(cronExpr); err != nil {
		h.renderBackups(w, "", "invalid cron_expr")
		return
	}

	timezone := strings.TrimSpace(r.FormValue("timezone"))
	if timezone == "" {
		timezone = "UTC"
	}

	item := models.Backup{
		Name:               strings.TrimSpace(r.FormValue("name")),
		ConnectionID:       strings.TrimSpace(r.FormValue("connection_id")),
		CronExpr:           cronExpr,
		Timezone:           timezone,
		TargetType:         targetType,
		LocalPath:          strings.TrimSpace(r.FormValue("local_path")),
		RetentionDays:      retention,
		Compression:        compression,
		IncludeFileStorage: includeFileStorage,
	}

	if item.Name == "" || item.ConnectionID == "" {
		h.renderBackups(w, "", "name and connection are required")
		return
	}
	conn, err := h.repo.GetConnection(r.Context(), item.ConnectionID)
	if err != nil {
		h.renderBackups(w, "", "connection_id not found")
		return
	}
	if strings.EqualFold(conn.Type, "convex") {
		item.Compression = "none"
	} else {
		item.IncludeFileStorage = false
		if item.Compression != "gzip" && item.Compression != "none" {
			h.renderBackups(w, "", "compression must be gzip or none")
			return
		}
	}
	if item.TargetType == "local" && item.LocalPath == "" {
		h.renderBackups(w, "", "local_path is required for local target")
		return
	}
	if item.TargetType == "s3" {
		remoteID := strings.TrimSpace(r.FormValue("remote_id"))
		if remoteID == "" {
			h.renderBackups(w, "", "remote_id is required for s3 target")
			return
		}
		if _, err := h.repo.GetRemote(r.Context(), remoteID); err != nil {
			h.renderBackups(w, "", "remote_id not found")
			return
		}
		item.RemoteID = &remoteID
	}

	updated, err := h.repo.UpdateBackup(r.Context(), id, &item)
	if err != nil {
		h.renderBackups(w, "", err.Error())
		return
	}

	if updated.Enabled {
		if err := h.scheduler.Upsert(r.Context(), id); err != nil {
			h.renderBackups(w, "", err.Error())
			return
		}
	} else {
		h.scheduler.Delete(id)
	}

	h.renderBackups(w, "Backup updated", "")
}

func (h *Handler) runBackupNow(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if _, err := h.repo.GetBackup(r.Context(), id); err != nil {
		h.renderBackups(w, "", "backup not found")
		return
	}
	h.scheduler.TriggerNow(id)
	h.renderBackups(w, "Backup run queued", "")
}

func (h *Handler) toggleBackup(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := r.ParseForm(); err != nil {
		h.renderBackups(w, "", "Invalid form data")
		return
	}
	enabled := r.FormValue("enabled") == "true"

	if err := h.repo.SetBackupEnabled(r.Context(), id, enabled); err != nil {
		h.renderBackups(w, "", "backup not found")
		return
	}

	if enabled {
		if err := h.scheduler.Upsert(r.Context(), id); err != nil {
			h.renderBackups(w, "", err.Error())
			return
		}
		h.renderBackups(w, "Backup enabled", "")
		return
	}
	h.scheduler.Delete(id)
	h.renderBackups(w, "Backup disabled", "")
}

func (h *Handler) deleteBackup(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	h.scheduler.Delete(id)
	if err := h.repo.DeleteBackup(r.Context(), id); err != nil {
		h.renderBackups(w, "", "backup not found")
		return
	}
	h.renderBackups(w, "Backup deleted", "")
}

func (h *Handler) renderConnections(w http.ResponseWriter, msg, errMsg string) {
	items, _ := h.repo.ListConnections(context.Background())
	redactConnections(items)
	h.render(w, "connections_section", pageData{CurrentPage: "connections", Connections: items, ConnectionsMsg: msg, ConnectionsErr: errMsg})
}

func (h *Handler) renderRemotes(w http.ResponseWriter, msg, errMsg string) {
	items, _ := h.repo.ListRemotes(context.Background())
	redactRemotes(items)
	h.render(w, "remotes_section", pageData{CurrentPage: "remotes", Remotes: items, RemotesMsg: msg, RemotesErr: errMsg})
}

func (h *Handler) renderBackups(w http.ResponseWriter, msg, errMsg string) {
	data, _ := h.loadBackupsData(context.Background())
	data.CurrentPage = "schedules"
	data.BackupsMsg = msg
	data.BackupsErr = errMsg
	h.render(w, "backups_section", data)
}

func (h *Handler) renderNotifications(w http.ResponseWriter, msg, errMsg, selectedBackupID string) {
	data, _ := h.loadNotificationsData(context.Background(), selectedBackupID)
	data.CurrentPage = "notifications"
	data.NotificationsMsg = msg
	data.NotificationsErr = errMsg
	h.render(w, "notifications_section", data)
}

func (h *Handler) renderHealthChecks(w http.ResponseWriter, msg, errMsg, selectedHealthCheckID string) {
	data, _ := h.loadHealthChecksData(context.Background(), selectedHealthCheckID)
	data.CurrentPage = "health-checks"
	data.HealthChecksMsg = msg
	data.HealthChecksErr = errMsg
	h.render(w, "health_checks_section", data)
}

func (h *Handler) render(w http.ResponseWriter, name string, data pageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.tmpl.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, "template rendering failed", http.StatusInternalServerError)
	}
}

func (h *Handler) renderPage(w http.ResponseWriter, r *http.Request, data pageData) {
	if err := h.decorateHeaderHealth(r.Context(), &data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.render(w, "page", data)
}

func (h *Handler) loadCoreData(ctx context.Context) ([]models.Connection, []models.Remote, []models.Backup, error) {
	connections, err := h.repo.ListConnections(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	redactConnections(connections)

	remotes, err := h.repo.ListRemotes(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	redactRemotes(remotes)

	backups, err := h.repo.ListBackups(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	redactBackups(backups)

	return connections, remotes, backups, nil
}

func (h *Handler) loadBackupsData(ctx context.Context) (pageData, error) {
	connections, remotes, backups, err := h.loadCoreData(ctx)
	if err != nil {
		return pageData{}, err
	}
	counts := make(map[string]int, len(backups))
	for _, backup := range backups {
		bindings, bindErr := h.repo.ListBackupNotifications(ctx, backup.ID)
		if bindErr != nil {
			return pageData{}, bindErr
		}
		active := 0
		for _, binding := range bindings {
			if binding.Enabled {
				active++
			}
		}
		counts[backup.ID] = active
	}

	return pageData{
		Connections:              connections,
		Remotes:                  remotes,
		Backups:                  backups,
		BackupNotificationCounts: counts,
	}, nil
}

func (h *Handler) decorateHeaderHealth(ctx context.Context, data *pageData) error {
	checks, err := h.repo.ListHealthChecks(ctx)
	if err != nil {
		return err
	}
	redactHealthChecks(checks)

	down := make([]models.HealthCheck, 0)
	for _, item := range checks {
		if strings.EqualFold(strings.TrimSpace(item.Status), "down") {
			down = append(down, item)
		}
	}

	data.TotalHealthChecks = len(checks)
	data.DownHealthChecks = down
	data.DownHealthCheckCount = len(down)
	if len(checks) == 0 {
		data.HealthSummaryLabel = "No health checks configured"
		return nil
	}
	if len(down) > 0 {
		data.HealthSummaryLabel = fmt.Sprintf("%d database(s) down", len(down))
		return nil
	}
	data.HealthSummaryLabel = "All systems operational"
	return nil
}

func (h *Handler) loadNotificationsData(ctx context.Context, selectedBackupID string) (pageData, error) {
	connections, remotes, backups, err := h.loadCoreData(ctx)
	if err != nil {
		return pageData{}, err
	}

	notifications, err := h.repo.ListNotifications(ctx)
	if err != nil {
		return pageData{}, err
	}
	redactNotifications(notifications)

	counts := make(map[string]int, len(backups))
	for _, backup := range backups {
		bindings, bindErr := h.repo.ListBackupNotifications(ctx, backup.ID)
		if bindErr != nil {
			return pageData{}, bindErr
		}
		active := 0
		for _, binding := range bindings {
			if binding.Enabled {
				active++
			}
		}
		counts[backup.ID] = active
	}

	if strings.TrimSpace(selectedBackupID) == "" && len(backups) > 0 {
		selectedBackupID = backups[0].ID
	}

	bindings := make([]models.BackupNotification, 0)
	if strings.TrimSpace(selectedBackupID) != "" {
		bindings, err = h.repo.ListBackupNotifications(ctx, selectedBackupID)
		if err != nil {
			return pageData{}, err
		}
		redactBackupNotifications(bindings)
	}

	return pageData{
		Connections:              connections,
		Remotes:                  remotes,
		Backups:                  backups,
		Notifications:            notifications,
		BackupBindings:           bindings,
		SelectedBackupID:         selectedBackupID,
		BackupNotificationCounts: counts,
	}, nil
}

func (h *Handler) loadHealthChecksData(ctx context.Context, selectedHealthCheckID string) (pageData, error) {
	connections, remotes, backups, err := h.loadCoreData(ctx)
	if err != nil {
		return pageData{}, err
	}

	healthChecks, err := h.repo.ListHealthChecks(ctx)
	if err != nil {
		return pageData{}, err
	}
	redactHealthChecks(healthChecks)

	notifications, err := h.repo.ListNotifications(ctx)
	if err != nil {
		return pageData{}, err
	}
	redactNotifications(notifications)

	counts := make(map[string]int, len(healthChecks))
	for _, check := range healthChecks {
		bindings, bindErr := h.repo.ListHealthCheckNotifications(ctx, check.ID)
		if bindErr != nil {
			return pageData{}, bindErr
		}
		active := 0
		for _, binding := range bindings {
			if binding.Enabled {
				active++
			}
		}
		counts[check.ID] = active
	}

	if strings.TrimSpace(selectedHealthCheckID) == "" && len(healthChecks) > 0 {
		selectedHealthCheckID = healthChecks[0].ID
	}

	bindings := make([]models.HealthCheckNotification, 0)
	if strings.TrimSpace(selectedHealthCheckID) != "" {
		bindings, err = h.repo.ListHealthCheckNotifications(ctx, selectedHealthCheckID)
		if err != nil {
			return pageData{}, err
		}
		redactHealthCheckNotifications(bindings)
	}

	return pageData{
		Connections:              connections,
		Remotes:                  remotes,
		Backups:                  backups,
		HealthChecks:             healthChecks,
		Notifications:            notifications,
		HealthCheckBindings:      bindings,
		SelectedHealthCheckID:    selectedHealthCheckID,
		HealthNotificationCounts: counts,
	}, nil
}

func (h *Handler) loadDashboardData(ctx context.Context) (pageData, error) {
	connections, remotes, backups, err := h.loadCoreData(ctx)
	if err != nil {
		return pageData{}, err
	}

	stats := dashboardStats{
		TotalConnections: len(connections),
		TotalSchedules:   len(backups),
		NextRunLabel:     "Not scheduled",
	}
	var nextRunAt *time.Time

	for _, item := range backups {
		if item.Enabled {
			stats.EnabledSchedules++
		}
	}

	runsLast30, err := h.repo.ListRunsSince(ctx, time.Now().UTC().AddDate(0, 0, -30))
	if err != nil {
		return pageData{}, err
	}
	stats.RunsLast30Days = len(runsLast30)
	if len(runsLast30) > 0 {
		success := 0
		for _, run := range runsLast30 {
			if run.Status == "success" {
				success++
			}
		}
		stats.SuccessRate = int(float64(success) / float64(len(runsLast30)) * 100)
	}

	recentRuns, err := h.repo.ListRecentRuns(ctx, 200)
	if err != nil {
		return pageData{}, err
	}

	backupByID := make(map[string]models.Backup, len(backups))
	for _, item := range backups {
		backupByID[item.ID] = item
	}

	latestByBackup := make(map[string]models.BackupRun)
	for _, run := range recentRuns {
		if _, seen := latestByBackup[run.BackupID]; seen {
			continue
		}
		latestByBackup[run.BackupID] = run
	}

	failedConnections := make(map[string]struct{})
	for backupID, run := range latestByBackup {
		if run.Status != "failed" {
			continue
		}
		backup := backupByID[backupID]
		if backup.ConnectionID == "" {
			continue
		}
		failedConnections[backup.ConnectionID] = struct{}{}
	}

	stats.FailedConnections = len(failedConnections)
	stats.HealthyConnections = stats.TotalConnections - stats.FailedConnections
	if stats.HealthyConnections < 0 {
		stats.HealthyConnections = 0
	}

	for _, item := range backups {
		if !item.Enabled || item.NextRunAt == nil {
			continue
		}
		next := item.NextRunAt.UTC()
		if nextRunAt == nil || next.Before(*nextRunAt) {
			nextCopy := next
			nextRunAt = &nextCopy
		}
	}
	if nextRunAt != nil {
		stats.NextRunLabel = nextRunAt.Format("2006-01-02 15:04 UTC")
	}

	return pageData{
		Connections: connections,
		Remotes:     remotes,
		Backups:     backups,
		Dashboard:   stats,
	}, nil
}

func parsePort(value string) (int, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(trimmed)
	if err != nil {
		return 0, errors.New("port must be a number")
	}
	if n < 0 || n > 65535 {
		return 0, errors.New("port must be between 0 and 65535")
	}
	return n, nil
}

func parseRetention(value string) (int, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 7, nil
	}
	n, err := strconv.Atoi(trimmed)
	if err != nil {
		return 0, errors.New("retention_days must be a number")
	}
	if n < 0 {
		return 0, errors.New("retention_days must be >= 0")
	}
	return n, nil
}

func parsePositiveInt(value string, fallback int) int {
	n, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func stringPtr(v string) *string {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func intPtr(v int) *int {
	return &v
}

func redactConnections(items []models.Connection) {
	for i := range items {
		items[i].Password = ""
	}
}

func redactRemotes(items []models.Remote) {
	for i := range items {
		items[i].AccessKey = ""
		items[i].SecretKey = ""
	}
}

func redactBackups(items []models.Backup) {
	for i := range items {
		items[i].Connection.Password = ""
		if items[i].Remote != nil {
			items[i].Remote.AccessKey = ""
			items[i].Remote.SecretKey = ""
		}
	}
}

func redactNotifications(items []models.NotificationDestination) {
	for i := range items {
		items[i].DiscordWebhookURL = ""
		items[i].SMTPPassword = ""
	}
}

func redactBackupNotifications(items []models.BackupNotification) {
	for i := range items {
		items[i].Notification.DiscordWebhookURL = ""
		items[i].Notification.SMTPPassword = ""
	}
}

func redactHealthChecks(items []models.HealthCheck) {
	for i := range items {
		items[i].Connection.Password = ""
	}
}

func redactHealthCheckNotifications(items []models.HealthCheckNotification) {
	for i := range items {
		items[i].Notification.DiscordWebhookURL = ""
		items[i].Notification.SMTPPassword = ""
	}
}

func connectionTypeLabel(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "postgres", "postgresql":
		return "PostgreSQL"
	case "mysql":
		return "MySQL"
	case "d1":
		return "Cloudflare D1"
	case "convex":
		return "Convex"
	case "redis":
		return "Redis"
	case "mssql":
		return "MSSQL"
	case "mongo", "mongodb":
		return "MongoDB"
	default:
		return strings.ToUpper(strings.TrimSpace(v))
	}
}

func connectionTypeIconName(v string) string {
	_ = v
	return "database"
}

func ago(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	d := time.Since(value)
	if d < time.Minute {
		return "just now"
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
	return fmt.Sprintf("%dd ago", int(d.Hours()/24))
}

func runDuration(start time.Time, finish *time.Time) string {
	if finish == nil {
		return "running"
	}
	d := finish.Sub(start)
	if d < time.Second {
		return "<1s"
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60
	if minutes < 60 {
		return fmt.Sprintf("%dm %02ds", minutes, seconds)
	}
	hours := minutes / 60
	minutes = minutes % 60
	return fmt.Sprintf("%dh %02dm", hours, minutes)
}

func buildRunLog(item runListItem) []runLogLine {
	lines := []runLogLine{
		{Time: item.StartedAt.UTC().Format("15:04:05.000"), Level: "info", Message: "[anchor] Starting run for " + item.BackupName},
		{Time: item.StartedAt.UTC().Add(150 * time.Millisecond).Format("15:04:05.000"), Level: "info", Message: "[anchor] Preparing backup pipeline"},
	}

	if item.OutputKey != "" {
		lines = append(lines, runLogLine{
			Time:    item.StartedAt.UTC().Add(450 * time.Millisecond).Format("15:04:05.000"),
			Level:   "ok",
			Message: "[anchor] Backup artifact stored at " + item.OutputKey,
		})
	}

	if strings.TrimSpace(item.ErrorText) != "" {
		lines = append(lines, runLogLine{
			Time:    item.StartedAt.UTC().Add(700 * time.Millisecond).Format("15:04:05.000"),
			Level:   "err",
			Message: "[anchor] " + strings.TrimSpace(item.ErrorText),
		})
	}

	if item.Status == "success" {
		ended := item.StartedAt
		if item.FinishedAt != nil {
			ended = item.FinishedAt.UTC()
		}
		lines = append(lines, runLogLine{
			Time:    ended.Format("15:04:05.000"),
			Level:   "ok",
			Message: "[anchor] Run completed successfully in " + runDuration(item.StartedAt, item.FinishedAt),
		})
	} else if item.Status == "failed" {
		ended := item.StartedAt.Add(1 * time.Second)
		if item.FinishedAt != nil {
			ended = item.FinishedAt.UTC()
		}
		lines = append(lines, runLogLine{
			Time:    ended.Format("15:04:05.000"),
			Level:   "err",
			Message: "[anchor] Run failed",
		})
	} else {
		lines = append(lines, runLogLine{
			Time:    time.Now().UTC().Format("15:04:05.000"),
			Level:   "info",
			Message: "[anchor] Run still in progress",
		})
	}

	return lines
}
