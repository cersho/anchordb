package ui

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"anchordb/internal/config"
	"anchordb/internal/models"
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
	cfg       config.Config
	tmpl      *template.Template
}

type pageData struct {
	Connections      []models.Connection
	Remotes          []models.Remote
	Backups          []models.Backup
	Runs             []models.BackupRun
	RunItems         []runListItem
	RunLog           []runLogLine
	SelectedRunID    string
	SelectedRun      runListItem
	HasSelectedRun   bool
	SelectedBackupID string
	CurrentPage      string
	Dashboard        dashboardStats

	ConnectionsMsg string
	ConnectionsErr string
	RemotesMsg     string
	RemotesErr     string
	BackupsMsg     string
	BackupsErr     string
	RunsErr        string
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
	}).ParseFS(templateFS, "templates/*.html"))

	return &Handler{repo: repo, scheduler: scheduler, cfg: cfg, tmpl: tmpl}
}

func (h *Handler) Router() http.Handler {
	r := chi.NewRouter()

	r.Get("/", h.home)
	r.Get("/dashboard", h.dashboardPage)
	r.Get("/connections", h.connectionsPage)
	r.Get("/connections/section", h.connectionsSection)
	r.Post("/connections", h.createConnection)
	r.Post("/connections/test", h.testConnection)
	r.Post("/connections/test-all", h.testAllConnections)
	r.Post("/connections/{id}/test", h.testSavedConnection)
	r.Get("/remotes", h.remotesPage)
	r.Get("/remotes/section", h.remotesSection)
	r.Post("/remotes", h.createRemote)
	r.Get("/backups", h.backupsPage)
	r.Get("/schedules", h.backupsPage)
	r.Get("/backups/section", h.backupsSection)
	r.Get("/schedules/section", h.backupsSection)
	r.Post("/backups", h.createBackup)
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
	h.render(w, "page", data)
}

func (h *Handler) connectionsPage(w http.ResponseWriter, r *http.Request) {
	items, err := h.repo.ListConnections(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redactConnections(items)
	h.render(w, "page", pageData{CurrentPage: "connections", Connections: items})
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
	h.render(w, "page", pageData{CurrentPage: "remotes", Remotes: items})
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
	h.render(w, "page", data)
}

func (h *Handler) backupRuns(w http.ResponseWriter, r *http.Request) {
	backupID := chi.URLParam(r, "id")
	http.Redirect(w, r, "/app/runs?backup_id="+backupID, http.StatusFound)
}

func (h *Handler) runsPage(w http.ResponseWriter, r *http.Request) {
	data, err := h.loadRunsData(r.Context(), strings.TrimSpace(r.URL.Query().Get("run_id")))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.CurrentPage = "runs"
	h.render(w, "page", data)
}

func (h *Handler) runsSection(w http.ResponseWriter, r *http.Request) {
	data, err := h.loadRunsData(r.Context(), strings.TrimSpace(r.URL.Query().Get("run_id")))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.render(w, "runs_section", data)
}

func (h *Handler) loadRunsData(ctx context.Context, selectedRunID string) (pageData, error) {
	backups, err := h.repo.ListBackups(ctx)
	if err != nil {
		return pageData{}, err
	}
	redactBackups(backups)

	backupByID := make(map[string]models.Backup, len(backups))
	for _, item := range backups {
		backupByID[item.ID] = item
	}

	runs, err := h.repo.ListRecentRuns(ctx, 60)
	if err != nil {
		return pageData{}, err
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

	data := pageData{Backups: backups, RunItems: runItems}
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
	applyConnectionDefaults(&item)

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

func applyConnectionDefaults(item *models.Connection) {
	if item.Port != 0 {
		return
	}

	switch strings.ToLower(strings.TrimSpace(item.Type)) {
	case "postgres", "postgresql":
		item.Port = 5432
	case "mysql":
		item.Port = 3306
	case "mssql":
		item.Port = 1433
	case "redis":
		item.Port = 6379
	case "mongo", "mongodb":
		item.Port = 27017
	}
}

func (h *Handler) probeConnection(ctx context.Context, item models.Connection) (string, error) {
	typeName := strings.ToLower(strings.TrimSpace(item.Type))
	host := strings.TrimSpace(item.Host)
	if host == "" {
		return "", errors.New("host is required")
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	switch typeName {
	case "postgres", "postgresql", "mysql", "mssql", "redis", "mongo", "mongodb":
		applyConnectionDefaults(&item)
		if item.Port <= 0 {
			return "", errors.New("port is required")
		}
		addr := host
		if _, _, err := net.SplitHostPort(host); err != nil {
			addr = net.JoinHostPort(host, strconv.Itoa(item.Port))
		}
		dialer := net.Dialer{Timeout: 4 * time.Second}
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
		dialer := net.Dialer{Timeout: 4 * time.Second}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return "", err
		}
		_ = conn.Close()
		return "Convex endpoint reachable (" + addr + ")", nil
	case "d1":
		accountID := strings.TrimSpace(item.Host)
		if accountID == "" {
			accountID = strings.TrimSpace(h.cfg.CloudflareAccountID)
		}
		databaseID := strings.TrimSpace(item.Database)
		if databaseID == "" {
			databaseID = strings.TrimSpace(h.cfg.CloudflareDatabaseID)
		}
		apiKey := strings.TrimSpace(item.Password)
		if apiKey == "" {
			apiKey = strings.TrimSpace(h.cfg.CloudflareAPIKey)
		}
		if accountID == "" || databaseID == "" || apiKey == "" {
			return "", errors.New("d1 test requires account id, database id, and api key")
		}
		if err := h.testD1Query(ctx, accountID, databaseID, apiKey); err != nil {
			return "", err
		}
		return "Cloudflare D1 API reachable", nil
	default:
		return "", fmt.Errorf("unsupported connection type: %s", item.Type)
	}
}

func (h *Handler) testD1Query(ctx context.Context, accountID, databaseID, apiKey string) error {
	endpoint := strings.TrimSuffix(strings.TrimSpace(h.cfg.D1APIBaseURL), "/")
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

func (h *Handler) render(w http.ResponseWriter, name string, data pageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.tmpl.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, "template rendering failed", http.StatusInternalServerError)
	}
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
	return pageData{Connections: connections, Remotes: remotes, Backups: backups}, nil
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
