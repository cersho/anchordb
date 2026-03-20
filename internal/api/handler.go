package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"anchordb/internal/models"
	"anchordb/internal/repository"
	"anchordb/internal/scheduler"

	"github.com/go-chi/chi/v5"
	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
)

type Handler struct {
	repo      *repository.Repository
	scheduler *scheduler.Scheduler
}

func NewHandler(repo *repository.Repository, scheduler *scheduler.Scheduler) *Handler {
	return &Handler{repo: repo, scheduler: scheduler}
}

func (h *Handler) Router() http.Handler {
	r := chi.NewRouter()

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)})
	})

	r.Route("/connections", func(r chi.Router) {
		r.Post("/", h.createConnection)
		r.Get("/", h.listConnections)
		r.Get("/{id}", h.getConnection)
		r.Patch("/{id}", h.updateConnection)
		r.Delete("/{id}", h.deleteConnection)
	})

	r.Route("/remotes", func(r chi.Router) {
		r.Post("/", h.createRemote)
		r.Get("/", h.listRemotes)
		r.Get("/{id}", h.getRemote)
		r.Patch("/{id}", h.updateRemote)
		r.Delete("/{id}", h.deleteRemote)
	})

	r.Route("/backups", func(r chi.Router) {
		r.Post("/", h.createBackup)
		r.Get("/", h.listBackups)
		r.Get("/{id}", h.getBackup)
		r.Patch("/{id}", h.updateBackup)
		r.Delete("/{id}", h.deleteBackup)
		r.Post("/{id}/run", h.runBackupNow)
		r.Patch("/{id}/enabled", h.setBackupEnabled)
		r.Get("/{id}/runs", h.listBackupRuns)
	})

	return r
}

func (h *Handler) createConnection(w http.ResponseWriter, r *http.Request) {
	var req models.Connection
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if err := validateConnectionCreate(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.Type = strings.ToLower(req.Type)
	if req.Port == 0 {
		req.Port = defaultPort(req.Type)
	}
	if req.Type == "convex" {
		if strings.TrimSpace(req.Database) == "" {
			req.Database = "convex"
		}
		if strings.TrimSpace(req.Username) == "" {
			req.Username = "convex"
		}
		req.SSLMode = ""
	}

	if err := h.repo.CreateConnection(r.Context(), &req); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	redactConnection(&req)
	writeJSON(w, http.StatusCreated, req)
}

func (h *Handler) getConnection(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	item, err := h.repo.GetConnection(r.Context(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "connection not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	redactConnection(&item)
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) listConnections(w http.ResponseWriter, r *http.Request) {
	items, err := h.repo.ListConnections(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for i := range items {
		redactConnection(&items[i])
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) updateConnection(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req models.Connection
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	if req.Type != "" {
		req.Type = strings.ToLower(req.Type)
		if !validConnectionType(req.Type) {
			writeError(w, http.StatusBadRequest, "type must be mysql, postgres, or convex")
			return
		}
	}

	item, err := h.repo.UpdateConnection(r.Context(), id, &req)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "connection not found")
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	redactConnection(&item)
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) deleteConnection(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := h.repo.DeleteConnection(r.Context(), id); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "connection not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (h *Handler) createRemote(w http.ResponseWriter, r *http.Request) {
	var req models.Remote
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if err := validateRemoteCreate(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := h.repo.CreateRemote(r.Context(), &req); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	redactRemote(&req)
	writeJSON(w, http.StatusCreated, req)
}

func (h *Handler) getRemote(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	item, err := h.repo.GetRemote(r.Context(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "remote not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	redactRemote(&item)
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) listRemotes(w http.ResponseWriter, r *http.Request) {
	items, err := h.repo.ListRemotes(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for i := range items {
		redactRemote(&items[i])
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) updateRemote(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req models.Remote
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if req.Provider != "" && strings.ToLower(req.Provider) != "s3" {
		writeError(w, http.StatusBadRequest, "provider must be s3")
		return
	}
	req.Provider = strings.ToLower(req.Provider)

	item, err := h.repo.UpdateRemote(r.Context(), id, &req)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "remote not found")
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	redactRemote(&item)
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) deleteRemote(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := h.repo.DeleteRemote(r.Context(), id); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "remote not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (h *Handler) createBackup(w http.ResponseWriter, r *http.Request) {
	var req models.Backup
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	if err := h.validateBackupRequest(r, req, true); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	connType, err := h.resolveBackupConnectionType(r.Context(), req, "")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if connType == "convex" {
		req.Compression = "none"
	}

	if err := h.repo.CreateBackup(r.Context(), &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := h.scheduler.Upsert(r.Context(), req.ID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	fresh, _ := h.repo.GetBackup(r.Context(), req.ID)
	redactBackupSecrets(&fresh)
	writeJSON(w, http.StatusCreated, fresh)
}

func (h *Handler) listBackups(w http.ResponseWriter, r *http.Request) {
	items, err := h.repo.ListBackups(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for i := range items {
		redactBackupSecrets(&items[i])
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) getBackup(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	item, err := h.repo.GetBackup(r.Context(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "backup not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	redactBackupSecrets(&item)
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) updateBackup(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req models.Backup
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	if err := h.validateBackupRequest(r, req, false); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	connType, err := h.resolveBackupConnectionType(r.Context(), req, id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "backup not found")
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if connType == "convex" {
		req.Compression = "none"
	}

	item, err := h.repo.UpdateBackup(r.Context(), id, &req)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "backup not found")
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if item.Enabled {
		if err := h.scheduler.Upsert(r.Context(), item.ID); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	} else {
		h.scheduler.Delete(item.ID)
	}

	redactBackupSecrets(&item)
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) deleteBackup(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	h.scheduler.Delete(id)
	if err := h.repo.DeleteBackup(r.Context(), id); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "backup not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (h *Handler) runBackupNow(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if _, err := h.repo.GetBackup(r.Context(), id); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "backup not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.scheduler.TriggerNow(id)
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "triggered"})
}

func (h *Handler) setBackupEnabled(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var body struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	if err := h.repo.SetBackupEnabled(r.Context(), id, body.Enabled); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "backup not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if body.Enabled {
		if err := h.scheduler.Upsert(r.Context(), id); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	} else {
		h.scheduler.Delete(id)
	}

	item, err := h.repo.GetBackup(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	redactBackupSecrets(&item)
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) listBackupRuns(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if _, err := h.repo.GetBackup(r.Context(), id); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "backup not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	limit := 50
	if q := r.URL.Query().Get("limit"); q != "" {
		n, err := strconv.Atoi(q)
		if err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}

	items, err := h.repo.ListBackupRuns(r.Context(), id, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) validateBackupRequest(r *http.Request, b models.Backup, create bool) error {
	if create && (b.Name == "" || b.ConnectionID == "" || b.CronExpr == "" || b.TargetType == "") {
		return errors.New("name, connection_id, cron_expr, target_type are required")
	}
	if b.ConnectionID != "" {
		if _, err := h.repo.GetConnection(r.Context(), b.ConnectionID); err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("connection_id not found")
			}
			return err
		}
	}
	if b.CronExpr != "" && !validCronExpr(b.CronExpr) {
		return errors.New("invalid cron_expr")
	}
	if b.Timezone != "" {
		if _, err := time.LoadLocation(b.Timezone); err != nil {
			return errors.New("invalid timezone")
		}
	}
	if b.TargetType != "" && b.TargetType != "local" && b.TargetType != "s3" {
		return errors.New("target_type must be local or s3")
	}
	if b.Compression != "" && b.Compression != "gzip" && b.Compression != "none" {
		return errors.New("compression must be gzip or none")
	}
	if b.RetentionDays < 0 {
		return errors.New("retention_days must be >= 0")
	}
	if b.TargetType == "local" && b.LocalPath == "" && create {
		return errors.New("local_path is required for local target")
	}
	if b.TargetType == "s3" {
		if b.RemoteID == nil {
			return errors.New("remote_id is required for s3 target")
		}
		if _, err := h.repo.GetRemote(r.Context(), *b.RemoteID); err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("remote_id not found")
			}
			return err
		}
	}
	return nil
}

func validateConnectionCreate(c models.Connection) error {
	if c.Name == "" || c.Type == "" || c.Host == "" || c.Password == "" {
		return errors.New("name, type, host, and password are required")
	}
	c.Type = strings.ToLower(c.Type)
	if !validConnectionType(c.Type) {
		return errors.New("type must be mysql, postgres, or convex")
	}
	if c.Type != "convex" && (c.Database == "" || c.Username == "") {
		return errors.New("database and username are required for mysql/postgres")
	}
	if c.Port < 0 || c.Port > 65535 {
		return errors.New("port must be between 0 and 65535")
	}
	return nil
}

func validateRemoteCreate(rem models.Remote) error {
	if rem.Name == "" || rem.Bucket == "" || rem.Region == "" || rem.AccessKey == "" || rem.SecretKey == "" {
		return errors.New("name, bucket, region, access_key, secret_key are required")
	}
	if rem.Provider != "" && strings.ToLower(rem.Provider) != "s3" {
		return errors.New("provider must be s3")
	}
	return nil
}

func validConnectionType(t string) bool {
	return t == "postgres" || t == "postgresql" || t == "mysql" || t == "convex"
}

func defaultPort(t string) int {
	if t == "postgres" || t == "postgresql" {
		return 5432
	}
	if t == "convex" {
		return 0
	}
	return 3306
}

func (h *Handler) resolveBackupConnectionType(ctx context.Context, b models.Backup, backupID string) (string, error) {
	if strings.TrimSpace(b.ConnectionID) != "" {
		conn, err := h.repo.GetConnection(ctx, strings.TrimSpace(b.ConnectionID))
		if err != nil {
			return "", err
		}
		return strings.ToLower(strings.TrimSpace(conn.Type)), nil
	}
	if strings.TrimSpace(backupID) == "" {
		return "", nil
	}
	item, err := h.repo.GetBackup(ctx, strings.TrimSpace(backupID))
	if err != nil {
		return "", err
	}
	return strings.ToLower(strings.TrimSpace(item.Connection.Type)), nil
}

func validCronExpr(expr string) bool {
	_, err := cron.ParseStandard(expr)
	return err == nil
}

func redactConnection(c *models.Connection) {
	c.Password = ""
}

func redactRemote(r *models.Remote) {
	r.AccessKey = ""
	r.SecretKey = ""
}

func redactBackupSecrets(b *models.Backup) {
	redactConnection(&b.Connection)
	if b.Remote != nil {
		redactRemote(b.Remote)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
