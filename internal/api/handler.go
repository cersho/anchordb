package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"anchordb/internal/config"
	"anchordb/internal/health"
	"anchordb/internal/models"
	"anchordb/internal/notifications"
	"anchordb/internal/repository"
	"anchordb/internal/scheduler"

	"github.com/go-chi/chi/v5"
	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
)

type Handler struct {
	repo      *repository.Repository
	scheduler *scheduler.Scheduler
	notifier  *notifications.Dispatcher
	cfg       config.Config
}

func NewHandler(repo *repository.Repository, scheduler *scheduler.Scheduler, cfg config.Config) *Handler {
	return &Handler{repo: repo, scheduler: scheduler, notifier: notifications.NewDispatcher(repo), cfg: cfg}
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

	r.Route("/notifications", func(r chi.Router) {
		r.Post("/", h.createNotification)
		r.Get("/", h.listNotifications)
		r.Get("/{id}", h.getNotification)
		r.Patch("/{id}", h.updateNotification)
		r.Post("/{id}/test", h.testNotification)
		r.Delete("/{id}", h.deleteNotification)
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
		r.Get("/{id}/notifications", h.listBackupNotifications)
		r.Put("/{id}/notifications", h.setBackupNotifications)
	})

	r.Route("/health-checks", func(r chi.Router) {
		r.Get("/", h.listHealthChecks)
		r.Get("/{id}", h.getHealthCheck)
		r.Patch("/{id}", h.updateHealthCheck)
		r.Post("/{id}/run", h.runHealthCheckNow)
		r.Get("/{id}/notifications", h.listHealthCheckNotifications)
		r.Put("/{id}/notifications", h.setHealthCheckNotifications)
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
	if req.Type == "d1" {
		if strings.TrimSpace(req.Username) == "" {
			req.Username = "d1"
		}
		req.Port = 0
		req.SSLMode = ""
	}

	if err := h.repo.CreateConnection(r.Context(), &req); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := h.repo.UpsertHealthCheckForConnection(r.Context(), req.ID, repository.HealthCheckDefaults{
		IntervalSecond:   int(h.cfg.DefaultHealthEvery / time.Second),
		TimeoutSecond:    int(h.cfg.DefaultHealthTimeout / time.Second),
		FailureThreshold: h.cfg.DefaultFailThreshold,
		SuccessThreshold: h.cfg.DefaultPassThreshold,
	}); err != nil {
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
			writeError(w, http.StatusBadRequest, "type must be mysql, postgres, convex, or d1")
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

func (h *Handler) createNotification(w http.ResponseWriter, r *http.Request) {
	type createRequest struct {
		Name              string `json:"name"`
		Type              string `json:"type"`
		Enabled           *bool  `json:"enabled"`
		DiscordWebhookURL string `json:"discord_webhook_url"`
		SMTPHost          string `json:"smtp_host"`
		SMTPPort          int    `json:"smtp_port"`
		SMTPUsername      string `json:"smtp_username"`
		SMTPPassword      string `json:"smtp_password"`
		SMTPFrom          string `json:"smtp_from"`
		SMTPTo            string `json:"smtp_to"`
		SMTPSecurity      string `json:"smtp_security"`
	}

	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	item := models.NotificationDestination{
		Name:              strings.TrimSpace(req.Name),
		Type:              strings.ToLower(strings.TrimSpace(req.Type)),
		Enabled:           enabled,
		DiscordWebhookURL: strings.TrimSpace(req.DiscordWebhookURL),
		SMTPHost:          strings.TrimSpace(req.SMTPHost),
		SMTPPort:          req.SMTPPort,
		SMTPUsername:      strings.TrimSpace(req.SMTPUsername),
		SMTPPassword:      req.SMTPPassword,
		SMTPFrom:          strings.TrimSpace(req.SMTPFrom),
		SMTPTo:            strings.TrimSpace(req.SMTPTo),
		SMTPSecurity:      strings.ToLower(strings.TrimSpace(req.SMTPSecurity)),
	}

	if err := h.repo.CreateNotification(r.Context(), &item); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	redactNotification(&item)
	writeJSON(w, http.StatusCreated, item)
}

func (h *Handler) listNotifications(w http.ResponseWriter, r *http.Request) {
	items, err := h.repo.ListNotifications(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for i := range items {
		redactNotification(&items[i])
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) getNotification(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	item, err := h.repo.GetNotification(r.Context(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "notification not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	redactNotification(&item)
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) updateNotification(w http.ResponseWriter, r *http.Request) {
	type patchRequest struct {
		Name              *string `json:"name"`
		Type              *string `json:"type"`
		Enabled           *bool   `json:"enabled"`
		DiscordWebhookURL *string `json:"discord_webhook_url"`
		SMTPHost          *string `json:"smtp_host"`
		SMTPPort          *int    `json:"smtp_port"`
		SMTPUsername      *string `json:"smtp_username"`
		SMTPPassword      *string `json:"smtp_password"`
		SMTPFrom          *string `json:"smtp_from"`
		SMTPTo            *string `json:"smtp_to"`
		SMTPSecurity      *string `json:"smtp_security"`
	}

	id := chi.URLParam(r, "id")
	var req patchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if req.Type != nil {
		trimmed := strings.ToLower(strings.TrimSpace(*req.Type))
		req.Type = &trimmed
	}
	if req.SMTPSecurity != nil {
		trimmed := strings.ToLower(strings.TrimSpace(*req.SMTPSecurity))
		req.SMTPSecurity = &trimmed
	}

	item, err := h.repo.UpdateNotification(r.Context(), id, repository.NotificationPatch{
		Name:              req.Name,
		Type:              req.Type,
		Enabled:           req.Enabled,
		DiscordWebhookURL: req.DiscordWebhookURL,
		SMTPHost:          req.SMTPHost,
		SMTPPort:          req.SMTPPort,
		SMTPUsername:      req.SMTPUsername,
		SMTPPassword:      req.SMTPPassword,
		SMTPFrom:          req.SMTPFrom,
		SMTPTo:            req.SMTPTo,
		SMTPSecurity:      req.SMTPSecurity,
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "notification not found")
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	redactNotification(&item)
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) testNotification(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	item, err := h.repo.GetNotification(r.Context(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "notification not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if !item.Enabled {
		writeError(w, http.StatusBadRequest, "notification is disabled")
		return
	}
	if err := h.notifier.SendTestNotification(r.Context(), item); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "sent"})
}

func (h *Handler) deleteNotification(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := h.repo.DeleteNotification(r.Context(), id); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "notification not found")
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

func (h *Handler) listBackupNotifications(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if _, err := h.repo.GetBackup(r.Context(), id); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "backup not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	items, err := h.repo.ListBackupNotifications(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for i := range items {
		redactNotification(&items[i].Notification)
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) setBackupNotifications(w http.ResponseWriter, r *http.Request) {
	type notificationBindingRequest struct {
		NotificationID string `json:"notification_id"`
		OnSuccess      *bool  `json:"on_success"`
		OnFailure      *bool  `json:"on_failure"`
		Enabled        *bool  `json:"enabled"`
	}
	type bindingsRequest struct {
		Notifications []notificationBindingRequest `json:"notifications"`
	}

	id := chi.URLParam(r, "id")
	if _, err := h.repo.GetBackup(r.Context(), id); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "backup not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var req bindingsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	bindings := make([]models.BackupNotification, 0, len(req.Notifications))
	for _, item := range req.Notifications {
		onSuccess := true
		if item.OnSuccess != nil {
			onSuccess = *item.OnSuccess
		}
		onFailure := true
		if item.OnFailure != nil {
			onFailure = *item.OnFailure
		}
		enabled := true
		if item.Enabled != nil {
			enabled = *item.Enabled
		}

		bindings = append(bindings, models.BackupNotification{
			NotificationID: strings.TrimSpace(item.NotificationID),
			OnSuccess:      onSuccess,
			OnFailure:      onFailure,
			Enabled:        enabled,
		})
	}

	items, err := h.repo.SetBackupNotifications(r.Context(), id, bindings)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "backup not found")
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	for i := range items {
		redactNotification(&items[i].Notification)
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) listHealthChecks(w http.ResponseWriter, r *http.Request) {
	items, err := h.repo.ListHealthChecks(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for i := range items {
		redactConnection(&items[i].Connection)
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) getHealthCheck(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	item, err := h.repo.GetHealthCheck(r.Context(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "health check not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	redactConnection(&item.Connection)
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) updateHealthCheck(w http.ResponseWriter, r *http.Request) {
	type patchRequest struct {
		Enabled             *bool `json:"enabled"`
		CheckIntervalSecond *int  `json:"check_interval_second"`
		TimeoutSecond       *int  `json:"timeout_second"`
		FailureThreshold    *int  `json:"failure_threshold"`
		SuccessThreshold    *int  `json:"success_threshold"`
	}

	id := chi.URLParam(r, "id")
	var req patchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	item, err := h.repo.UpdateHealthCheck(r.Context(), id, repository.HealthCheckPatch{
		Enabled:             req.Enabled,
		CheckIntervalSecond: req.CheckIntervalSecond,
		TimeoutSecond:       req.TimeoutSecond,
		FailureThreshold:    req.FailureThreshold,
		SuccessThreshold:    req.SuccessThreshold,
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "health check not found")
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	redactConnection(&item.Connection)
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) runHealthCheckNow(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	item, err := h.repo.GetHealthCheck(r.Context(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "health check not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if item.LastCheckedAt != nil && h.cfg.HealthManualCooldown > 0 {
		nextAllowedAt := item.LastCheckedAt.UTC().Add(h.cfg.HealthManualCooldown)
		if time.Now().UTC().Before(nextAllowedAt) {
			retryAfter := int(time.Until(nextAllowedAt).Seconds())
			if retryAfter < 1 {
				retryAfter = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			writeJSON(w, http.StatusTooManyRequests, map[string]any{
				"status":              "cooldown",
				"error":               "manual health check is on cooldown",
				"retry_after_seconds": retryAfter,
			})
			return
		}
	}

	timeoutSecond := item.TimeoutSecond
	if timeoutSecond <= 0 {
		timeoutSecond = int(h.cfg.DefaultHealthTimeout / time.Second)
	}
	if timeoutSecond <= 0 {
		timeoutSecond = 5
	}
	timeout := time.Duration(timeoutSecond) * time.Second

	result, probeErr := health.ProbeConnection(r.Context(), item.Connection, h.cfg, timeout)
	healthy := probeErr == nil
	errText := ""
	if probeErr != nil {
		errText = strings.TrimSpace(probeErr.Error())
	}

	updated, event, saveErr := h.repo.SaveHealthCheckProbeResult(r.Context(), item.ID, time.Now().UTC(), healthy, errText)
	if saveErr != nil {
		writeError(w, http.StatusInternalServerError, saveErr.Error())
		return
	}
	if event != "" {
		if notifyErr := h.notifier.NotifyHealthCheckEvent(r.Context(), updated, event); notifyErr != nil {
			writeError(w, http.StatusBadGateway, "health check updated but notification failed: "+notifyErr.Error())
			return
		}
		_ = h.repo.MarkHealthCheckNotified(r.Context(), updated.ID, time.Now().UTC())
	}

	redactConnection(&updated.Connection)
	if probeErr != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"status": "failed", "error": probeErr.Error(), "health_check": updated})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "result": result, "health_check": updated})
}

func (h *Handler) listHealthCheckNotifications(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if _, err := h.repo.GetHealthCheck(r.Context(), id); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "health check not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	items, err := h.repo.ListHealthCheckNotifications(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for i := range items {
		redactNotification(&items[i].Notification)
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) setHealthCheckNotifications(w http.ResponseWriter, r *http.Request) {
	type notificationBindingRequest struct {
		NotificationID string `json:"notification_id"`
		OnDown         *bool  `json:"on_down"`
		OnRecovered    *bool  `json:"on_recovered"`
		Enabled        *bool  `json:"enabled"`
	}
	type bindingsRequest struct {
		Notifications []notificationBindingRequest `json:"notifications"`
	}

	id := chi.URLParam(r, "id")
	if _, err := h.repo.GetHealthCheck(r.Context(), id); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "health check not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var req bindingsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	bindings := make([]models.HealthCheckNotification, 0, len(req.Notifications))
	for _, item := range req.Notifications {
		onDown := true
		if item.OnDown != nil {
			onDown = *item.OnDown
		}
		onRecovered := true
		if item.OnRecovered != nil {
			onRecovered = *item.OnRecovered
		}
		enabled := true
		if item.Enabled != nil {
			enabled = *item.Enabled
		}

		bindings = append(bindings, models.HealthCheckNotification{
			NotificationID: strings.TrimSpace(item.NotificationID),
			OnDown:         onDown,
			OnRecovered:    onRecovered,
			Enabled:        enabled,
		})
	}

	items, err := h.repo.SetHealthCheckNotifications(r.Context(), id, bindings)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "health check not found")
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	for i := range items {
		redactNotification(&items[i].Notification)
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
		return errors.New("type must be mysql, postgres, convex, or d1")
	}
	if c.Type == "d1" && c.Database == "" {
		return errors.New("database is required for d1")
	}
	if c.Type != "convex" && c.Type != "d1" && (c.Database == "" || c.Username == "") {
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
	return t == "postgres" || t == "postgresql" || t == "mysql" || t == "convex" || t == "d1"
}

func defaultPort(t string) int {
	if t == "postgres" || t == "postgresql" {
		return 5432
	}
	if t == "convex" {
		return 0
	}
	if t == "d1" {
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

func redactNotification(n *models.NotificationDestination) {
	n.DiscordWebhookURL = ""
	n.SMTPPassword = ""
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
