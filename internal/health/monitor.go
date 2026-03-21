package health

import (
	"context"
	"log"
	"strings"
	"time"

	"anchordb/internal/config"
	"anchordb/internal/models"
	"anchordb/internal/notifications"
	"anchordb/internal/repository"
)

type Monitor struct {
	cfg      config.Config
	repo     *repository.Repository
	notifier *notifications.Dispatcher
	sem      chan struct{}
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func NewMonitor(cfg config.Config, repo *repository.Repository) *Monitor {
	concurrency := cfg.MaxConcurrentHealth
	if concurrency <= 0 {
		concurrency = 2
	}
	return &Monitor{
		cfg:      cfg,
		repo:     repo,
		notifier: notifications.NewDispatcher(repo),
		sem:      make(chan struct{}, concurrency),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

func (m *Monitor) Start(ctx context.Context) {
	if err := m.repo.EnsureHealthChecksForConnections(ctx, repository.HealthCheckDefaults{
		IntervalSecond:   int(m.cfg.DefaultHealthEvery / time.Second),
		TimeoutSecond:    int(m.cfg.DefaultHealthTimeout / time.Second),
		FailureThreshold: m.cfg.DefaultFailThreshold,
		SuccessThreshold: m.cfg.DefaultPassThreshold,
	}); err != nil {
		log.Printf("health monitor: ensure default checks failed: %v", err)
	}

	interval := m.cfg.HealthPollInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)

	go func() {
		defer close(m.doneCh)
		defer ticker.Stop()
		m.processDue(context.Background())
		for {
			select {
			case <-ticker.C:
				m.processDue(context.Background())
			case <-m.stopCh:
				return
			}
		}
	}()
}

func (m *Monitor) Stop() {
	close(m.stopCh)
	<-m.doneCh
}

func (m *Monitor) processDue(ctx context.Context) {
	batchSize := m.cfg.HealthDueBatchSize
	if batchSize <= 0 {
		batchSize = 50
	}

	now := time.Now().UTC()
	items, err := m.repo.ListDueHealthChecks(ctx, now, batchSize)
	if err != nil {
		log.Printf("health monitor: list due checks failed: %v", err)
		return
	}

	for _, item := range items {
		itemCopy := item
		timeoutSec := itemCopy.TimeoutSecond
		if timeoutSec <= 0 {
			timeoutSec = int(m.cfg.DefaultHealthTimeout / time.Second)
		}
		if timeoutSec <= 0 {
			timeoutSec = 5
		}
		lease := time.Duration(timeoutSec+2) * time.Second
		claimed, claimErr := m.repo.ClaimHealthCheckRun(ctx, itemCopy.ID, time.Now().UTC(), lease)
		if claimErr != nil {
			log.Printf("health monitor: claim check failed for %s: %v", itemCopy.ID, claimErr)
			continue
		}
		if !claimed {
			continue
		}
		m.sem <- struct{}{}
		go func() {
			defer func() { <-m.sem }()
			m.runCheck(context.Background(), itemCopy)
		}()
	}
}

func (m *Monitor) runCheck(ctx context.Context, checkItem models.HealthCheck) {
	timeoutSec := checkItem.TimeoutSecond
	if timeoutSec <= 0 {
		timeoutSec = int(m.cfg.DefaultHealthTimeout / time.Second)
	}
	if timeoutSec <= 0 {
		timeoutSec = 5
	}
	timeout := time.Duration(timeoutSec) * time.Second

	_, probeErr := ProbeConnection(ctx, checkItem.Connection, m.cfg, timeout)
	healthy := probeErr == nil
	errText := ""
	if probeErr != nil {
		errText = strings.TrimSpace(probeErr.Error())
	}

	updated, event, err := m.repo.SaveHealthCheckProbeResult(ctx, checkItem.ID, time.Now().UTC(), healthy, errText)
	if err != nil {
		log.Printf("health monitor: save probe result failed for %s: %v", checkItem.ID, err)
		return
	}
	if event == "" {
		return
	}
	if notifyErr := m.notifier.NotifyHealthCheckEvent(ctx, updated, event); notifyErr != nil {
		log.Printf("health monitor: notify failed for %s (%s): %v", updated.ID, event, notifyErr)
		return
	}
	_ = m.repo.MarkHealthCheckNotified(ctx, updated.ID, time.Now().UTC())
}
