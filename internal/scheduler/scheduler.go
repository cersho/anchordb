package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"anchordb/internal/config"
	"anchordb/internal/repository"

	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	repo     *repository.Repository
	executor *Executor
	cron     *cron.Cron
	sem      chan struct{}

	mu      sync.Mutex
	entries map[string]cron.EntryID
}

func New(repo *repository.Repository, executor *Executor, cfg config.Config) *Scheduler {
	return &Scheduler{
		repo:     repo,
		executor: executor,
		cron:     cron.New(),
		sem:      make(chan struct{}, cfg.MaxConcurrentBackups),
		entries:  make(map[string]cron.EntryID),
	}
}

func (s *Scheduler) Start() {
	s.cron.Start()
}

func (s *Scheduler) Stop() {
	ctx := s.cron.Stop()
	<-ctx.Done()
}

func (s *Scheduler) LoadAll(ctx context.Context) error {
	backups, err := s.repo.ListEnabledBackups(ctx)
	if err != nil {
		return err
	}
	for _, b := range backups {
		if err := s.Upsert(ctx, b.ID); err != nil {
			log.Printf("schedule backup %s failed: %v", b.ID, err)
		}
	}
	return nil
}

func (s *Scheduler) Upsert(ctx context.Context, backupID string) error {
	b, err := s.repo.GetBackup(ctx, backupID)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if old, ok := s.entries[backupID]; ok {
		s.cron.Remove(old)
		delete(s.entries, backupID)
	}
	s.mu.Unlock()

	if !b.Enabled {
		return nil
	}

	loc, err := time.LoadLocation(b.Timezone)
	if err != nil {
		loc = time.UTC
	}
	spec := "CRON_TZ=" + loc.String() + " " + b.CronExpr
	entryID, err := s.cron.AddFunc(spec, func() {
		s.executeBackup(backupID)
	})
	if err != nil {
		return err
	}

	next := s.cron.Entry(entryID).Next.UTC()
	_ = s.repo.UpdateBackupNextRun(context.Background(), backupID, &next)

	s.mu.Lock()
	s.entries[backupID] = entryID
	s.mu.Unlock()

	return nil
}

func (s *Scheduler) Delete(backupID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.entries[backupID]; ok {
		s.cron.Remove(old)
		delete(s.entries, backupID)
	}
}

func (s *Scheduler) TriggerNow(backupID string) {
	go s.executeBackup(backupID)
}

func (s *Scheduler) executeBackup(backupID string) {
	s.sem <- struct{}{}
	defer func() { <-s.sem }()

	ctx := context.Background()
	run, runErr := s.repo.StartBackupRun(ctx, backupID)
	if runErr != nil {
		log.Printf("backup %s run record failed: %v", backupID, runErr)
	}

	b, err := s.repo.GetBackup(ctx, backupID)
	if err != nil {
		log.Printf("backup %s load failed: %v", backupID, err)
		if runErr == nil {
			_ = s.repo.FinishBackupRun(ctx, run.ID, "failed", err.Error(), "")
		}
		return
	}

	outputKey, err := s.executor.Run(ctx, b)
	if err != nil {
		log.Printf("backup %s failed: %v", backupID, err)
		if runErr == nil {
			_ = s.repo.FinishBackupRun(ctx, run.ID, "failed", err.Error(), "")
		}
		return
	}

	if err := s.executor.CleanupRetention(ctx, b); err != nil {
		log.Printf("backup %s retention cleanup failed: %v", backupID, err)
	}

	last := time.Now().UTC()
	next := s.nextRun(backupID)
	if err := s.repo.TouchBackupRun(ctx, backupID, last, next); err != nil {
		log.Printf("backup %s update run metadata failed: %v", backupID, err)
	}
	if runErr == nil {
		if err := s.repo.FinishBackupRun(ctx, run.ID, "success", "", outputKey); err != nil {
			log.Printf("backup %s finish run record failed: %v", backupID, err)
		}
	}

	log.Printf("backup %s completed", backupID)
}

func (s *Scheduler) nextRun(backupID string) *time.Time {
	s.mu.Lock()
	entryID, ok := s.entries[backupID]
	s.mu.Unlock()
	if !ok {
		return nil
	}
	n := s.cron.Entry(entryID).Next.UTC()
	return &n
}
