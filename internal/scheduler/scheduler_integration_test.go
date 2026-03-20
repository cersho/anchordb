package scheduler_test

import (
	"context"
	"testing"

	"anchordb/internal/testutil"
)

func TestUpsertSetsNextRun(t *testing.T) {
	stack := testutil.NewStack(t)
	ctx := context.Background()

	conn := testutil.MustCreateConnection(t, stack.Repo, "scheduler-source")
	backup := testutil.MustCreateLocalBackup(t, stack.Repo, "scheduler-local", conn.ID, t.TempDir(), true)

	if err := stack.Scheduler.Upsert(ctx, backup.ID); err != nil {
		t.Fatalf("upsert backup schedule: %v", err)
	}

	fresh, err := stack.Repo.GetBackup(ctx, backup.ID)
	if err != nil {
		t.Fatalf("get backup after upsert: %v", err)
	}

	if fresh.NextRunAt == nil {
		t.Fatal("expected next_run_at to be set after upsert")
	}

	stack.Scheduler.Delete(backup.ID)
}

func TestLoadAllSchedulesOnlyEnabledBackups(t *testing.T) {
	stack := testutil.NewStack(t)
	ctx := context.Background()

	conn := testutil.MustCreateConnection(t, stack.Repo, "loadall-source")
	enabled := testutil.MustCreateLocalBackup(t, stack.Repo, "enabled-backup", conn.ID, t.TempDir(), true)
	disabled := testutil.MustCreateLocalBackup(t, stack.Repo, "disabled-backup", conn.ID, t.TempDir(), true)

	if err := stack.Repo.SetBackupEnabled(ctx, disabled.ID, false); err != nil {
		t.Fatalf("disable backup: %v", err)
	}

	if err := stack.Scheduler.LoadAll(ctx); err != nil {
		t.Fatalf("load all schedules: %v", err)
	}

	enabledFresh, err := stack.Repo.GetBackup(ctx, enabled.ID)
	if err != nil {
		t.Fatalf("get enabled backup: %v", err)
	}
	if enabledFresh.NextRunAt == nil {
		t.Fatal("expected enabled backup to have next_run_at after load all")
	}

	disabledFresh, err := stack.Repo.GetBackup(ctx, disabled.ID)
	if err != nil {
		t.Fatalf("get disabled backup: %v", err)
	}
	if disabledFresh.NextRunAt != nil {
		t.Fatalf("expected disabled backup next_run_at to be nil, got %v", *disabledFresh.NextRunAt)
	}
}
