package sqlite

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/project"
)

func TestAdmissionAndPredecessorsDoNotReadProjectSnapshots(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	var lastID string
	for i := 0; i < 40; i++ {
		id := fmt.Sprintf("run-%02d", i)
		state := fresh(t, id, limits)
		if err := s.CreateRunWithProject(ctx, testRun(id), state, project.Snapshot{Root: "/workspace"}); err != nil {
			t.Fatal(err)
		}
		stopRun(t, s, testRun(id))
		lastID = id
	}
	// The project column is deliberately unreadable. Admission and
	// predecessor navigation must not decode it; only the run's own request
	// builder may.
	if _, err := s.db.ExecContext(ctx, `UPDATE runs SET project = '{'`); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckAdmission(ctx); err != nil {
		t.Fatalf("admission decoded project snapshots: %v", err)
	}
	previous, total, err := s.PreviousRuns(ctx, lastID)
	if err != nil {
		t.Fatalf("predecessor navigation decoded project snapshots: %v", err)
	}
	if total != 23 || len(previous) != 16 {
		t.Fatalf("previous runs = %d of %d earlier runs left out", len(previous), total)
	}
	meta, err := s.ProjectMeta(ctx, lastID)
	if err != nil || meta.Root != "/workspace" {
		t.Fatalf("project metadata = %+v, %v", meta, err)
	}
	if _, err := s.Project(ctx, lastID); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("project read = %v, want ErrIncompatible", err)
	}
}
