package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func sessionRun(id, input string) harness.Run {
	r := testRun(id)
	r.SessionID = "session_1"
	r.InputID = input
	return r
}

func stopRun(t *testing.T, s *Store, run harness.Run) {
	t.Helper()
	ctx := context.Background()
	from := fresh(t, run.ID, limits)
	event := core.StopRequested{Reason: core.StopCancelled}
	to, err := from.Next(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, harness.Transition{Seq: 1, EventID: "stop-" + run.ID, From: from, Event: event, To: to}); err != nil {
		t.Fatal(err)
	}
}

func TestSessionSubmitRetryAndPagingAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s := open(t, path)
	first := sessionRun("run-1", "input-1")
	got, err := s.Submit(ctx, first, fresh(t, first.ID, limits))
	if err != nil || got.SessionSeq != 1 {
		t.Fatalf("first Submit = %#v, %v", got, err)
	}
	stopRun(t, s, got)
	retry := sessionRun("unused-id", "input-1")
	retry.Created = retry.Created.Add(10)
	retry.Config = []byte("changed config")
	retried, err := s.Submit(ctx, retry, fresh(t, retry.ID, limits))
	if err != nil || !sameRun(retried, got) {
		t.Fatalf("retry = %#v, %v; want %#v", retried, err, got)
	}
	retry.Goal = "different"
	if _, err := s.Submit(ctx, retry, fresh(t, retry.ID, limits)); err == nil {
		t.Fatal("conflicting retry succeeded")
	}
	second := sessionRun("run-2", "input-2")
	got, err = s.Submit(ctx, second, fresh(t, second.ID, limits))
	if err != nil || got.SessionSeq != 2 {
		t.Fatalf("second Submit = %#v, %v", got, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path)
	page, err := s.SessionRuns(ctx, first.SessionID, 0, 1)
	if err != nil || len(page) != 1 || page[0].ID != second.ID {
		t.Fatalf("latest page = %#v, %v", page, err)
	}
	page, err = s.SessionRuns(ctx, first.SessionID, 2, 1)
	if err != nil || len(page) != 1 || page[0].ID != first.ID {
		t.Fatalf("older page = %#v, %v", page, err)
	}
	read, err := s.SessionInput(ctx, first.SessionID, first.InputID)
	if err != nil || read.ID != first.ID {
		t.Fatalf("SessionInput = %#v, %v", read, err)
	}
}

func TestSessionSubmitRejectsUnfinishedAndRollsBack(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	legacy := testRun("legacy")
	if err := s.CreateRun(ctx, legacy, fresh(t, legacy.ID, limits)); err != nil {
		t.Fatal(err)
	}
	r := sessionRun("run-1", "input-1")
	if _, err := s.Submit(ctx, r, fresh(t, r.ID, limits)); err == nil {
		t.Fatal("Submit over active legacy run succeeded")
	}
	if _, err := s.SessionInput(ctx, r.SessionID, r.InputID); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("input after rollback: %v", err)
	}
	stopRun(t, s, legacy)
	if _, err := s.Submit(ctx, r, fresh(t, r.ID, limits)); err != nil {
		t.Fatal(err)
	}
	bypass := sessionRun("run-2", "input-2")
	if err := s.CreateRun(ctx, bypass, fresh(t, bypass.ID, limits)); err == nil {
		t.Fatal("CreateRun bypass succeeded")
	}
}

func TestConcurrentSessionSubmitOneAdmission(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := sessionRun("run-"+string(rune('a'+i)), "input-"+string(rune('a'+i)))
			<-start
			_, errs[i] = s.Submit(ctx, r, fresh(t, r.ID, limits))
		}(i)
	}
	close(start)
	wg.Wait()
	if (errs[0] == nil) == (errs[1] == nil) {
		t.Fatalf("concurrent results = %v, %v", errs[0], errs[1])
	}
	page, err := s.SessionRuns(ctx, "session_1", 0, 32)
	if err != nil || len(page) != 1 {
		t.Fatalf("runs = %#v, %v", page, err)
	}
}

func TestSessionMigrationFromV1PreservesLegacyRun(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	v1 := strings.Split(schema, "CREATE TABLE session_runs")[0]
	if _, err := db.ExecContext(ctx, v1+"PRAGMA user_version = 1;"); err != nil {
		t.Fatal(err)
	}
	legacy := testRun("legacy")
	runData, initialData, err := encodeRun(legacy, fresh(t, legacy.ID, limits))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, insertRunSQL, legacy.ID, core.SemanticsVersion, runData, initialData); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s := open(t, path)
	if got, err := s.Run(ctx, legacy.ID); err != nil || !sameRun(got, legacy) {
		t.Fatalf("legacy run after migration = %#v, %v", got, err)
	}
	stopRun(t, s, legacy)
	r := sessionRun("run-1", "input-1")
	if _, err := s.Submit(ctx, r, fresh(t, r.ID, limits)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path)
	if got, err := s.SessionInput(ctx, r.SessionID, r.InputID); err != nil || got.ID != r.ID {
		t.Fatalf("migrated input = %#v, %v", got, err)
	}
}

func TestSessionAssociationFailureRollsBackRun(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER reject_session BEFORE INSERT ON session_runs BEGIN SELECT RAISE(ABORT, 'reject'); END`); err != nil {
		t.Fatal(err)
	}
	r := sessionRun("run-1", "input-1")
	if _, err := s.Submit(ctx, r, fresh(t, r.ID, limits)); err == nil {
		t.Fatal("Submit succeeded despite rejected association")
	}
	if _, err := s.Run(ctx, r.ID); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("run after rollback: %v", err)
	}
}

func TestSessionRejectsIdentityAndRunIDCollisionWithoutAssociation(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	for _, id := range []string{"../escape", "with space", "ü", strings.Repeat("a", 129), ""} {
		r := sessionRun("run-1", id)
		if _, err := s.Submit(ctx, r, fresh(t, r.ID, limits)); err == nil {
			t.Fatalf("Submit(input %q) succeeded", id)
		}
	}
	legacy := testRun("run-1")
	if err := s.CreateRun(ctx, legacy, fresh(t, legacy.ID, limits)); err != nil {
		t.Fatal(err)
	}
	stopRun(t, s, legacy)
	r := sessionRun(legacy.ID, "input-1")
	if _, err := s.Submit(ctx, r, fresh(t, r.ID, limits)); err == nil {
		t.Fatal("Submit with existing run ID succeeded")
	}
	if _, err := s.SessionInput(ctx, r.SessionID, r.InputID); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("orphan association: %v", err)
	}
}

func TestSessionAbandonedUnknownEffectBlocksAdmission(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	r := sessionRun("run-1", "input-1")
	if _, err := s.Submit(ctx, r, fresh(t, r.ID, limits)); err != nil {
		t.Fatal(err)
	}
	d := newDriver(t, s, harness.Snapshot{State: fresh(t, r.ID, limits)})
	d.decide(execBody("printf ok"), harness.Usage{})
	d.call(harness.Runner.Bind)
	d.call(harness.Runner.Start)
	d.do(core.OutcomeUnknown{EffectID: d.snap.State.EffectID}, harness.Facts{Failure: "lost job"})
	d.do(core.Resolved{EffectID: d.snap.State.EffectID, Execution: core.ExecutionUnknown}, harness.Facts{Observation: observation(core.ExecutionUnknown, 0)})
	if d.snap.State.Phase != core.PhaseStopped {
		t.Fatalf("phase = %s", d.snap.State.Phase)
	}
	next := sessionRun("run-2", "input-2")
	if _, err := s.Submit(ctx, next, fresh(t, next.ID, limits)); err == nil {
		t.Fatal("Submit after abandoned unknown effect succeeded")
	}
}
