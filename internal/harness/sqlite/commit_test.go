package sqlite

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

var errInjected = errors.New("injected failure")

func latestOf(t testing.TB, s *Store, runID string) harness.Snapshot {
	t.Helper()
	snap, err := s.Latest(context.Background(), runID)
	if err != nil {
		t.Fatalf("Latest(%q) error = %v", runID, err)
	}
	return snap
}

func totalsOf(t testing.TB, s *Store, runID string) harness.Totals {
	t.Helper()
	totals, err := s.Totals(context.Background(), runID)
	if err != nil {
		t.Fatalf("Totals(%q) error = %v", runID, err)
	}
	return totals
}

func history(t testing.TB, s *Store, runID string) []harness.Transition {
	t.Helper()
	got, err := s.Transitions(context.Background(), runID, 0, 1000)
	if err != nil {
		t.Fatalf("Transitions(%q) error = %v", runID, err)
	}
	return got
}

// unchanged fails unless the run still holds exactly want.
func unchanged(t testing.TB, s *Store, runID string, want []harness.Transition, totals harness.Totals) {
	t.Helper()
	if got := history(t, s, runID); !reflect.DeepEqual(got, want) {
		t.Fatalf("Transitions() = %#v, want %#v", got, want)
	}
	if got := totalsOf(t, s, runID); got != totals {
		t.Fatalf("Totals() = %#v, want %#v", got, totals)
	}
}

// cancelling drives a new run to a committed stop request during its effect.
func cancelling(t testing.TB, s *Store, runID string) *driver {
	t.Helper()
	d := newDriver(t, s, createRun(t, s, runID, limits))
	d.decide(execBody("sleep 60"), harness.Usage{InputTokens: 7, OutputTokens: 3})
	d.do(core.StopRequested{Reason: core.StopCancelled}, harness.Facts{})
	if d.snap.State.Phase != core.PhaseCancelling {
		t.Fatalf("fixture phase = %q, want cancelling", d.snap.State.Phase)
	}
	return d
}

func TestCommitPersistsTransitionsAndTotalsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	if got := latestOf(t, s, "run-1"); got != d.snap {
		t.Fatalf("Latest() of a new run = %#v, want the initial state at seq 0", got)
	}
	if got := totalsOf(t, s, "run-1"); got != (harness.Totals{}) {
		t.Fatalf("Totals() of a new run = %#v, want zero", got)
	}
	d.step("printf ok", harness.Usage{InputTokens: 10, OutputTokens: 5}, 42)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open(t, path)
	if got := latestOf(t, s, "run-1"); got != d.snap {
		t.Fatalf("Latest() = %#v, want %#v", got, d.snap)
	}
	want := harness.Totals{Usage: harness.Usage{InputTokens: 10, OutputTokens: 5}, OutputBytes: 42, Steps: 1}
	unchanged(t, s, "run-1", d.commits, want)
	page, err := s.Transitions(ctx, "run-1", 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(page, d.commits[2:5]) {
		t.Fatalf("Transitions(after 2, limit 3) = %#v, want seq 3..5", page)
	}
}

func TestCommitRejectsSequenceGap(t *testing.T) {
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.freeze()
	d.snap.Seq += 5
	if err := d.try(core.AttemptStarted{Step: 1, Attempt: 1}, harness.Facts{}); !errors.Is(err, harness.ErrStaleSnapshot) {
		t.Fatalf("error = %v, want ErrStaleSnapshot", err)
	}
	unchanged(t, s, "run-1", d.commits, harness.Totals{})
}

func TestCommitRejectsCompetingCommitAtUsedSeq(t *testing.T) {
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	idle := d.snap
	d.freeze()
	_, err := newRunner(t, s).Commit(context.Background(), idle, core.StopRequested{Reason: core.StopCancelled}, harness.Facts{})
	if !errors.Is(err, harness.ErrStaleSnapshot) {
		t.Fatalf("competing commit error = %v, want ErrStaleSnapshot", err)
	}
	unchanged(t, s, "run-1", d.commits, harness.Totals{})
}

func TestCommitRejectsStaleSnapshotAfterSelfLoop(t *testing.T) {
	s := open(t, tempPath(t))
	d := cancelling(t, s, "run-1")
	before := d.snap
	d.do(core.StopRequested{Reason: core.StopTimedOut}, harness.Facts{})
	if d.snap.State != before.State {
		t.Fatalf("self-loop changed the state to %#v", d.snap.State)
	}
	_, err := newRunner(t, s).Commit(context.Background(), before, core.StopRequested{Reason: core.StopBudgetExhausted}, harness.Facts{})
	if !errors.Is(err, harness.ErrStaleSnapshot) {
		t.Fatalf("error = %v, want ErrStaleSnapshot", err)
	}
	unchanged(t, s, "run-1", d.commits, harness.Totals{Usage: harness.Usage{InputTokens: 7, OutputTokens: 3}})
}

func TestCommitConfirmsIdenticalRetry(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.freeze()
	d.send()
	sent := d.snap
	d.save(execBody("ls"), harness.Usage{InputTokens: 11, OutputTokens: 4})
	totals := totalsOf(t, s, "run-1")
	last := d.commits[len(d.commits)-1]

	if err := s.Commit(ctx, last); err != nil {
		t.Fatalf("identical retry error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path)
	retried, err := newRunner(t, s).Commit(ctx, sent, last.Event, last.Facts)
	if err != nil {
		t.Fatalf("Runner retry error = %v", err)
	}
	if retried != d.snap {
		t.Fatalf("Runner retry = %#v, want the recorded %#v", retried, d.snap)
	}
	unchanged(t, s, "run-1", d.commits, totals)
}

func TestCommitNeverOverwritesConflictingContent(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.freeze()
	d.send()
	d.save(execBody("ls"), harness.Usage{InputTokens: 11, OutputTokens: 4})
	totals := totalsOf(t, s, "run-1")
	last := d.commits[len(d.commits)-1]

	otherResponse := *last.Facts.Response
	otherResponse.Usage.OutputTokens++
	otherTo := last.To
	otherTo.Phase = core.PhaseStopped
	otherTo.Reason = core.StopCancelled
	conflicts := map[string]func(*harness.Transition){
		"event ID": func(t *harness.Transition) { t.EventID += "x" },
		"event":    func(t *harness.Transition) { t.Event = core.ResponseSaved{Step: 1, Attempt: 2} },
		"kind":     func(t *harness.Transition) { t.Event = core.ResponseIncomplete{Step: 1, Attempt: 1} },
		"from":     func(t *harness.Transition) { t.From.Attempt = 2 },
		"to":       func(t *harness.Transition) { t.To = otherTo },
		"facts":    func(t *harness.Transition) { t.Facts.Response = &otherResponse },
	}
	for name, change := range conflicts {
		t.Run(name, func(t *testing.T) {
			conflict := last
			change(&conflict)
			if err := s.Commit(ctx, conflict); !errors.Is(err, harness.ErrStaleSnapshot) {
				t.Fatalf("error = %v, want ErrStaleSnapshot", err)
			}
			unchanged(t, s, "run-1", d.commits, totals)
		})
	}
}

func TestCommitRejectsFromOutsideTheChain(t *testing.T) {
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.freeze()
	d.snap.State.Attempt = 2
	if err := d.try(core.AttemptStarted{Step: 1, Attempt: 2}, harness.Facts{}); !errors.Is(err, harness.ErrBrokenChain) {
		t.Fatalf("error = %v, want ErrBrokenChain", err)
	}
	unchanged(t, s, "run-1", d.commits, harness.Totals{})
}

func TestCommitRequiresTheRun(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	ghost := harness.Snapshot{State: fresh(t, "ghost", limits)}
	if err := newDriver(t, s, ghost).try(core.RequestFrozen{}, harness.Facts{Request: requestFor(ghost.State)}); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("unknown run error = %v, want ErrNotFound", err)
	}
	for name, read := range map[string]func() error{
		"Latest":      func() error { _, err := s.Latest(ctx, "ghost"); return err },
		"Totals":      func() error { _, err := s.Totals(ctx, "ghost"); return err },
		"Transitions": func() error { _, err := s.Transitions(ctx, "ghost", 0, 10); return err },
	} {
		if err := read(); !errors.Is(err, harness.ErrNotFound) {
			t.Errorf("%s(unknown run) error = %v, want ErrNotFound", name, err)
		}
	}

	snap := createRun(t, s, "run-1", limits)
	moved := derive(t, snap, core.RequestFrozen{}, harness.Facts{Request: requestFor(snap.State)})
	moved.To.RunID = "run-2"
	createRun(t, s, "run-2", limits)
	if err := s.Commit(ctx, moved); err == nil {
		t.Fatal("Commit(To of another run) succeeded, want error")
	}
	unchanged(t, s, "run-1", nil, harness.Totals{})
	unchanged(t, s, "run-2", nil, harness.Totals{})
}

func TestCommitWritesNothingOnceContextIsDone(t *testing.T) {
	s := open(t, tempPath(t))
	snap := createRun(t, s, "run-1", limits)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tr := derive(t, snap, core.RequestFrozen{}, harness.Facts{Request: requestFor(snap.State)})
	if err := s.Commit(ctx, tr); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	unchanged(t, s, "run-1", nil, harness.Totals{})
}

func TestCommitRollsBackFailureAfterInsert(t *testing.T) {
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.freeze()
	d.send()
	before, totals := d.snap, totalsOf(t, s, "run-1")
	committed := append([]harness.Transition(nil), d.commits...)

	afterInsert = func() error { return errInjected }
	t.Cleanup(func() { afterInsert = nil })
	body, usage := execBody("ls"), harness.Usage{InputTokens: 9, OutputTokens: 2}
	if err := d.try(core.ResponseSaved{Step: 1, Attempt: 1}, harness.Facts{Response: responseFor(before.State, body, usage)}); !errors.Is(err, errInjected) {
		t.Fatalf("error = %v, want the injected failure", err)
	}
	if got := latestOf(t, s, "run-1"); got != before {
		t.Fatalf("Latest() = %#v, want %#v", got, before)
	}
	unchanged(t, s, "run-1", committed, totals)

	afterInsert = nil
	d.save(body, usage)
	if got := totalsOf(t, s, "run-1"); got.Usage != usage {
		t.Fatalf("Totals() after the corrected commit = %#v, want usage %#v", got, usage)
	}
}

func TestConcurrentStoresCommitOneTransitionPerSeq(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	stores := []*Store{open(t, path), open(t, path)}
	for i := range 20 {
		runID := fmt.Sprintf("run-%d", i)
		snap := createRun(t, stores[0], runID, limits)
		candidates := []harness.Transition{
			derive(t, snap, core.RequestFrozen{}, harness.Facts{Request: requestFor(snap.State)}),
			derive(t, snap, core.StopRequested{Reason: core.StopCancelled}, harness.Facts{}),
		}
		errs := make([]error, len(candidates))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for j, candidate := range candidates {
			wg.Go(func() {
				<-start
				errs[j] = stores[(i+j)%2].Commit(ctx, candidate)
			})
		}
		close(start)
		wg.Wait()

		winner := -1
		for j, err := range errs {
			switch {
			case err == nil && winner < 0:
				winner = j
			case !errors.Is(err, harness.ErrStaleSnapshot):
				t.Fatalf("%s: commit %d error = %v; want one success and one ErrStaleSnapshot", runID, j, err)
			}
		}
		if winner < 0 {
			t.Fatalf("%s: no commit succeeded: %v", runID, errs)
		}
		for _, s := range stores {
			unchanged(t, s, runID, candidates[winner:winner+1], totalsOf(t, s, runID))
		}
	}
}
