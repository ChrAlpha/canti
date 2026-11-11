package harness_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

const (
	earlierRun = "line-earlier"
	lineRun    = "line-current"
)

var errTransient = errors.New("journal read: input/output error")

// tampered reads through a SQLite journal, but hands the first read of
// method for run, any run when run is empty, to alter, as a failing or
// corrupted journal would answer it.
type tampered struct {
	*sqlite.Store
	method, run string
	alter       func(any) (any, error)
	done        bool
}

func tamper[T any](j *tampered, method, run string, v T, err error) (T, error) {
	if j.done || err != nil || method != j.method || j.run != "" && run != j.run {
		return v, err
	}
	j.done = true
	got, err := j.alter(v)
	return got.(T), err
}

func (j *tampered) Run(ctx context.Context, runID string) (harness.Run, error) {
	v, err := j.Store.Run(ctx, runID)
	return tamper(j, "Run", runID, v, err)
}

func (j *tampered) LatestRequest(ctx context.Context, runID string) (harness.Request, error) {
	v, err := j.Store.LatestRequest(ctx, runID)
	return tamper(j, "LatestRequest", runID, v, err)
}

func (j *tampered) PreviousRuns(ctx context.Context, runID string) ([]harness.PreviousRun, int, error) {
	v, more, err := j.Store.PreviousRuns(ctx, runID)
	if j.done || err != nil || j.method != "PreviousRuns" || j.run != "" && runID != j.run {
		return v, more, err
	}
	j.done = true
	got, err := j.alter(v)
	return got.([]harness.PreviousRun), more, err
}

func (j *tampered) Latest(ctx context.Context, runID string) (harness.Snapshot, error) {
	v, err := j.Store.Latest(ctx, runID)
	return tamper(j, "Latest", runID, v, err)
}

func (j *tampered) Recent(ctx context.Context, runID string, before int64, limit int) ([]harness.StepRecord, error) {
	v, err := j.Store.Recent(ctx, runID, before, limit)
	return tamper(j, "Recent", runID, v, err)
}

func (j *tampered) Totals(ctx context.Context, runID string) (harness.Totals, error) {
	v, err := j.Store.Totals(ctx, runID)
	return tamper(j, "Totals", runID, v, err)
}

func (j *tampered) Context(ctx context.Context, snap harness.Snapshot) (harness.ContextState, error) {
	v, err := j.Store.Context(ctx, snap)
	return tamper(j, "Context", snap.State.RunID, v, err)
}

func (j *tampered) Receipts(ctx context.Context, id string, before int64, limit int) ([]harness.StepReceipt, error) {
	v, err := j.Store.Receipts(ctx, id, before, limit)
	return tamper(j, "Receipts", id, v, err)
}

// lineAfterAStep returns a fixture whose current run, which follows an
// earlier run of its line stopped after it froze its first request, has
// observed its first step and waits to prepare its second.
func lineAfterAStep(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t, limits, harness.Budget{}, execReply("true"), returnReply("done"))
	ctx := f.bounded()
	r, err := harness.NewRunner(f.store)
	if err != nil {
		t.Fatal(err)
	}
	stop := func(id string) {
		t.Helper()
		s, err := f.store.Latest(ctx, id)
		if err == nil {
			_, err = r.Commit(ctx, s, core.StopRequested{Reason: core.StopCancelled}, harness.Facts{})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	create := func(id, goal string, fresh bool) {
		t.Helper()
		state, err := core.NewState(id, limits)
		if err == nil {
			err = f.store.CreateRun(ctx, harness.Run{ID: id, Goal: goal, Created: created, Fresh: fresh}, state)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	stop(runID)
	create(earlierRun, "Report the Go version.", true)
	l := f.loop()
	if s, err := l.Step(ctx, earlierRun); err != nil || s.State.Phase != core.PhaseRequestFrozen {
		t.Fatalf("earlier run: %+v, %v", s.State, err)
	}
	stop(earlierRun)
	create(lineRun, "Count the Go files.", false)
	for range 10 {
		s, err := l.Step(ctx, lineRun)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Step == 2 {
			wantState(t, s, core.PhaseIdle, "")
			return f
		}
	}
	t.Fatal("the current run did not complete its first step")
	return nil
}

func lineTransitions(t *testing.T, f *fixture) []harness.Transition {
	t.Helper()
	ts, err := f.store.Transitions(context.Background(), lineRun, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

func TestPrepareRetriesAJournalReadThatFailed(t *testing.T) {
	for _, method := range []string{"Context", "Receipts", "Recent", "PreviousRuns"} {
		t.Run(method, func(t *testing.T) {
			f := lineAfterAStep(t)
			before := lineTransitions(t, f)
			prompts, sent := f.model.calls()
			j := &tampered{Store: f.open(), method: method, alter: func(v any) (any, error) { return v, errTransient }}
			l := f.loopOn(j)
			snap, err := l.Step(f.bounded(), lineRun)
			if !errors.Is(err, errTransient) || snap.Seq != before[len(before)-1].Seq || len(lineTransitions(t, f)) != len(before) {
				t.Fatalf("failed read committed: %+v %v", snap, err)
			}
			if p, s := f.model.calls(); !j.done || p != prompts || s != sent {
				t.Fatal("failed read reached model")
			}
			snap, err = l.Step(f.bounded(), lineRun)
			if err != nil || snap.State.Phase != core.PhaseRequestFrozen {
				t.Fatalf("recovery=%+v %v", snap, err)
			}
			request, err := f.store.LatestRequest(f.bounded(), lineRun)
			if err != nil || request.Context.PreviousRun != earlierRun {
				t.Fatalf("previous run navigation=%+v %v", request.Context, err)
			}
		})
	}
}

func TestPrepareStopsOnAnInconsistentContext(t *testing.T) {
	for _, method := range []string{"Run", "PreviousRuns"} {
		t.Run(method, func(t *testing.T) {
			f := lineAfterAStep(t)
			j := &tampered{Store: f.open(), method: method, alter: func(v any) (any, error) {
				if method == "Run" {
					r := v.(harness.Run)
					r.Goal = ""
					return r, nil
				}
				runs := v.([]harness.PreviousRun)
				runs[0].Run.ID = lineRun
				return runs, nil
			}}
			snap, err := f.loopOn(j).Step(f.bounded(), lineRun)
			if err != nil || snap.State.Reason != core.StopUnrecoverable || !j.done {
				t.Fatalf("corrupt context=%+v %v", snap, err)
			}
		})
	}
}
