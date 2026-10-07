package harness_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

const (
	earlierRun = "session-earlier"
	sessionRun = "session-current"
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

func (j *tampered) SessionRuns(ctx context.Context, sessionID string, before int64, limit int) ([]harness.Run, error) {
	v, err := j.Store.SessionRuns(ctx, sessionID, before, limit)
	return tamper(j, "SessionRuns", "", v, err)
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

// sessionAfterAStep returns a fixture whose session run, which follows an
// earlier run of its session stopped after it froze its first request, has
// observed its first step and waits to prepare its second.
func sessionAfterAStep(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t, limits, harness.Budget{}, execReply("true"), returnReply("done"))
	ctx := f.bounded()
	r, err := harness.NewRunner(f.store, f.exec)
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
	submit := func(id, goal string) {
		t.Helper()
		state, err := core.NewState(id, limits)
		if err == nil {
			_, err = f.store.Submit(ctx, harness.Run{ID: id, SessionID: "session-1", InputID: id, Goal: goal, Created: created}, state)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	stop(runID)
	submit(earlierRun, "Report the Go version.")
	l := f.loop()
	if s, err := l.Step(ctx, earlierRun); err != nil || s.State.Phase != core.PhaseRequestFrozen {
		t.Fatalf("earlier run: %+v, %v", s.State, err)
	}
	stop(earlierRun)
	submit(sessionRun, "Count the Go files.")
	for range 10 {
		s, err := l.Step(ctx, sessionRun)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Step == 2 {
			wantState(t, s, core.PhaseIdle, "")
			return f
		}
	}
	t.Fatal("the session run did not complete its first step")
	return nil
}

func sessionTransitions(t *testing.T, f *fixture) []harness.Transition {
	t.Helper()
	ts, err := f.store.Transitions(context.Background(), sessionRun, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

func TestPrepareRetriesAJournalReadThatFailed(t *testing.T) {
	incompatible := fmt.Errorf("%w: frozen request missing", sqlite.ErrIncompatible)
	for _, tc := range []struct {
		method, run string
		err         error
	}{
		{"LatestRequest", sessionRun, errTransient},
		{"LatestRequest", sessionRun, incompatible},
		{"SessionRuns", "", errTransient},
		{"Latest", earlierRun, errTransient},
		{"Totals", earlierRun, errTransient},
		{"Recent", earlierRun, errTransient},
		{"LatestRequest", earlierRun, errTransient},
	} {
		t.Run(fmt.Sprintf("%s %s/%v", tc.method, tc.run, tc.err), func(t *testing.T) {
			f := sessionAfterAStep(t)
			ctx := f.bounded()
			before := sessionTransitions(t, f)
			prompts, sent := f.model.calls()
			j := &tampered{Store: f.open(), method: tc.method, run: tc.run, alter: func(v any) (any, error) { return v, tc.err }}
			l := f.loopOn(j)
			snap, err := l.Step(ctx, sessionRun)
			after := sessionTransitions(t, f)
			if !errors.Is(err, tc.err) || snap.Seq != before[len(before)-1].Seq || len(after) != len(before) {
				t.Fatalf("Step() = %+v, %v after %d transitions, the last %s %+v; want %v and no commit",
					snap.State, err, len(after), after[len(after)-1].Event.Kind(), after[len(after)-1].Facts, tc.err)
			}
			if p, s := f.model.calls(); !j.done || p != prompts || s != sent {
				t.Fatalf("read reached %t; froze %d prompts and sent %d requests, want none", j.done, p-prompts, s-sent)
			}
			snap, err = l.Step(ctx, sessionRun)
			if err != nil || snap.State.Phase != core.PhaseRequestFrozen || snap.State.Step != 2 {
				t.Fatalf("Step() after the failed read = %+v, %v; want a request frozen for step 2", snap.State, err)
			}
			req, err := f.store.LatestRequest(ctx, sessionRun)
			want := []harness.ContextSessionSource{{RunID: earlierRun, SessionSeq: 1, Status: "stopped", IncludedGoal: true}}
			if err != nil || req.Context == nil || !slices.Equal(req.Context.Session, want) {
				t.Fatalf("frozen session context = %+v, %v; want %+v", req.Context, err, want)
			}
			snap, err = l.Run(ctx, sessionRun)
			if err != nil || snap.State.Phase != core.PhaseReturned || snap.State.Decision.Payload != "done" {
				t.Fatalf("Run() = %+v, %v; want the scripted return", snap.State, err)
			}
		})
	}
}

func TestPrepareStopsOnAnInconsistentContext(t *testing.T) {
	alter := func(method, run string, alter func(any) (any, error)) func(*sqlite.Store) harness.Journal {
		return func(s *sqlite.Store) harness.Journal {
			return &tampered{Store: s, method: method, run: run, alter: alter}
		}
	}
	request := func(edit func(*harness.Request)) func(any) (any, error) {
		return func(v any) (any, error) {
			r := v.(harness.Request)
			edit(&r)
			return r, nil
		}
	}
	for _, tc := range []struct {
		name    string
		journal func(*sqlite.Store) harness.Journal
		failure string
	}{
		{"window request missing after progress", alter("LatestRequest", sessionRun, func(any) (any, error) { return harness.Request{}, harness.ErrNotFound }),
			"harness: durable context window request is missing after progress"},
		{"window configuration changed", alter("LatestRequest", sessionRun, request(func(r *harness.Request) { r.Window = nil })),
			"harness: window configuration changed during a run"},
		{"invalid window", alter("LatestRequest", sessionRun, request(func(r *harness.Request) { r.Window = &harness.Window{FirstStep: 3} })),
			"harness: invalid context window at step 2"},
		{"no session sequence", alter("Run", sessionRun, func(v any) (any, error) {
			r := v.(harness.Run)
			r.SessionSeq = 0
			return r, nil
		}), "harness: session sequence is required"},
		{"candidate limit exceeded", alter("SessionRuns", "", func(v any) (any, error) { return slices.Repeat(v.([]harness.Run), 9), nil }),
			"harness: session journal exceeded the candidate limit"},
		{"invalid earlier run", alter("SessionRuns", "", func(v any) (any, error) {
			runs := v.([]harness.Run)
			runs[0].SessionSeq = 2
			return runs, nil
		}), "harness: invalid earlier session run"},
		{"earlier run not terminal", alter("Latest", earlierRun, func(any) (any, error) {
			return harness.Snapshot{State: core.State{RunID: earlierRun, Phase: core.PhaseIdle}}, nil
		}), `harness: earlier session run "session-earlier" is not terminal`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := sessionAfterAStep(t)
			prompts, _ := f.model.calls()
			j := tc.journal(f.open())
			snap, err := f.loopOn(j).Step(f.bounded(), sessionRun)
			if err != nil {
				t.Fatalf("Step() error = %v, want an unrecoverable stop", err)
			}
			wantState(t, snap, core.PhaseStopped, core.StopUnrecoverable)
			ts := sessionTransitions(t, f)
			if last := ts[len(ts)-1]; last.Event.Kind() != "stop_requested" || last.Facts != (harness.Facts{Failure: tc.failure}) || last.Seq != snap.Seq {
				t.Fatalf("last transition = %s %+v at %d, want stop_requested with %q at %d", last.Event.Kind(), last.Facts, last.Seq, tc.failure, snap.Seq)
			}
			if p, _ := f.model.calls(); p != prompts {
				t.Fatalf("froze %d prompts, want none", p-prompts)
			}
			if tj, ok := j.(*tampered); ok && !tj.done {
				t.Fatal("the tampered read was not reached")
			}
		})
	}
}
