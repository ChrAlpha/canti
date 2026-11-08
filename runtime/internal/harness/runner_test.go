package harness

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

var (
	errInjected = errors.New("injected boundary failure")
	execBody    = []byte(`{"kind":"exec","payload":"printf ok"}`)
	returnBody  = []byte(`{"kind":"return","payload":"done"}`)
)

type recorder struct {
	log       []string
	commits   []Transition
	bySeq     map[int64]Transition
	seeded    bool
	lastSeq   int64
	failAt    core.Phase
	loseAckAt core.Phase
}

func (r *recorder) Commit(_ context.Context, transition Transition) error {
	r.log = append(r.log, "commit:"+string(transition.To.Phase))
	if transition.To.Phase == r.failAt {
		return errInjected
	}
	if existing, ok := r.bySeq[transition.Seq]; ok {
		if existing.EventID == transition.EventID && reflect.DeepEqual(existing, transition) {
			return nil
		}
		return ErrStaleSnapshot
	}
	if r.seeded && transition.Seq != r.lastSeq+1 {
		return ErrStaleSnapshot
	}
	if previous, ok := r.bySeq[transition.Seq-1]; ok && transition.From != previous.To {
		return ErrBrokenChain
	}
	if r.bySeq == nil {
		r.bySeq = make(map[int64]Transition)
	}
	r.bySeq[transition.Seq] = transition
	r.commits = append(r.commits, transition)
	r.seeded = true
	r.lastSeq = transition.Seq
	if transition.To.Phase == r.loseAckAt {
		return errInjected
	}
	return nil
}

func newRunner(t *testing.T, rec *recorder) Runner {
	t.Helper()
	runner, err := NewRunner(rec)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func advance(t *testing.T, snap Snapshot, events ...core.Event) Snapshot {
	t.Helper()
	for _, event := range events {
		next, err := snap.State.Next(event)
		if err != nil {
			t.Fatalf("Next(%T) error = %v", event, err)
		}
		snap = Snapshot{Seq: snap.Seq + 1, State: next}
	}
	return snap
}

func snapAt(t *testing.T, phase core.Phase) Snapshot {
	t.Helper()
	state, err := core.NewState("run-1", core.Limits{MaxSteps: 3, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	snap := Snapshot{State: state}
	switch phase {
	case core.PhaseIdle:
		return snap
	case core.PhaseRequestFrozen:
		return advance(t, snap, core.RequestFrozen{})
	case core.PhaseRequestSent:
		return advance(t, snap, core.RequestFrozen{}, core.AttemptStarted{Step: 1, Attempt: 1})
	case core.PhaseResponseSaved:
		return advance(t, snap, core.RequestFrozen{}, core.AttemptStarted{Step: 1, Attempt: 1}, core.ResponseSaved{Step: 1, Attempt: 1})
	case core.PhaseStopped:
		return advance(t, snap, core.StopRequested{Reason: core.StopCancelled})
	}
	t.Fatalf("snapAt(%q) is not supported", phase)
	return Snapshot{}
}

func effectSaved(t *testing.T) Snapshot {
	t.Helper()
	return advance(t, snapAt(t, core.PhaseResponseSaved), core.ResponseLoaded{Step: 1, Attempt: 1, Body: execBody})
}

func TestNewRunnerRequiresDependencies(t *testing.T) {
	if _, err := NewRunner(nil); err == nil {
		t.Error("NewRunner(nil store) succeeded, want error")
	}
}

func TestCommitRecordsTransitionWithFacts(t *testing.T) {
	rec := &recorder{}
	runner := newRunner(t, rec)
	from := snapAt(t, core.PhaseIdle)
	request := Request{ContextState: &ContextState{FirstStep: 1}, Body: []byte("frozen"), Model: "m", Protocol: "p", Adapter: "a"}

	got, err := runner.Commit(context.Background(), from, core.RequestFrozen{}, Facts{Request: &request})
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if got.State.Phase != core.PhaseRequestFrozen || got.Seq != from.Seq+1 {
		t.Fatalf("Commit() = %#v, want request_frozen at seq %d", got, from.Seq+1)
	}
	if len(rec.commits) != 1 {
		t.Fatalf("commits = %#v, want one", rec.commits)
	}
	c := rec.commits[0]
	if c.Seq != got.Seq || c.EventID == "" {
		t.Fatalf("commit identity = seq %d, event %q; want seq %d and a derived event ID", c.Seq, c.EventID, got.Seq)
	}
	if c.From != from.State || c.Event != (core.RequestFrozen{}) || c.To != got.State || c.Facts != (Facts{Request: &request}) {
		t.Fatalf("commit = %#v", c)
	}
}

func TestCommitWritesSelfLoopTransitionWithNewSeq(t *testing.T) {
	rec := &recorder{}
	saved := effectSaved(t)
	from := advance(t, saved, core.StopRequested{Reason: core.StopCancelled}, core.OutcomeUnknown{EffectID: saved.State.EffectID})

	got, err := newRunner(t, rec).Commit(context.Background(), from, core.StopRequested{Reason: core.StopTimedOut}, Facts{})
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if got.State != from.State || got.Seq != from.Seq+1 {
		t.Fatalf("Commit() = %#v, want the unchanged state at seq %d", got, from.Seq+1)
	}
	if len(rec.commits) != 1 || rec.commits[0].From != rec.commits[0].To {
		t.Fatalf("commits = %#v; want the unchanged state recorded once", rec.commits)
	}
}

func TestCommitDerivesDeterministicEventIDs(t *testing.T) {
	ctx := context.Background()
	from := snapAt(t, core.PhaseIdle)
	facts := Facts{Request: &Request{ContextState: &ContextState{FirstStep: 1}, Body: []byte("x")}}

	recA, recB := &recorder{}, &recorder{}
	a, err := newRunner(t, recA).Commit(ctx, from, core.RequestFrozen{}, facts)
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if _, err := newRunner(t, recB).Commit(ctx, from, core.RequestFrozen{}, facts); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if recA.commits[0].EventID != recB.commits[0].EventID {
		t.Fatalf("event IDs differ for the same commit: %q != %q", recA.commits[0].EventID, recB.commits[0].EventID)
	}
	if _, err := newRunner(t, recA).Commit(ctx, a, core.AttemptStarted{Step: 1, Attempt: 1}, Facts{}); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if recA.commits[0].EventID == recA.commits[1].EventID {
		t.Fatal("distinct commits share an event ID")
	}
}

func TestCommitRetryAfterLostAckIsIdempotent(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{loseAckAt: core.PhaseRequestFrozen}
	runner := newRunner(t, rec)
	from := snapAt(t, core.PhaseIdle)
	facts := Facts{Request: &Request{ContextState: &ContextState{FirstStep: 1}, Body: []byte("x")}}

	if _, err := runner.Commit(ctx, from, core.RequestFrozen{}, facts); !errors.Is(err, errInjected) {
		t.Fatalf("error = %v, want the lost acknowledgement", err)
	}
	rec.loseAckAt = ""
	got, err := runner.Commit(ctx, from, core.RequestFrozen{}, facts)
	if err != nil {
		t.Fatalf("retry error = %v", err)
	}
	if got.Seq != from.Seq+1 || got.State.Phase != core.PhaseRequestFrozen {
		t.Fatalf("retry = %#v, want the original commit confirmed", got)
	}
	if len(rec.commits) != 1 {
		t.Fatalf("commits = %#v, want the transition recorded once", rec.commits)
	}
}

func TestStaleSnapshotCannotCommitAfterSelfLoop(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{}
	runner := newRunner(t, rec)
	cancelling := advance(t, effectSaved(t), core.StopRequested{Reason: core.StopCancelled})

	first, err := runner.Commit(ctx, cancelling, core.StopRequested{Reason: core.StopTimedOut}, Facts{})
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if first.State != cancelling.State {
		t.Fatalf("self-loop changed state to %#v", first.State)
	}
	got, err := runner.Commit(ctx, cancelling, core.StopRequested{Reason: core.StopBudgetExhausted}, Facts{})
	if !errors.Is(err, ErrStaleSnapshot) {
		t.Fatalf("error = %v, want stale snapshot", err)
	}
	if got != cancelling || len(rec.commits) != 1 {
		t.Fatalf("stale commit advanced to %#v with commits %#v", got, rec.commits)
	}
}

func TestCommitDistinguishesRepeatedEventsBySequence(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{}
	runner := newRunner(t, rec)
	cancelling := advance(t, effectSaved(t), core.StopRequested{Reason: core.StopCancelled})

	first, err := runner.Commit(ctx, cancelling, core.StopRequested{Reason: core.StopTimedOut}, Facts{})
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if _, err := runner.Commit(ctx, first, core.StopRequested{Reason: core.StopTimedOut}, Facts{}); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if len(rec.commits) != 2 {
		t.Fatalf("commits = %#v, want two records", rec.commits)
	}
	if rec.commits[0].EventID == rec.commits[1].EventID {
		t.Fatal("the same event at consecutive sequences shares an event ID")
	}
}

func TestCommitRejectsSequenceGap(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{}
	runner := newRunner(t, rec)
	from := snapAt(t, core.PhaseIdle)

	mid, err := runner.Commit(ctx, from, core.RequestFrozen{}, Facts{Request: &Request{ContextState: &ContextState{FirstStep: 1}, Body: []byte("x")}})
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	ahead := Snapshot{Seq: mid.Seq + 5, State: mid.State}
	got, err := runner.Commit(ctx, ahead, core.AttemptStarted{Step: 1, Attempt: 1}, Facts{})
	if !errors.Is(err, ErrStaleSnapshot) {
		t.Fatalf("error = %v, want stale snapshot", err)
	}
	if got != ahead || len(rec.commits) != 1 {
		t.Fatalf("gapped commit advanced to %#v with commits %#v", got, rec.commits)
	}
}

func TestCommitRejectsFromOutsideTheChain(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{}
	runner := newRunner(t, rec)
	from := snapAt(t, core.PhaseIdle)

	mid, err := runner.Commit(ctx, from, core.RequestFrozen{}, Facts{Request: &Request{ContextState: &ContextState{FirstStep: 1}, Body: []byte("x")}})
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	foreign := mid
	foreign.State.Attempt = 2
	got, err := runner.Commit(ctx, foreign, core.AttemptStarted{Step: 1, Attempt: 2}, Facts{})
	if !errors.Is(err, ErrBrokenChain) {
		t.Fatalf("error = %v, want the broken chain rejected", err)
	}
	if got != foreign || len(rec.commits) != 1 {
		t.Fatalf("foreign From advanced to %#v with commits %#v", got, rec.commits)
	}
}

func TestCommitRequiresFactsForEvent(t *testing.T) {
	saved := effectSaved(t)
	awaiting := saved
	if awaiting.State.Phase != core.PhaseEffectSaved {
		t.Fatalf("fixture phase = %q, want effect_saved", awaiting.State.Phase)
	}
	observation := Observation{Execution: core.ExecutionExited}
	tests := []struct {
		name  string
		from  Snapshot
		event core.Event
		facts Facts
	}{
		{"frozen request without bytes", snapAt(t, core.PhaseIdle), core.RequestFrozen{}, Facts{}},
		{"frozen request missing context", snapAt(t, core.PhaseIdle), core.RequestFrozen{}, Facts{Request: &Request{Body: []byte("frozen")}}},
		{"frozen request with empty body", snapAt(t, core.PhaseIdle), core.RequestFrozen{}, Facts{Request: &Request{}}},
		{"frozen request with a response", snapAt(t, core.PhaseIdle), core.RequestFrozen{}, Facts{Request: &Request{ContextState: &ContextState{FirstStep: 1}, Body: []byte("x")}, Response: &Response{}}},
		{"attempt with facts", snapAt(t, core.PhaseRequestFrozen), core.AttemptStarted{Step: 1, Attempt: 1}, Facts{Failure: "x"}},
		{"saved response without record", snapAt(t, core.PhaseRequestSent), core.ResponseSaved{Step: 1, Attempt: 1}, Facts{}},
		{"saved response incomplete", snapAt(t, core.PhaseRequestSent), core.ResponseSaved{Step: 1, Attempt: 1}, Facts{Response: &Response{Body: execBody}}},
		{"saved response without body", snapAt(t, core.PhaseRequestSent), core.ResponseSaved{Step: 1, Attempt: 1}, Facts{Response: &Response{Complete: true}}},
		{"incomplete response marked complete", snapAt(t, core.PhaseRequestSent), core.ResponseIncomplete{Step: 1, Attempt: 1}, Facts{Response: &Response{Complete: true}}},
		{"observation without record", awaiting, core.Observed{EffectID: awaiting.State.EffectID, Execution: core.ExecutionExited}, Facts{}},
		{"observation mismatch", awaiting, core.Observed{EffectID: awaiting.State.EffectID, Execution: core.ExecutionExited}, Facts{Observation: &Observation{Execution: core.ExecutionTimedOut}}},
		{"observation with failure text", awaiting, core.Observed{EffectID: awaiting.State.EffectID, Execution: core.ExecutionExited}, Facts{Observation: &observation, Failure: "x"}},
		{"resolution without record", advance(t, awaiting, core.OutcomeUnknown{EffectID: awaiting.State.EffectID}), core.Resolved{EffectID: awaiting.State.EffectID, Execution: core.ExecutionExited}, Facts{}},
		{"stop with a request", snapAt(t, core.PhaseIdle), core.StopRequested{Reason: core.StopTimedOut}, Facts{Request: &Request{ContextState: &ContextState{FirstStep: 1}, Body: []byte("x")}, Failure: "x"}},
		{"stop with an observation", awaiting, core.StopRequested{Reason: core.StopCancelled}, Facts{Observation: &observation}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			got, err := newRunner(t, rec).Commit(context.Background(), tt.from, tt.event, tt.facts)
			if !errors.Is(err, ErrFacts) {
				t.Fatalf("error = %v, want facts error", err)
			}
			if got != tt.from || len(rec.log) != 0 {
				t.Fatalf("rejected commit changed snapshot to %#v or wrote %v", got, rec.log)
			}
		})
	}
}

func TestCommitReportsCoreErrorsBeforeFacts(t *testing.T) {
	tests := []struct {
		name  string
		from  Snapshot
		event core.Event
		facts Facts
		want  error
	}{
		{"invalid event with permitted facts", snapAt(t, core.PhaseIdle), &core.RequestFrozen{}, Facts{Request: &Request{ContextState: &ContextState{FirstStep: 1}, Body: []byte("x")}}, core.ErrInvalidEvent},
		{"illegal transition with wrong facts", snapAt(t, core.PhaseStopped), core.RequestFrozen{}, Facts{}, core.ErrIllegalTransition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			got, err := newRunner(t, rec).Commit(context.Background(), tt.from, tt.event, tt.facts)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if got != tt.from || len(rec.log) != 0 {
				t.Fatalf("rejected commit changed snapshot to %#v or wrote %v", got, rec.log)
			}
		})
	}
}

func TestCommitAcceptsPermittedFacts(t *testing.T) {
	saved := effectSaved(t)
	unresolved := advance(t, saved, core.OutcomeUnknown{EffectID: saved.State.EffectID})
	tests := []struct {
		name  string
		from  Snapshot
		event core.Event
		facts Facts
	}{
		{"incomplete response with record and failure", snapAt(t, core.PhaseRequestSent), core.ResponseIncomplete{Step: 1, Attempt: 1}, Facts{Response: &Response{Body: []byte("partial")}, Failure: "timeout"}},
		{"incomplete response without facts", snapAt(t, core.PhaseRequestSent), core.ResponseIncomplete{Step: 1, Attempt: 1}, Facts{}},
		{"unknown outcome with failure", saved, core.OutcomeUnknown{EffectID: saved.State.EffectID}, Facts{Failure: "lost"}},
		{"stop with failure", snapAt(t, core.PhaseIdle), core.StopRequested{Reason: core.StopTimedOut}, Facts{Failure: "wall time spent"}},
		{"stop without facts", saved, core.StopRequested{Reason: core.StopCancelled}, Facts{}},
		{"resolution with record", unresolved, core.Resolved{EffectID: unresolved.State.EffectID, Execution: core.ExecutionExited}, Facts{Observation: &Observation{Execution: core.ExecutionExited}, Resolution: &Resolution{Source: ResolutionOperator, OwnerID: "job-1"}}},
		{"abandonment with unknown record", unresolved, core.Resolved{EffectID: unresolved.State.EffectID, Execution: core.ExecutionUnknown}, Facts{Observation: &Observation{Execution: core.ExecutionUnknown}, Resolution: &Resolution{Source: ResolutionOperator}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			if _, err := newRunner(t, rec).Commit(context.Background(), tt.from, tt.event, tt.facts); err != nil {
				t.Fatalf("Commit() error = %v", err)
			}
			if len(rec.commits) != 1 || rec.commits[0].Facts != tt.facts {
				t.Fatalf("commits = %#v, want facts %#v", rec.commits, tt.facts)
			}
		})
	}
}

func TestRunnerRefusesInvalidStates(t *testing.T) {
	tests := []struct {
		name string
		call func(Runner, Snapshot) (Snapshot, error)
		from Snapshot
	}{
		{"commit", func(r Runner, s Snapshot) (Snapshot, error) {
			return r.Commit(context.Background(), s, core.RequestFrozen{}, Facts{Request: &Request{ContextState: &ContextState{FirstStep: 1}, Body: []byte("x")}})
		}, Snapshot{State: core.State{Phase: core.PhaseIdle}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			got, err := tt.call(newRunner(t, rec), tt.from)
			if !errors.Is(err, core.ErrInvalidState) {
				t.Fatalf("error = %v, want invalid state", err)
			}
			if got != tt.from || len(rec.log) != 0 {
				t.Fatalf("invalid state reached I/O: snapshot %#v, log %v", got, rec.log)
			}
		})
	}

	t.Run("negative sequence", func(t *testing.T) {
		rec := &recorder{}
		from := snapAt(t, core.PhaseIdle)
		from.Seq = -1
		got, err := newRunner(t, rec).Commit(context.Background(), from, core.RequestFrozen{}, Facts{Request: &Request{ContextState: &ContextState{FirstStep: 1}, Body: []byte("x")}})
		if err == nil || len(rec.log) != 0 || got != from {
			t.Fatalf("negative sequence reached I/O: %#v, %v, log %v", got, err, rec.log)
		}
	})
}

func TestCommitStopsAtStoreFailure(t *testing.T) {
	rec := &recorder{failAt: core.PhaseRequestFrozen}
	from := snapAt(t, core.PhaseIdle)
	got, err := newRunner(t, rec).Commit(context.Background(), from, core.RequestFrozen{}, Facts{Request: &Request{ContextState: &ContextState{FirstStep: 1}, Body: []byte("x")}})
	if !errors.Is(err, errInjected) {
		t.Fatalf("error = %v, want injected failure", err)
	}
	if got != from {
		t.Fatalf("failed commit changed snapshot to %#v", got)
	}
}

func TestCommitHonoursCancellationBeforeWriting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := &recorder{}
	from := snapAt(t, core.PhaseIdle)
	got, err := newRunner(t, rec).Commit(ctx, from, core.RequestFrozen{}, Facts{Request: &Request{ContextState: &ContextState{FirstStep: 1}, Body: []byte("x")}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want cancellation", err)
	}
	if got != from || len(rec.log) != 0 {
		t.Fatalf("cancelled commit changed snapshot to %#v or wrote %v", got, rec.log)
	}
}

var commandAction = Action{Name: ActionCommand, Recovery: RecoveryExternal}

// TestCommitPermitsActionsOutcomesAndWindowsOnlyWhereTheyBelong requires
// that an accepting ResponseLoaded carries exactly a valid action, that a
// Performed carries exactly a valid outcome and at most a window valid for
// the next step, and that no other event carries any of them.
func TestCommitPermitsActionsOutcomesAndWindowsOnlyWhereTheyBelong(t *testing.T) {
	ctx := context.Background()
	saved := snapAt(t, core.PhaseResponseSaved)
	accept := core.ResponseLoaded{Step: 1, Attempt: 1, Body: execBody}
	reject := core.ResponseLoaded{Step: 1, Attempt: 1, Body: []byte(`{"kind":"wait","payload":"x"}`)}
	effect, err := saved.State.Next(accept)
	if err != nil {
		t.Fatal(err)
	}
	performed := Snapshot{Seq: 4, State: effect}
	done := &Outcome{Status: OutcomeDone, Focus: &FocusSelection{}}
	bad := []struct {
		name  string
		snap  Snapshot
		event core.Event
		facts Facts
	}{
		{"acceptance without action", saved, accept, Facts{}},
		{"invalid action", saved, accept, Facts{Action: &Action{Name: ActionCommand, Recovery: RecoveryRead}}},
		{"action on a rejected decision", saved, reject, Facts{Action: &commandAction}},
		{"outcome on an acceptance", saved, accept, Facts{Action: &commandAction, Outcome: done}},
		{"performed without outcome", performed, core.Performed{EffectID: effect.EffectID}, Facts{}},
		{"invalid outcome", performed, core.Performed{EffectID: effect.EffectID}, Facts{Outcome: &Outcome{Status: OutcomeDone}}},
		{"observation on a performance", performed, core.Performed{EffectID: effect.EffectID}, Facts{Outcome: done, Observation: &Observation{Execution: core.ExecutionExited}}},
		{"window on an observation", performed, core.Observed{EffectID: effect.EffectID, Execution: core.ExecutionFailedToStart},
			Facts{Observation: &Observation{Execution: core.ExecutionFailedToStart}, ContextState: &ContextState{FirstStep: 1}}},
		{"handover in an observation", performed, core.Observed{EffectID: effect.EffectID, Execution: core.ExecutionFailedToStart},
			Facts{Observation: &Observation{Execution: core.ExecutionFailedToStart, Retrieval: &archive.Retrieval{}}}},
	}
	for _, tc := range bad {
		rec := &recorder{}
		if _, err := newRunner(t, rec).Commit(ctx, tc.snap, tc.event, tc.facts); !errors.Is(err, ErrFacts) || len(rec.log) != 0 {
			t.Errorf("%s: error %v, wrote %v", tc.name, err, rec.log)
		}
	}
	rec := &recorder{}
	if _, err := newRunner(t, rec).Commit(ctx, performed, core.Performed{EffectID: effect.EffectID},
		Facts{Outcome: done, ContextState: &ContextState{FirstStep: 1}}); err != nil {
		t.Fatalf("a performed handover: %v", err)
	}
}
