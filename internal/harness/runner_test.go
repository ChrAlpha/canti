package harness

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

var (
	errInjected = errors.New("injected boundary failure")
	errExecutor = errors.New("executor failure")
	execBody    = []byte(`{"kind":"exec","payload":"printf ok"}`)
	returnBody  = []byte(`{"kind":"return","payload":"done"}`)
)

type recorder struct {
	log        []string
	commits    []Transition
	bySeq      map[int64]Transition
	seeded     bool
	lastSeq    int64
	failAt     core.Phase
	loseAckAt  core.Phase
	job        Job
	jobErr     error
	startJob   Job
	startErr   error
	onBind     func()
	onStart    func()
	lastEffect Effect
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

func (r *recorder) Bind(_ context.Context, effect Effect) (Job, error) {
	r.log = append(r.log, "bind")
	r.lastEffect = effect
	if r.onBind != nil {
		r.onBind()
	}
	return r.job, r.jobErr
}

func (r *recorder) Start(context.Context, string) (Job, error) {
	r.log = append(r.log, "start")
	if r.onStart != nil {
		r.onStart()
	}
	return r.startJob, r.startErr
}

func (r *recorder) Inspect(context.Context, string) (Job, bool, error) {
	r.log = append(r.log, "inspect")
	return Job{}, false, nil
}

func (r *recorder) Cancel(context.Context, string) error {
	r.log = append(r.log, "cancel")
	return nil
}

func newRunner(t *testing.T, rec *recorder) Runner {
	t.Helper()
	runner, err := NewRunner(rec, rec)
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
		return advance(t, snap, core.RequestFrozen{}, core.AttemptStarted{Attempt: 1})
	case core.PhaseResponseSaved:
		return advance(t, snap, core.RequestFrozen{}, core.AttemptStarted{Attempt: 1}, core.ResponseSaved{Attempt: 1})
	case core.PhaseStopped:
		return advance(t, snap, core.StopRequested{Reason: core.StopCancelled})
	}
	t.Fatalf("snapAt(%q) is not supported", phase)
	return Snapshot{}
}

func effectSaved(t *testing.T) Snapshot {
	t.Helper()
	return advance(t, snapAt(t, core.PhaseResponseSaved), core.ResponseLoaded{Attempt: 1, Body: execBody})
}

func jobBound(t *testing.T, snap Snapshot, jobID string) Snapshot {
	t.Helper()
	return advance(t, snap, core.JobBound{EffectID: snap.State.EffectID, JobID: jobID})
}

func jobStarted(t *testing.T, snap Snapshot, jobID string) Snapshot {
	t.Helper()
	bound := jobBound(t, snap, jobID)
	return advance(t, bound, core.JobStarted{EffectID: bound.State.EffectID, JobID: jobID})
}

func TestNewRunnerRequiresDependencies(t *testing.T) {
	rec := &recorder{}
	if _, err := NewRunner(nil, rec); err == nil {
		t.Error("NewRunner(nil store) succeeded, want error")
	}
	if _, err := NewRunner(rec, nil); err == nil {
		t.Error("NewRunner(rec, nil) succeeded, want error")
	}
}

func TestCommitRecordsTransitionWithFacts(t *testing.T) {
	rec := &recorder{}
	runner := newRunner(t, rec)
	from := snapAt(t, core.PhaseIdle)
	request := Request{Body: []byte("frozen"), Model: "m", Protocol: "p", Adapter: "a"}

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
	facts := Facts{Request: &Request{Body: []byte("x")}}

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
	if _, err := newRunner(t, recA).Commit(ctx, a, core.AttemptStarted{Attempt: 1}, Facts{}); err != nil {
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
	facts := Facts{Request: &Request{Body: []byte("x")}}

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

func TestCommitRequiresFactsForEvent(t *testing.T) {
	saved := effectSaved(t)
	awaiting := jobStarted(t, saved, "job-1")
	if awaiting.State.Phase != core.PhaseAwaitingObservation {
		t.Fatalf("fixture phase = %q, want awaiting_observation", awaiting.State.Phase)
	}
	observation := Observation{Execution: core.ExecutionExited}
	tests := []struct {
		name  string
		from  Snapshot
		event core.Event
		facts Facts
	}{
		{"frozen request without bytes", snapAt(t, core.PhaseIdle), core.RequestFrozen{}, Facts{}},
		{"frozen request with empty body", snapAt(t, core.PhaseIdle), core.RequestFrozen{}, Facts{Request: &Request{}}},
		{"frozen request with a response", snapAt(t, core.PhaseIdle), core.RequestFrozen{}, Facts{Request: &Request{Body: []byte("x")}, Response: &Response{}}},
		{"attempt with facts", snapAt(t, core.PhaseRequestFrozen), core.AttemptStarted{Attempt: 1}, Facts{Failure: "x"}},
		{"saved response without record", snapAt(t, core.PhaseRequestSent), core.ResponseSaved{Attempt: 1}, Facts{}},
		{"saved response incomplete", snapAt(t, core.PhaseRequestSent), core.ResponseSaved{Attempt: 1}, Facts{Response: &Response{Body: execBody}}},
		{"saved response without body", snapAt(t, core.PhaseRequestSent), core.ResponseSaved{Attempt: 1}, Facts{Response: &Response{Complete: true}}},
		{"incomplete response marked complete", snapAt(t, core.PhaseRequestSent), core.ResponseIncomplete{Attempt: 1}, Facts{Response: &Response{Complete: true}}},
		{"job bound with facts", saved, core.JobBound{EffectID: saved.State.EffectID, JobID: "job-1"}, Facts{Failure: "x"}},
		{"job started with facts", jobBound(t, saved, "job-1"), core.JobStarted{EffectID: saved.State.EffectID, JobID: "job-1"}, Facts{Failure: "x"}},
		{"observation without record", awaiting, core.Observed{EffectID: awaiting.State.EffectID, Execution: core.ExecutionExited}, Facts{}},
		{"observation mismatch", awaiting, core.Observed{EffectID: awaiting.State.EffectID, Execution: core.ExecutionExited}, Facts{Observation: &Observation{Execution: core.ExecutionTimedOut}}},
		{"observation with failure text", awaiting, core.Observed{EffectID: awaiting.State.EffectID, Execution: core.ExecutionExited}, Facts{Observation: &observation, Failure: "x"}},
		{"resolution without record", advance(t, awaiting, core.OutcomeUnknown{EffectID: awaiting.State.EffectID}), core.Resolved{EffectID: awaiting.State.EffectID, Execution: core.ExecutionExited}, Facts{}},
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
		{"invalid event with permitted facts", snapAt(t, core.PhaseIdle), &core.RequestFrozen{}, Facts{Request: &Request{Body: []byte("x")}}, core.ErrInvalidEvent},
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
	unresolved := advance(t, jobStarted(t, saved, "job-1"), core.OutcomeUnknown{EffectID: saved.State.EffectID})
	tests := []struct {
		name  string
		from  Snapshot
		event core.Event
		facts Facts
	}{
		{"incomplete response with record and failure", snapAt(t, core.PhaseRequestSent), core.ResponseIncomplete{Attempt: 1}, Facts{Response: &Response{Body: []byte("partial")}, Failure: "timeout"}},
		{"incomplete response without facts", snapAt(t, core.PhaseRequestSent), core.ResponseIncomplete{Attempt: 1}, Facts{}},
		{"unknown outcome with failure", saved, core.OutcomeUnknown{EffectID: saved.State.EffectID}, Facts{Failure: "lost"}},
		{"resolution with record", unresolved, core.Resolved{EffectID: unresolved.State.EffectID, Execution: core.ExecutionExited}, Facts{Observation: &Observation{Execution: core.ExecutionExited}}},
		{"abandonment with unknown record", unresolved, core.Resolved{EffectID: unresolved.State.EffectID, Execution: core.ExecutionUnknown}, Facts{Observation: &Observation{Execution: core.ExecutionUnknown}}},
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
			return r.Commit(context.Background(), s, core.RequestFrozen{}, Facts{Request: &Request{Body: []byte("x")}})
		}, Snapshot{State: core.State{Phase: core.PhaseIdle}}},
		{"bind", func(r Runner, s Snapshot) (Snapshot, error) {
			return r.Bind(context.Background(), s)
		}, Snapshot{State: core.State{Phase: core.PhaseEffectSaved}}},
		{"start", func(r Runner, s Snapshot) (Snapshot, error) {
			return r.Start(context.Background(), s)
		}, Snapshot{State: core.State{Phase: core.PhaseJobBound}}},
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
		got, err := newRunner(t, rec).Commit(context.Background(), from, core.RequestFrozen{}, Facts{Request: &Request{Body: []byte("x")}})
		if err == nil || len(rec.log) != 0 || got != from {
			t.Fatalf("negative sequence reached I/O: %#v, %v, log %v", got, err, rec.log)
		}
	})
}

func TestCommitStopsAtStoreFailure(t *testing.T) {
	rec := &recorder{failAt: core.PhaseRequestFrozen}
	from := snapAt(t, core.PhaseIdle)
	got, err := newRunner(t, rec).Commit(context.Background(), from, core.RequestFrozen{}, Facts{Request: &Request{Body: []byte("x")}})
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
	got, err := newRunner(t, rec).Commit(ctx, from, core.RequestFrozen{}, Facts{Request: &Request{Body: []byte("x")}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want cancellation", err)
	}
	if got != from || len(rec.log) != 0 {
		t.Fatalf("cancelled commit changed snapshot to %#v or wrote %v", got, rec.log)
	}
}

func TestBindCommitsEffectBeforeBindingJob(t *testing.T) {
	rec := &recorder{job: Job{ID: "job-1", Status: JobPending}}
	runner := newRunner(t, rec)
	ctx := context.Background()

	saved, err := runner.Commit(ctx, snapAt(t, core.PhaseResponseSaved), core.ResponseLoaded{Attempt: 1, Body: execBody}, Facts{})
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	got, err := runner.Bind(ctx, saved)
	if err != nil {
		t.Fatalf("Bind() error = %v", err)
	}

	if got.State.Phase != core.PhaseJobBound || got.State.JobID != "job-1" {
		t.Fatalf("Bind() = %#v, want job_bound job-1", got)
	}
	wantLog := []string{"commit:effect_saved", "bind", "commit:job_bound"}
	if !reflect.DeepEqual(rec.log, wantLog) {
		t.Fatalf("log = %v, want %v", rec.log, wantLog)
	}
	if want := (Effect{ID: saved.State.EffectID, Command: "printf ok"}); rec.lastEffect != want {
		t.Fatalf("executor received %#v, want %#v", rec.lastEffect, want)
	}
	if last := rec.commits[len(rec.commits)-1]; last.Event != (core.JobBound{EffectID: saved.State.EffectID, JobID: "job-1"}) || last.From != saved.State || last.To != got.State {
		t.Fatalf("bind commit = %#v", last)
	}
}

func TestBindKeepsJobIdentityWhateverItsStatus(t *testing.T) {
	tests := []struct {
		name   string
		status JobStatus
	}{
		{"pending", JobPending},
		{"running", JobRunning},
		{"succeeded", JobSucceeded},
		{"failed", JobFailed},
		{"unknown", JobUnknown},
		{"empty", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{job: Job{ID: "job-1", Status: tt.status}}
			got, err := newRunner(t, rec).Bind(context.Background(), effectSaved(t))
			if err != nil {
				t.Fatalf("Bind() error = %v", err)
			}
			if got.State.Phase != core.PhaseJobBound || got.State.JobID != "job-1" {
				t.Fatalf("Bind() = %#v, want job_bound job-1", got)
			}
		})
	}
}

func TestBindRequiresBindIntent(t *testing.T) {
	rec := &recorder{}
	from := snapAt(t, core.PhaseIdle)
	got, err := newRunner(t, rec).Bind(context.Background(), from)
	if err == nil {
		t.Fatal("Bind() succeeded outside effect_saved, want error")
	}
	if got != from || len(rec.log) != 0 {
		t.Fatalf("misdirected bind changed snapshot to %#v or wrote %v", got, rec.log)
	}
}

func TestBindKeepsUnknownOutcome(t *testing.T) {
	tests := []struct {
		name        string
		rec         *recorder
		wantFailure string
	}{
		{"executor error", &recorder{jobErr: errExecutor}, errExecutor.Error()},
		{"missing job ID", &recorder{job: Job{Status: JobPending}}, `executor returned no job ID with status "pending"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from := effectSaved(t)
			got, err := newRunner(t, tt.rec).Bind(context.Background(), from)
			if err != nil {
				t.Fatalf("Bind() error = %v", err)
			}
			if got.State.Phase != core.PhaseNeedsResolution || got.State.JobID != "" {
				t.Fatalf("Bind() = %#v, want needs_resolution without job", got)
			}
			c := tt.rec.commits
			if len(c) != 1 || c[0].Event != (core.OutcomeUnknown{EffectID: from.State.EffectID}) || c[0].Facts != (Facts{Failure: tt.wantFailure}) {
				t.Fatalf("commits = %#v", c)
			}
		})
	}
}

func TestBindStopsAtStoreFailureAfterBind(t *testing.T) {
	tests := []struct {
		name         string
		rec          *recorder
		wantLog      []string
		wantExecutor bool
		wantText     string
	}{
		{"bind commit fails", &recorder{job: Job{ID: "job-1", Status: JobPending}, failAt: core.PhaseJobBound}, []string{"bind", "commit:job_bound"}, false, ""},
		{"unknown outcome commit fails", &recorder{jobErr: errExecutor, failAt: core.PhaseNeedsResolution}, []string{"bind", "commit:needs_resolution"}, true, errExecutor.Error()},
		{"missing job ID commit fails", &recorder{job: Job{Status: JobPending}, failAt: core.PhaseNeedsResolution}, []string{"bind", "commit:needs_resolution"}, false, "no job ID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from := effectSaved(t)
			got, err := newRunner(t, tt.rec).Bind(context.Background(), from)
			if !errors.Is(err, errInjected) {
				t.Fatalf("error = %v, want injected store failure", err)
			}
			if errors.Is(err, errExecutor) != tt.wantExecutor {
				t.Fatalf("error = %v, want executor failure in chain: %v", err, tt.wantExecutor)
			}
			if tt.wantText != "" && !strings.Contains(err.Error(), tt.wantText) {
				t.Fatalf("error %q does not mention %q", err, tt.wantText)
			}
			if got != from || len(tt.rec.commits) != 0 {
				t.Fatalf("Bind() = %#v with commits %v; want unchanged snapshot and nothing committed", got, tt.rec.commits)
			}
			if !reflect.DeepEqual(tt.rec.log, tt.wantLog) {
				t.Fatalf("log = %v, want %v", tt.rec.log, tt.wantLog)
			}
		})
	}
}

func TestBindHonoursCancellation(t *testing.T) {
	t.Run("before bind", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		rec := &recorder{}
		from := effectSaved(t)
		got, err := newRunner(t, rec).Bind(ctx, from)
		if !errors.Is(err, context.Canceled) || got != from || len(rec.log) != 0 {
			t.Fatalf("Bind() = %#v, %v, log %v; want untouched cancellation", got, err, rec.log)
		}
	})

	for _, tt := range []struct {
		name string
		rec  *recorder
	}{
		{"bind fails after cancellation", &recorder{jobErr: context.Canceled}},
		{"job binds as cancellation arrives", &recorder{job: Job{ID: "job-1", Status: JobPending}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tt.rec.onBind = cancel
			from := effectSaved(t)
			got, err := newRunner(t, tt.rec).Bind(ctx, from)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want cancellation", err)
			}
			if got != from || !reflect.DeepEqual(tt.rec.log, []string{"bind"}) {
				t.Fatalf("Bind() = %#v, log %v; want unchanged snapshot and no commit", got, tt.rec.log)
			}
		})
	}
}

func TestStartCommitsIssuedStart(t *testing.T) {
	rec := &recorder{startJob: Job{ID: "job-1", Status: JobRunning}}
	from := jobBound(t, effectSaved(t), "job-1")
	got, err := newRunner(t, rec).Start(context.Background(), from)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if got.State.Phase != core.PhaseAwaitingObservation || got.State.JobID != "job-1" {
		t.Fatalf("Start() = %#v, want awaiting_observation job-1", got)
	}
	wantLog := []string{"start", "commit:awaiting_observation"}
	if !reflect.DeepEqual(rec.log, wantLog) {
		t.Fatalf("log = %v, want %v", rec.log, wantLog)
	}
	c := rec.commits
	if len(c) != 1 || c[0].Event != (core.JobStarted{EffectID: from.State.EffectID, JobID: "job-1"}) || c[0].From != from.State || c[0].To != got.State {
		t.Fatalf("commits = %#v", c)
	}
}

func TestStartRequiresStartIntent(t *testing.T) {
	rec := &recorder{}
	from := effectSaved(t)
	got, err := newRunner(t, rec).Start(context.Background(), from)
	if err == nil {
		t.Fatal("Start() succeeded outside job_bound, want error")
	}
	if got != from || len(rec.log) != 0 {
		t.Fatalf("misdirected start changed snapshot to %#v or wrote %v", got, rec.log)
	}
}

func TestStartKeepsUnknownOutcome(t *testing.T) {
	tests := []struct {
		name        string
		rec         *recorder
		wantFailure string
	}{
		{"executor error", &recorder{startErr: errExecutor}, errExecutor.Error()},
		{"foreign job started", &recorder{startJob: Job{ID: "job-2", Status: JobRunning}}, `executor started job "job-2" instead of bound job "job-1"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from := jobBound(t, effectSaved(t), "job-1")
			got, err := newRunner(t, tt.rec).Start(context.Background(), from)
			if err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			if got.State.Phase != core.PhaseNeedsResolution || got.State.JobID != "job-1" {
				t.Fatalf("Start() = %#v, want needs_resolution with the bound job", got)
			}
			c := tt.rec.commits
			if len(c) != 1 || c[0].Event != (core.OutcomeUnknown{EffectID: from.State.EffectID}) || c[0].Facts != (Facts{Failure: tt.wantFailure}) {
				t.Fatalf("commits = %#v", c)
			}
		})
	}
}

func TestStartHonoursCancellation(t *testing.T) {
	t.Run("before start", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		rec := &recorder{}
		from := jobBound(t, effectSaved(t), "job-1")
		got, err := newRunner(t, rec).Start(ctx, from)
		if !errors.Is(err, context.Canceled) || got != from || len(rec.log) != 0 {
			t.Fatalf("Start() = %#v, %v, log %v; want untouched cancellation", got, err, rec.log)
		}
	})

	for _, tt := range []struct {
		name string
		rec  *recorder
	}{
		{"start fails after cancellation", &recorder{startErr: context.Canceled}},
		{"job starts as cancellation arrives", &recorder{startJob: Job{ID: "job-1", Status: JobRunning}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tt.rec.onStart = cancel
			from := jobBound(t, effectSaved(t), "job-1")
			got, err := newRunner(t, tt.rec).Start(ctx, from)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want cancellation", err)
			}
			if got != from || !reflect.DeepEqual(tt.rec.log, []string{"start"}) {
				t.Fatalf("Start() = %#v, log %v; want unchanged snapshot and no commit", got, tt.rec.log)
			}
		})
	}
}

func TestStartStopsAtStoreFailure(t *testing.T) {
	rec := &recorder{startJob: Job{ID: "job-1", Status: JobRunning}, failAt: core.PhaseAwaitingObservation}
	from := jobBound(t, effectSaved(t), "job-1")
	got, err := newRunner(t, rec).Start(context.Background(), from)
	if !errors.Is(err, errInjected) {
		t.Fatalf("error = %v, want injected store failure", err)
	}
	if got != from || len(rec.commits) != 0 {
		t.Fatalf("Start() = %#v with commits %v; want unchanged snapshot and nothing committed", got, rec.commits)
	}
}

func TestOfflineRunReachesReturn(t *testing.T) {
	rec := &recorder{job: Job{ID: "job-1", Status: JobPending}, startJob: Job{ID: "job-1", Status: JobRunning}}
	runner := newRunner(t, rec)
	ctx := context.Background()
	request := Request{Body: []byte("frozen")}
	observation := Observation{Execution: core.ExecutionExited}

	commit := func(event func(core.State) core.Event, facts Facts) func(Snapshot) (Snapshot, error) {
		return func(s Snapshot) (Snapshot, error) { return runner.Commit(ctx, s, event(s.State), facts) }
	}
	frozen := func(core.State) core.Event { return core.RequestFrozen{} }
	attempt := func(s core.State) core.Event { return core.AttemptStarted{Attempt: s.Attempt} }
	saved := func(s core.State) core.Event { return core.ResponseSaved{Attempt: s.Attempt} }
	loaded := func(body []byte) func(core.State) core.Event {
		return func(s core.State) core.Event { return core.ResponseLoaded{Attempt: s.Attempt, Body: body} }
	}
	observed := func(s core.State) core.Event {
		return core.Observed{EffectID: s.EffectID, Execution: core.ExecutionExited}
	}
	snap := snapAt(t, core.PhaseIdle)
	steps := []func(Snapshot) (Snapshot, error){
		commit(frozen, Facts{Request: &request}),
		commit(attempt, Facts{}),
		commit(saved, Facts{Response: &Response{Body: execBody, Complete: true}}),
		commit(loaded(execBody), Facts{}),
		func(s Snapshot) (Snapshot, error) { return runner.Bind(ctx, s) },
		func(s Snapshot) (Snapshot, error) { return runner.Start(ctx, s) },
		commit(observed, Facts{Observation: &observation}),
		commit(frozen, Facts{Request: &request}),
		commit(attempt, Facts{}),
		commit(saved, Facts{Response: &Response{Body: returnBody, Complete: true}}),
		commit(loaded(returnBody), Facts{}),
	}
	for i, step := range steps {
		next, err := step(snap)
		if err != nil {
			t.Fatalf("step %d error = %v", i, err)
		}
		snap = next
	}

	var phases []core.Phase
	for i, commit := range rec.commits {
		if commit.Seq != int64(i)+1 {
			t.Fatalf("commit %d has seq %d, want %d", i, commit.Seq, i+1)
		}
		phases = append(phases, commit.To.Phase)
	}
	want := []core.Phase{
		core.PhaseRequestFrozen, core.PhaseRequestSent, core.PhaseResponseSaved, core.PhaseEffectSaved,
		core.PhaseJobBound, core.PhaseAwaitingObservation,
		core.PhaseIdle, core.PhaseRequestFrozen, core.PhaseRequestSent, core.PhaseResponseSaved, core.PhaseReturned,
	}
	if !reflect.DeepEqual(phases, want) {
		t.Fatalf("committed phases = %v, want %v", phases, want)
	}
	if snap.State.Intent() != core.IntentHalt || snap.State.Step != 2 || snap.State.Decision.Payload != "done" {
		t.Fatalf("final state = %#v, want halted return at step 2", snap.State)
	}
}
