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
	failAt     core.Phase
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
	r.commits = append(r.commits, transition)
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

func stateAt(t *testing.T, phase core.Phase) core.State {
	t.Helper()
	state, err := core.NewState("run-1", core.Limits{MaxSteps: 3, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	state.Phase = phase
	return state
}

func advance(t *testing.T, state core.State, events ...core.Event) core.State {
	t.Helper()
	for _, event := range events {
		next, err := state.Next(event)
		if err != nil {
			t.Fatalf("Next(%T) error = %v", event, err)
		}
		state = next
	}
	return state
}

func effectSaved(t *testing.T) core.State {
	t.Helper()
	return advance(t, stateAt(t, core.PhaseIdle), core.RequestFrozen{}, core.AttemptStarted{Attempt: 1}, core.ResponseSaved{Attempt: 1}, core.ResponseLoaded{Attempt: 1, Body: execBody})
}

func jobBound(t *testing.T, state core.State, jobID string) core.State {
	t.Helper()
	return advance(t, state, core.JobBound{EffectID: state.EffectID, JobID: jobID})
}

func jobStarted(t *testing.T, state core.State, jobID string) core.State {
	t.Helper()
	bound := jobBound(t, state, jobID)
	return advance(t, bound, core.JobStarted{EffectID: bound.EffectID, JobID: jobID})
}

func TestNewRunnerRequiresDependencies(t *testing.T) {
	rec := &recorder{}
	if _, err := NewRunner(nil, rec); err == nil {
		t.Error("NewRunner(nil store) succeeded, want error")
	}
	if _, err := NewRunner(rec, nil); err == nil {
		t.Error("NewRunner(nil executor) succeeded, want error")
	}
}

func TestCommitRecordsTransitionWithFacts(t *testing.T) {
	rec := &recorder{}
	runner := newRunner(t, rec)
	from := stateAt(t, core.PhaseIdle)
	request := Request{Body: []byte("frozen"), Model: "m", Protocol: "p", Adapter: "a"}

	got, err := runner.Commit(context.Background(), from, core.RequestFrozen{}, Facts{Request: &request})
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if got.Phase != core.PhaseRequestFrozen {
		t.Fatalf("Commit() phase = %q, want request_frozen", got.Phase)
	}
	want := Transition{From: from, Event: core.RequestFrozen{}, To: got, Facts: Facts{Request: &request}}
	if len(rec.commits) != 1 || !reflect.DeepEqual(rec.commits[0], want) {
		t.Fatalf("commits = %#v, want [%#v]", rec.commits, want)
	}
}

func TestCommitWritesSelfLoopTransition(t *testing.T) {
	rec := &recorder{}
	saved := effectSaved(t)
	from := advance(t, saved, core.StopRequested{Reason: core.StopCancelled}, core.OutcomeUnknown{EffectID: saved.EffectID})

	got, err := newRunner(t, rec).Commit(context.Background(), from, core.StopRequested{Reason: core.StopTimedOut}, Facts{})
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if got != from || len(rec.commits) != 1 || rec.commits[0].From != rec.commits[0].To {
		t.Fatalf("Commit() = %#v, commits %#v; want the unchanged state recorded once", got, rec.commits)
	}
}

func TestCommitRequiresFactsForEvent(t *testing.T) {
	saved := effectSaved(t)
	awaiting := jobStarted(t, saved, "job-1")
	if awaiting.Phase != core.PhaseAwaitingObservation {
		t.Fatalf("fixture phase = %q, want awaiting_observation", awaiting.Phase)
	}
	observation := Observation{Execution: core.ExecutionExited}
	tests := []struct {
		name  string
		from  core.State
		event core.Event
		facts Facts
	}{
		{"frozen request without bytes", stateAt(t, core.PhaseIdle), core.RequestFrozen{}, Facts{}},
		{"frozen request with empty body", stateAt(t, core.PhaseIdle), core.RequestFrozen{}, Facts{Request: &Request{}}},
		{"frozen request with a response", stateAt(t, core.PhaseIdle), core.RequestFrozen{}, Facts{Request: &Request{Body: []byte("x")}, Response: &Response{}}},
		{"attempt with facts", stateAt(t, core.PhaseRequestFrozen), core.AttemptStarted{Attempt: 1}, Facts{Failure: "x"}},
		{"saved response without record", stateAt(t, core.PhaseRequestSent), core.ResponseSaved{Attempt: 1}, Facts{}},
		{"saved response incomplete", stateAt(t, core.PhaseRequestSent), core.ResponseSaved{Attempt: 1}, Facts{Response: &Response{Body: execBody}}},
		{"saved response without body", stateAt(t, core.PhaseRequestSent), core.ResponseSaved{Attempt: 1}, Facts{Response: &Response{Complete: true}}},
		{"incomplete response marked complete", stateAt(t, core.PhaseRequestSent), core.ResponseIncomplete{Attempt: 1}, Facts{Response: &Response{Complete: true}}},
		{"job bound with facts", saved, core.JobBound{EffectID: saved.EffectID, JobID: "job-1"}, Facts{Failure: "x"}},
		{"job started with facts", jobBound(t, saved, "job-1"), core.JobStarted{EffectID: saved.EffectID, JobID: "job-1"}, Facts{Failure: "x"}},
		{"observation without record", awaiting, core.Observed{EffectID: awaiting.EffectID, Execution: core.ExecutionExited}, Facts{}},
		{"observation mismatch", awaiting, core.Observed{EffectID: awaiting.EffectID, Execution: core.ExecutionExited}, Facts{Observation: &Observation{Execution: core.ExecutionTimedOut}}},
		{"observation with failure text", awaiting, core.Observed{EffectID: awaiting.EffectID, Execution: core.ExecutionExited}, Facts{Observation: &observation, Failure: "x"}},
		{"resolution without record", advance(t, awaiting, core.OutcomeUnknown{EffectID: awaiting.EffectID}), core.Resolved{EffectID: awaiting.EffectID, Execution: core.ExecutionExited}, Facts{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			got, err := newRunner(t, rec).Commit(context.Background(), tt.from, tt.event, tt.facts)
			if !errors.Is(err, ErrFacts) {
				t.Fatalf("error = %v, want facts error", err)
			}
			if got != tt.from || len(rec.log) != 0 {
				t.Fatalf("rejected commit changed state to %#v or wrote %v", got, rec.log)
			}
		})
	}
}

func TestCommitReportsCoreErrorsBeforeFacts(t *testing.T) {
	tests := []struct {
		name  string
		from  core.State
		event core.Event
		facts Facts
		want  error
	}{
		{"invalid event with permitted facts", stateAt(t, core.PhaseIdle), &core.RequestFrozen{}, Facts{Request: &Request{Body: []byte("x")}}, core.ErrInvalidEvent},
		{"illegal transition with wrong facts", stateAt(t, core.PhaseStopped), core.RequestFrozen{}, Facts{}, core.ErrIllegalTransition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			got, err := newRunner(t, rec).Commit(context.Background(), tt.from, tt.event, tt.facts)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if got != tt.from || len(rec.log) != 0 {
				t.Fatalf("rejected commit changed state to %#v or wrote %v", got, rec.log)
			}
		})
	}
}

func TestCommitAcceptsPermittedFacts(t *testing.T) {
	saved := effectSaved(t)
	unresolved := advance(t, jobStarted(t, saved, "job-1"), core.OutcomeUnknown{EffectID: saved.EffectID})
	tests := []struct {
		name  string
		from  core.State
		event core.Event
		facts Facts
	}{
		{"incomplete response with record and failure", stateAt(t, core.PhaseRequestSent), core.ResponseIncomplete{Attempt: 1}, Facts{Response: &Response{Body: []byte("partial")}, Failure: "timeout"}},
		{"incomplete response without facts", stateAt(t, core.PhaseRequestSent), core.ResponseIncomplete{Attempt: 1}, Facts{}},
		{"unknown outcome with failure", saved, core.OutcomeUnknown{EffectID: saved.EffectID}, Facts{Failure: "lost"}},
		{"resolution with record", unresolved, core.Resolved{EffectID: unresolved.EffectID, Execution: core.ExecutionExited}, Facts{Observation: &Observation{Execution: core.ExecutionExited}}},
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

func TestCommitStopsAtStoreFailure(t *testing.T) {
	rec := &recorder{failAt: core.PhaseRequestFrozen}
	from := stateAt(t, core.PhaseIdle)
	got, err := newRunner(t, rec).Commit(context.Background(), from, core.RequestFrozen{}, Facts{Request: &Request{Body: []byte("x")}})
	if !errors.Is(err, errInjected) {
		t.Fatalf("error = %v, want injected failure", err)
	}
	if got != from {
		t.Fatalf("failed commit changed state to %#v", got)
	}
}

func TestCommitHonoursCancellationBeforeWriting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := &recorder{}
	from := stateAt(t, core.PhaseIdle)
	got, err := newRunner(t, rec).Commit(ctx, from, core.RequestFrozen{}, Facts{Request: &Request{Body: []byte("x")}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want cancellation", err)
	}
	if got != from || len(rec.log) != 0 {
		t.Fatalf("cancelled commit changed state to %#v or wrote %v", got, rec.log)
	}
}

func TestBindCommitsEffectBeforeBindingJob(t *testing.T) {
	rec := &recorder{job: Job{ID: "job-1", Status: JobPending}}
	runner := newRunner(t, rec)
	ctx := context.Background()

	saved, err := runner.Commit(ctx, stateAt(t, core.PhaseResponseSaved), core.ResponseLoaded{Attempt: 1, Body: execBody}, Facts{})
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	got, err := runner.Bind(ctx, saved)
	if err != nil {
		t.Fatalf("Bind() error = %v", err)
	}

	if got.Phase != core.PhaseJobBound || got.JobID != "job-1" {
		t.Fatalf("Bind() = %#v, want job_bound job-1", got)
	}
	wantLog := []string{"commit:effect_saved", "bind", "commit:job_bound"}
	if !reflect.DeepEqual(rec.log, wantLog) {
		t.Fatalf("log = %v, want %v", rec.log, wantLog)
	}
	if want := (Effect{ID: saved.EffectID, Command: "printf ok"}); rec.lastEffect != want {
		t.Fatalf("executor received %#v, want %#v", rec.lastEffect, want)
	}
	if last := rec.commits[len(rec.commits)-1]; last.Event != (core.JobBound{EffectID: saved.EffectID, JobID: "job-1"}) || last.From != saved || last.To != got {
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
			if got.Phase != core.PhaseJobBound || got.JobID != "job-1" {
				t.Fatalf("Bind() = %#v, want job_bound job-1", got)
			}
		})
	}
}

func TestBindRequiresBindIntent(t *testing.T) {
	rec := &recorder{}
	from := stateAt(t, core.PhaseIdle)
	got, err := newRunner(t, rec).Bind(context.Background(), from)
	if err == nil {
		t.Fatal("Bind() succeeded outside effect_saved, want error")
	}
	if got != from || len(rec.log) != 0 {
		t.Fatalf("misdirected bind changed state to %#v or wrote %v", got, rec.log)
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
			if got.Phase != core.PhaseNeedsResolution || got.JobID != "" {
				t.Fatalf("Bind() = %#v, want needs_resolution without job", got)
			}
			want := Transition{From: from, Event: core.OutcomeUnknown{EffectID: from.EffectID}, To: got, Facts: Facts{Failure: tt.wantFailure}}
			if len(tt.rec.commits) != 1 || !reflect.DeepEqual(tt.rec.commits[0], want) {
				t.Fatalf("commits = %#v, want [%#v]", tt.rec.commits, want)
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
				t.Fatalf("Bind() = %#v with commits %v; want unchanged state and nothing committed", got, tt.rec.commits)
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
				t.Fatalf("Bind() = %#v, log %v; want unchanged state and no commit", got, tt.rec.log)
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
	if got.Phase != core.PhaseAwaitingObservation || got.JobID != "job-1" {
		t.Fatalf("Start() = %#v, want awaiting_observation job-1", got)
	}
	wantLog := []string{"start", "commit:awaiting_observation"}
	if !reflect.DeepEqual(rec.log, wantLog) {
		t.Fatalf("log = %v, want %v", rec.log, wantLog)
	}
	want := Transition{From: from, Event: core.JobStarted{EffectID: from.EffectID, JobID: "job-1"}, To: got, Facts: Facts{}}
	if len(rec.commits) != 1 || !reflect.DeepEqual(rec.commits[0], want) {
		t.Fatalf("commits = %#v, want [%#v]", rec.commits, want)
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
		t.Fatalf("misdirected start changed state to %#v or wrote %v", got, rec.log)
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
			if got.Phase != core.PhaseNeedsResolution || got.JobID != "job-1" {
				t.Fatalf("Start() = %#v, want needs_resolution with the bound job", got)
			}
			want := Transition{From: from, Event: core.OutcomeUnknown{EffectID: from.EffectID}, To: got, Facts: Facts{Failure: tt.wantFailure}}
			if len(tt.rec.commits) != 1 || !reflect.DeepEqual(tt.rec.commits[0], want) {
				t.Fatalf("commits = %#v, want [%#v]", tt.rec.commits, want)
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
				t.Fatalf("Start() = %#v, log %v; want unchanged state and no commit", got, tt.rec.log)
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
		t.Fatalf("Start() = %#v with commits %v; want unchanged state and nothing committed", got, rec.commits)
	}
}

func TestOfflineRunReachesReturn(t *testing.T) {
	rec := &recorder{job: Job{ID: "job-1", Status: JobPending}, startJob: Job{ID: "job-1", Status: JobRunning}}
	runner := newRunner(t, rec)
	ctx := context.Background()
	request := Request{Body: []byte("frozen")}
	observation := Observation{Execution: core.ExecutionExited}

	commit := func(event func(core.State) core.Event, facts Facts) func(core.State) (core.State, error) {
		return func(s core.State) (core.State, error) { return runner.Commit(ctx, s, event(s), facts) }
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
	state := stateAt(t, core.PhaseIdle)
	steps := []func(core.State) (core.State, error){
		commit(frozen, Facts{Request: &request}),
		commit(attempt, Facts{}),
		commit(saved, Facts{Response: &Response{Body: execBody, Complete: true}}),
		commit(loaded(execBody), Facts{}),
		func(s core.State) (core.State, error) { return runner.Bind(ctx, s) },
		func(s core.State) (core.State, error) { return runner.Start(ctx, s) },
		commit(observed, Facts{Observation: &observation}),
		commit(frozen, Facts{Request: &request}),
		commit(attempt, Facts{}),
		commit(saved, Facts{Response: &Response{Body: returnBody, Complete: true}}),
		commit(loaded(returnBody), Facts{}),
	}
	for i, step := range steps {
		next, err := step(state)
		if err != nil {
			t.Fatalf("step %d error = %v", i, err)
		}
		state = next
	}

	var phases []core.Phase
	for _, commit := range rec.commits {
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
	if state.Intent() != core.IntentHalt || state.Step != 2 || state.Decision.Payload != "done" {
		t.Fatalf("final state = %#v, want halted return at step 2", state)
	}
}
