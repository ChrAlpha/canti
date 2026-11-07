package core_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

var (
	execBody    = []byte(`{"kind":"exec","payload":"printf ok"}`)
	returnBody  = []byte(`{"kind":"return","payload":"done"}`)
	invalidBody = []byte(`{"kind":"wait","payload":"x"}`)
)

const legalPairs = 29

func at(phase core.Phase, edits ...func(*core.State)) core.State {
	s := core.State{
		RunID:   "run-1",
		Step:    1,
		Attempt: 1,
		Phase:   phase,
		Limits:  core.Limits{MaxSteps: 3, MaxAttempts: 2},
	}
	for _, edit := range edits {
		edit(&s)
	}
	return s
}

func withExec(s *core.State) {
	s.Decision = core.Decision{Kind: core.Exec, Payload: "printf ok"}
	s.EffectID = "effect-1"
}

func withJob(s *core.State) { s.JobID = "job-1" }

func withStarted(s *core.State) { s.Started = true }

func withReason(reason core.StopReason) func(*core.State) {
	return func(s *core.State) { s.Reason = reason }
}

func withStep(step int) func(*core.State) {
	return func(s *core.State) { s.Step = step }
}

func withAttempt(attempt int) func(*core.State) {
	return func(s *core.State) { s.Attempt = attempt }
}

func withRev(rev int) func(*core.State) {
	return func(s *core.State) { s.RequestRev = rev }
}

func TestNewState(t *testing.T) {
	t.Parallel()

	got, err := core.NewState("run-1", core.Limits{MaxSteps: 3, MaxAttempts: 2})
	if err != nil {
		t.Fatalf("NewState() error = %v", err)
	}
	if want := at(core.PhaseIdle); got != want {
		t.Fatalf("NewState() = %#v, want %#v", got, want)
	}

	invalid := []struct {
		name   string
		runID  string
		limits core.Limits
	}{
		{name: "missing run", runID: "", limits: core.Limits{MaxSteps: 1, MaxAttempts: 1}},
		{name: "negative steps", runID: "run-1", limits: core.Limits{MaxSteps: -1, MaxAttempts: 1}},
		{name: "zero attempts", runID: "run-1", limits: core.Limits{MaxSteps: 1, MaxAttempts: 0}},
	}
	for _, tt := range invalid {
		if _, err := core.NewState(tt.runID, tt.limits); err == nil {
			t.Errorf("NewState(%s) succeeded, want error", tt.name)
		}
	}
}

type transition struct {
	name  string
	from  core.State
	event core.Event
	want  core.State
}

func eventSamples() []struct {
	event core.Event
	kind  string
} {
	return []struct {
		event core.Event
		kind  string
	}{
		{core.RequestFrozen{}, "request_frozen"},
		{core.AttemptStarted{Step: 1, Attempt: 1}, "attempt_started"},
		{core.ResponseSaved{Step: 1, Attempt: 1}, "response_saved"},
		{core.ResponseIncomplete{Step: 1, Attempt: 1}, "response_incomplete"},
		{core.ResponseLoaded{Step: 1, Attempt: 1, Body: execBody}, "response_loaded"},
		{core.JobBound{EffectID: "effect-1", JobID: "job-1"}, "job_bound"},
		{core.JobStarted{EffectID: "effect-1", JobID: "job-1"}, "job_started"},
		{core.Observed{EffectID: "effect-1", Execution: core.ExecutionFailedToStart}, "observed"},
		{core.Performed{EffectID: "effect-1"}, "performed"},
		{core.OutcomeUnknown{EffectID: "effect-1"}, "outcome_unknown"},
		{core.StopRequested{Reason: core.StopCancelled}, "stop_requested"},
		{core.Resolved{EffectID: "effect-1", Execution: core.ExecutionExited}, "resolved"},
	}
}

func legalTransitions() []transition {
	return []transition{
		{"idle freezes request", at(core.PhaseIdle), core.RequestFrozen{}, at(core.PhaseRequestFrozen, withRev(1))},
		{"repair freezes a new request", at(core.PhaseIdle, withRev(1), withAttempt(2)), core.RequestFrozen{}, at(core.PhaseRequestFrozen, withRev(2), withAttempt(2))},
		{"idle stops", at(core.PhaseIdle), core.StopRequested{Reason: core.StopCancelled}, at(core.PhaseStopped, withReason(core.StopCancelled))},
		{"idle stops as unrecoverable", at(core.PhaseIdle), core.StopRequested{Reason: core.StopUnrecoverable}, at(core.PhaseStopped, withReason(core.StopUnrecoverable))},

		{"frozen request starts attempt", at(core.PhaseRequestFrozen, withRev(1)), core.AttemptStarted{Step: 1, Attempt: 1}, at(core.PhaseRequestSent, withRev(1))},
		{"frozen request stops", at(core.PhaseRequestFrozen, withRev(1)), core.StopRequested{Reason: core.StopTimedOut}, at(core.PhaseStopped, withRev(1), withReason(core.StopTimedOut))},

		{"sent request counts lost attempt", at(core.PhaseRequestSent, withRev(1)), core.AttemptStarted{Step: 1, Attempt: 2}, at(core.PhaseRequestSent, withRev(1), withAttempt(2))},
		{"sent request saves response", at(core.PhaseRequestSent, withRev(1)), core.ResponseSaved{Step: 1, Attempt: 1}, at(core.PhaseResponseSaved, withRev(1))},
		{"sent request retries incomplete response", at(core.PhaseRequestSent, withRev(1)), core.ResponseIncomplete{Step: 1, Attempt: 1}, at(core.PhaseRequestFrozen, withRev(1), withAttempt(2))},
		{"sent request stops", at(core.PhaseRequestSent, withRev(1)), core.StopRequested{Reason: core.StopTimedOut}, at(core.PhaseStopped, withRev(1), withReason(core.StopTimedOut))},

		{"saved response decodes return", at(core.PhaseResponseSaved, withRev(1)), core.ResponseLoaded{Step: 1, Attempt: 1, Body: returnBody}, at(core.PhaseReturned, withRev(1), func(s *core.State) {
			s.Decision = core.Decision{Kind: core.Return, Payload: "done"}
		})},
		{"saved response repairs invalid decision with a new request", at(core.PhaseResponseSaved, withRev(1)), core.ResponseLoaded{Step: 1, Attempt: 1, Body: invalidBody}, at(core.PhaseIdle, withRev(1), withAttempt(2))},
		{"saved response stops", at(core.PhaseResponseSaved, withRev(1)), core.StopRequested{Reason: core.StopCancelled}, at(core.PhaseStopped, withRev(1), withReason(core.StopCancelled))},

		{"saved effect binds job", at(core.PhaseEffectSaved, withRev(1), withExec), core.JobBound{EffectID: "effect-1", JobID: "job-1"}, at(core.PhaseJobBound, withRev(1), withExec, withJob)},
		{"saved effect observes failed start", at(core.PhaseEffectSaved, withRev(1), withExec), core.Observed{EffectID: "effect-1", Execution: core.ExecutionFailedToStart}, at(core.PhaseIdle, withStep(2))},
		{"saved effect is performed without a job", at(core.PhaseEffectSaved, withRev(1), withExec), core.Performed{EffectID: "effect-1"}, at(core.PhaseIdle, withStep(2))},
		{"saved effect keeps unknown outcome", at(core.PhaseEffectSaved, withRev(1), withExec), core.OutcomeUnknown{EffectID: "effect-1"}, at(core.PhaseNeedsResolution, withRev(1), withExec)},
		{"saved effect cancels", at(core.PhaseEffectSaved, withRev(1), withExec), core.StopRequested{Reason: core.StopCancelled}, at(core.PhaseCancelling, withRev(1), withExec, withReason(core.StopCancelled))},

		{"bound job starts", at(core.PhaseJobBound, withRev(1), withExec, withJob), core.JobStarted{EffectID: "effect-1", JobID: "job-1"}, at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob, withStarted)},
		{"bound job observes result", at(core.PhaseJobBound, withRev(1), withExec, withJob), core.Observed{EffectID: "effect-1", Execution: core.ExecutionExited}, at(core.PhaseIdle, withStep(2))},
		{"bound job keeps unknown outcome", at(core.PhaseJobBound, withRev(1), withExec, withJob), core.OutcomeUnknown{EffectID: "effect-1"}, at(core.PhaseNeedsResolution, withRev(1), withExec, withJob)},
		{"bound job cancels", at(core.PhaseJobBound, withRev(1), withExec, withJob), core.StopRequested{Reason: core.StopCancelled}, at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled))},

		{"awaiting observation advances", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob, withStarted), core.Observed{EffectID: "effect-1", Execution: core.ExecutionExited}, at(core.PhaseIdle, withStep(2))},
		{"awaiting observation keeps unknown outcome", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob, withStarted), core.OutcomeUnknown{EffectID: "effect-1"}, at(core.PhaseNeedsResolution, withRev(1), withExec, withJob, withStarted)},
		{"awaiting observation cancels", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob, withStarted), core.StopRequested{Reason: core.StopBudgetExhausted}, at(core.PhaseCancelling, withRev(1), withExec, withJob, withStarted, withReason(core.StopBudgetExhausted))},

		{"cancelling finds job", at(core.PhaseCancelling, withRev(1), withExec, withReason(core.StopCancelled)), core.JobBound{EffectID: "effect-1", JobID: "job-1"}, at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"cancelling confirms known job", at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.JobBound{EffectID: "effect-1", JobID: "job-1"}, at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"cancelling records missing job as cancelled", at(core.PhaseCancelling, withRev(1), withExec, withReason(core.StopCancelled)), core.Observed{EffectID: "effect-1", Execution: core.ExecutionCancelled}, at(core.PhaseStopped, withRev(1), withExec, withReason(core.StopCancelled))},
		{"cancelling records true result", at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.Observed{EffectID: "effect-1", Execution: core.ExecutionExited}, at(core.PhaseStopped, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"cancelling keeps unknown outcome", at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.OutcomeUnknown{EffectID: "effect-1"}, at(core.PhaseNeedsResolution, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"cancelling keeps first stop reason", at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.StopRequested{Reason: core.StopTimedOut}, at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled))},

		{"resolution records found job for starting", at(core.PhaseNeedsResolution, withRev(1), withExec), core.JobBound{EffectID: "effect-1", JobID: "job-1"}, at(core.PhaseJobBound, withRev(1), withExec, withJob)},
		{"resolution reconfirms known job for starting", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob), core.JobBound{EffectID: "effect-1", JobID: "job-1"}, at(core.PhaseJobBound, withRev(1), withExec, withJob)},
		{"resolution resumes inspection after a committed start", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob, withStarted), core.JobBound{EffectID: "effect-1", JobID: "job-1"}, at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob, withStarted)},
		{"resolution with pending stop records found job", at(core.PhaseNeedsResolution, withRev(1), withExec, withReason(core.StopCancelled)), core.JobBound{EffectID: "effect-1", JobID: "job-1"}, at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"resolution continues", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob), core.Resolved{EffectID: "effect-1", Execution: core.ExecutionFailedToStart}, at(core.PhaseIdle, withStep(2))},
		{"jobless resolution records a failed start", at(core.PhaseNeedsResolution, withRev(1), withExec), core.Resolved{EffectID: "effect-1", Execution: core.ExecutionFailedToStart}, at(core.PhaseIdle, withStep(2))},
		{"resolution completes pending stop", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.Resolved{EffectID: "effect-1", Execution: core.ExecutionExited}, at(core.PhaseStopped, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"resolution abandons with the outcome unknown", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob), core.Resolved{EffectID: "effect-1", Execution: core.ExecutionUnknown}, at(core.PhaseStopped, withRev(1), withExec, withJob, withReason(core.StopUnresolved))},
		{"resolution abandons keeping the pending stop reason", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.Resolved{EffectID: "effect-1", Execution: core.ExecutionUnknown}, at(core.PhaseStopped, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"unresolved run records stop", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob), core.StopRequested{Reason: core.StopCancelled}, at(core.PhaseNeedsResolution, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"unresolved run keeps first stop reason", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.StopRequested{Reason: core.StopTimedOut}, at(core.PhaseNeedsResolution, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
	}
}

func TestNextLegalTransitions(t *testing.T) {
	t.Parallel()

	for _, tt := range legalTransitions() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.from.Validate(); err != nil {
				t.Fatalf("from.Validate() error = %v", err)
			}
			got, err := tt.from.Next(tt.event)
			if err != nil {
				t.Fatalf("Next() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("Next() = %#v, want %#v", got, tt.want)
			}
			if err := got.Validate(); err != nil {
				t.Fatalf("next.Validate() error = %v", err)
			}
		})
	}
}

func TestValidateRejectsIncoherentStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from core.State
	}{
		{"zero state", core.State{}},
		{"bare effect phase", core.State{Phase: core.PhaseEffectSaved}},
		{"missing run", at(core.PhaseIdle, func(s *core.State) { s.RunID = "" })},
		{"unknown phase", at("future_phase")},
		{"zero step", at(core.PhaseIdle, withStep(0))},
		{"step beyond budget", at(core.PhaseIdle, withStep(4))},
		{"attempt beyond budget", at(core.PhaseIdle, withAttempt(3))},
		{"negative request revision", at(core.PhaseIdle, withRev(-1))},
		{"idle with decision", at(core.PhaseIdle, func(s *core.State) { s.Decision = core.Decision{Kind: core.Exec} })},
		{"idle with effect", at(core.PhaseIdle, func(s *core.State) { s.EffectID = "effect-1" })},
		{"idle with job", at(core.PhaseIdle, withJob)},
		{"idle with reason", at(core.PhaseIdle, withReason(core.StopCancelled))},
		{"frozen request without revision", at(core.PhaseRequestFrozen)},
		{"sent request with effect", at(core.PhaseRequestSent, withRev(1), withExec)},
		{"saved effect without decision", at(core.PhaseEffectSaved, withRev(1), func(s *core.State) { s.EffectID = "effect-1" })},
		{"saved effect with return decision", at(core.PhaseEffectSaved, withRev(1), withExec, func(s *core.State) { s.Decision.Kind = core.Return })},
		{"saved effect without effect", at(core.PhaseEffectSaved, withRev(1), func(s *core.State) { s.Decision = core.Decision{Kind: core.Exec} })},
		{"saved effect with job", at(core.PhaseEffectSaved, withRev(1), withExec, withJob)},
		{"saved effect with reason", at(core.PhaseEffectSaved, withRev(1), withExec, withReason(core.StopCancelled))},
		{"bound job without job", at(core.PhaseJobBound, withRev(1), withExec)},
		{"bound job already started", at(core.PhaseJobBound, withRev(1), withExec, withJob, withStarted)},
		{"awaiting observation without job", at(core.PhaseAwaitingObservation, withRev(1), withExec, withStarted)},
		{"awaiting observation without a start", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob)},
		{"saved effect already started", at(core.PhaseEffectSaved, withRev(1), withExec, withStarted)},
		{"idle with start flag", at(core.PhaseIdle, withStarted)},
		{"cancelling without reason", at(core.PhaseCancelling, withRev(1), withExec, withJob)},
		{"resolution without effect", at(core.PhaseNeedsResolution, withRev(1), func(s *core.State) { s.Decision = core.Decision{Kind: core.Exec} })},
		{"returned with exec decision", at(core.PhaseReturned, withRev(1), func(s *core.State) { s.Decision = core.Decision{Kind: core.Exec} })},
		{"returned with effect", at(core.PhaseReturned, withRev(1), withExec, func(s *core.State) { s.Decision.Kind = core.Return })},
		{"returned with reason", at(core.PhaseReturned, withRev(1), withReason(core.StopCancelled), func(s *core.State) { s.Decision = core.Decision{Kind: core.Return} })},
		{"stopped without reason", at(core.PhaseStopped)},
		{"stopped with unknown reason", at(core.PhaseStopped, withReason("lol"))},
		{"stopped with unknown decision kind", at(core.PhaseStopped, withReason(core.StopCancelled), func(s *core.State) { s.Decision.Kind = "bogus" })},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.from.Validate(); !errors.Is(err, core.ErrInvalidState) {
				t.Fatalf("Validate() = %v, want invalid state", err)
			}
		})
	}
}

func TestEventKindsAreStableAndUnique(t *testing.T) {
	t.Parallel()

	seen := make(map[string]bool)
	for _, tt := range eventSamples() {
		if got := tt.event.Kind(); got != tt.kind {
			t.Errorf("%T.Kind() = %q, want %q", tt.event, got, tt.kind)
		}
		if seen[tt.kind] {
			t.Errorf("kind %q is not unique", tt.kind)
		}
		seen[tt.kind] = true
	}
	if core.SemanticsVersion != 4 {
		t.Errorf("SemanticsVersion = %d, want 4", core.SemanticsVersion)
	}
}

func TestNextDerivesEffectFromExecDecision(t *testing.T) {
	t.Parallel()

	from := at(core.PhaseResponseSaved, withRev(1))
	got, err := from.Next(core.ResponseLoaded{Step: 1, Attempt: 1, Body: execBody})
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if got.EffectID == "" {
		t.Fatal("exec decision must derive an effect identity")
	}
	want := at(core.PhaseEffectSaved, withRev(1))
	want.Decision = core.Decision{Kind: core.Exec, Payload: "printf ok"}
	want.EffectID = got.EffectID
	if got != want {
		t.Fatalf("Next() = %#v, want %#v", got, want)
	}
}

func TestNextKeepsTheDecisionPurpose(t *testing.T) {
	t.Parallel()

	from := at(core.PhaseResponseSaved, withRev(1))
	got, err := from.Next(core.ResponseLoaded{Step: 1, Attempt: 1, Body: []byte(`{"kind":"exec","purpose":"list, then read go.mod","payload":"ls"}`)})
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if got.Phase != core.PhaseEffectSaved || got.Decision != (core.Decision{Kind: core.Exec, Purpose: "list, then read go.mod", Payload: "ls"}) {
		t.Fatalf("Next() = %#v, want the exec decision with its purpose", got)
	}
	for _, d := range []core.Decision{{Kind: core.Exec, Payload: "ls"}, got.Decision} {
		data, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		want := `{"kind":"exec","payload":"ls"}`
		if d.Purpose != "" {
			want = `{"kind":"exec","purpose":"list, then read go.mod","payload":"ls"}`
		}
		if string(data) != want {
			t.Fatalf("Decision JSON = %s, want %s", data, want)
		}
	}
}

func TestEffectIDIsStableAndDistinct(t *testing.T) {
	t.Parallel()

	effectID := func(runID string, step int) string {
		from := at(core.PhaseResponseSaved, withRev(1), withStep(step))
		from.RunID = runID
		from.Limits.MaxSteps = step
		got, err := from.Next(core.ResponseLoaded{Step: step, Attempt: 1, Body: execBody})
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		return got.EffectID
	}

	pairs := []struct {
		runID string
		step  int
	}{{"run-1", 1}, {"run-1", 2}, {"run-2", 1}, {"a1", 2}, {"a", 12}, {"a\x00b", 1}}
	seen := make(map[string]int)
	for i, pair := range pairs {
		id := effectID(pair.runID, pair.step)
		if previous, exists := seen[id]; exists {
			t.Fatalf("%v and %v share effect ID %q", pairs[previous], pair, id)
		}
		if again := effectID(pair.runID, pair.step); again != id {
			t.Fatalf("effect ID changed: %q != %q", id, again)
		}
		seen[id] = i
	}
}

func TestNextRejectsIllegalTransitions(t *testing.T) {
	t.Parallel()

	legal := make(map[core.Phase]map[reflect.Type]bool)
	for _, tt := range legalTransitions() {
		if legal[tt.from.Phase] == nil {
			legal[tt.from.Phase] = make(map[reflect.Type]bool)
		}
		legal[tt.from.Phase][reflect.TypeOf(tt.event)] = true
	}
	distinct := 0
	for _, events := range legal {
		distinct += len(events)
	}
	if distinct != legalPairs {
		t.Fatalf("legalTransitions() covers %d distinct pairs, want %d", distinct, legalPairs)
	}

	phases := []core.Phase{
		core.PhaseIdle, core.PhaseRequestFrozen, core.PhaseRequestSent, core.PhaseResponseSaved,
		core.PhaseEffectSaved, core.PhaseJobBound, core.PhaseAwaitingObservation, core.PhaseCancelling,
		core.PhaseReturned, core.PhaseNeedsResolution, core.PhaseStopped, "", "future_phase",
	}
	var events []core.Event
	for _, sample := range eventSamples() {
		events = append(events, sample.event)
	}

	checked := 0
	for _, phase := range phases {
		for _, event := range events {
			if legal[phase][reflect.TypeOf(event)] {
				continue
			}
			checked++
			from := at(phase, withRev(1), withExec, withJob, withReason(core.StopCancelled))
			got, err := from.Next(event)
			if !errors.Is(err, core.ErrIllegalTransition) {
				t.Errorf("%T in %q: error = %v, want illegal transition", event, phase, err)
			}
			if got != from {
				t.Errorf("%T in %q changed state to %#v", event, phase, got)
			}
		}
	}
	if want := len(phases)*len(events) - legalPairs; checked != want {
		t.Fatalf("checked %d illegal pairs, want %d", checked, want)
	}
}

func TestNextRejectsInvalidEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		from  core.State
		event core.Event
	}{
		{"nil event", at(core.PhaseIdle), nil},
		{"pointer event", at(core.PhaseIdle), &core.RequestFrozen{}},
		{"empty stop reason", at(core.PhaseIdle), core.StopRequested{}},
		{"unsupported stop reason", at(core.PhaseIdle), core.StopRequested{Reason: "lol"}},
		{"core-owned stop reason", at(core.PhaseIdle), core.StopRequested{Reason: core.StopStepsExhausted}},
		{"unresolved as a requested stop", at(core.PhaseIdle), core.StopRequested{Reason: core.StopUnresolved}},
		{"zero attempt started", at(core.PhaseRequestFrozen, withRev(1)), core.AttemptStarted{}},
		{"stale attempt started", at(core.PhaseRequestFrozen, withRev(1), withAttempt(2)), core.AttemptStarted{Step: 1, Attempt: 1}},
		{"attempt started ahead", at(core.PhaseRequestFrozen, withRev(1)), core.AttemptStarted{Step: 1, Attempt: 2}},
		{"redelivered attempt start after send", at(core.PhaseRequestSent, withRev(1)), core.AttemptStarted{Step: 1, Attempt: 1}},
		{"skipped attempt start", at(core.PhaseRequestSent, withRev(1)), core.AttemptStarted{Step: 1, Attempt: 3}},
		{"stale response saved", at(core.PhaseRequestSent, withRev(1), withAttempt(2)), core.ResponseSaved{Step: 1, Attempt: 1}},
		{"attempt started for an earlier step", at(core.PhaseRequestSent, withRev(1), withStep(2)), core.AttemptStarted{Step: 1, Attempt: 2}},
		{"response saved for an earlier step", at(core.PhaseRequestSent, withRev(1), withStep(2)), core.ResponseSaved{Step: 1, Attempt: 1}},
		{"incomplete response for an earlier step", at(core.PhaseRequestSent, withRev(1), withStep(2)), core.ResponseIncomplete{Step: 1, Attempt: 1}},
		{"response loaded for an earlier step", at(core.PhaseResponseSaved, withRev(1), withStep(2)), core.ResponseLoaded{Step: 1, Attempt: 1, Body: execBody}},
		{"stale incomplete response", at(core.PhaseRequestSent, withRev(1), withAttempt(2)), core.ResponseIncomplete{Step: 1, Attempt: 1}},
		{"empty response body", at(core.PhaseResponseSaved, withRev(1)), core.ResponseLoaded{Step: 1, Attempt: 1}},
		{"stale response loaded", at(core.PhaseResponseSaved, withRev(1), withAttempt(2)), core.ResponseLoaded{Step: 1, Attempt: 1, Body: execBody}},
		{"empty job bound", at(core.PhaseEffectSaved, withRev(1), withExec), core.JobBound{EffectID: "effect-1"}},
		{"bound job without effect", at(core.PhaseEffectSaved, withRev(1), withExec), core.JobBound{JobID: "job-1"}},
		{"bound job for foreign effect", at(core.PhaseEffectSaved, withRev(1), withExec), core.JobBound{EffectID: "effect-2", JobID: "job-1"}},
		{"different job while cancelling", at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.JobBound{EffectID: "effect-1", JobID: "job-2"}},
		{"different job while resolving", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob), core.JobBound{EffectID: "effect-1", JobID: "job-2"}},
		{"empty job started", at(core.PhaseJobBound, withRev(1), withExec, withJob), core.JobStarted{EffectID: "effect-1"}},
		{"start without effect", at(core.PhaseJobBound, withRev(1), withExec, withJob), core.JobStarted{JobID: "job-1"}},
		{"start of a different job", at(core.PhaseJobBound, withRev(1), withExec, withJob), core.JobStarted{EffectID: "effect-1", JobID: "job-2"}},
		{"start of an unbound job", at(core.PhaseJobBound, withRev(1), withExec), core.JobStarted{EffectID: "effect-1", JobID: "job-1"}},
		{"observation without effect", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob, withStarted), core.Observed{Execution: core.ExecutionExited}},
		{"observation for foreign effect", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob, withStarted), core.Observed{EffectID: "effect-2", Execution: core.ExecutionExited}},
		{"unknown outcome without effect", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob, withStarted), core.OutcomeUnknown{}},
		{"unknown outcome for foreign effect", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob, withStarted), core.OutcomeUnknown{EffectID: "effect-2"}},
		{"resolution for foreign effect", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob), core.Resolved{EffectID: "effect-2", Execution: core.ExecutionExited}},
		{"exit observed without job", at(core.PhaseEffectSaved, withRev(1), withExec), core.Observed{EffectID: "effect-1", Execution: core.ExecutionExited}},
		{"performed without effect", at(core.PhaseEffectSaved, withRev(1), withExec), core.Performed{}},
		{"performed for foreign effect", at(core.PhaseEffectSaved, withRev(1), withExec), core.Performed{EffectID: "effect-2"}},
		{"cancel observed without job before dispatch", at(core.PhaseEffectSaved, withRev(1), withExec), core.Observed{EffectID: "effect-1", Execution: core.ExecutionCancelled}},
		{"exit observed without job while cancelling", at(core.PhaseCancelling, withRev(1), withExec, withReason(core.StopCancelled)), core.Observed{EffectID: "effect-1", Execution: core.ExecutionExited}},
		{"failed start observed without job while cancelling", at(core.PhaseCancelling, withRev(1), withExec, withReason(core.StopCancelled)), core.Observed{EffectID: "effect-1", Execution: core.ExecutionFailedToStart}},
		{"unsupported execution observed without job", at(core.PhaseEffectSaved, withRev(1), withExec), core.Observed{EffectID: "effect-1", Execution: "bogus"}},
		{"unknown execution observed", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob, withStarted), core.Observed{EffectID: "effect-1", Execution: core.ExecutionUnknown}},
		{"unsupported execution observed", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob, withStarted), core.Observed{EffectID: "effect-1", Execution: "bogus"}},
		{"empty execution observed", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob, withStarted), core.Observed{EffectID: "effect-1"}},
		{"empty execution resolved", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob), core.Resolved{EffectID: "effect-1"}},
		{"unsupported execution resolved", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob), core.Resolved{EffectID: "effect-1", Execution: "bogus"}},
		{"exit resolved without job", at(core.PhaseNeedsResolution, withRev(1), withExec), core.Resolved{EffectID: "effect-1", Execution: core.ExecutionExited}},
		{"timeout resolved without job", at(core.PhaseNeedsResolution, withRev(1), withExec), core.Resolved{EffectID: "effect-1", Execution: core.ExecutionTimedOut}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.from.Next(tt.event)
			if !errors.Is(err, core.ErrInvalidEvent) {
				t.Fatalf("error = %v, want invalid event", err)
			}
			if got != tt.from {
				t.Fatalf("invalid event changed state to %#v", got)
			}
		})
	}
}

func walk(t *testing.T, state core.State, events ...core.Event) core.State {
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

func TestTrajectoryRejectsEchoesFromEarlierPositions(t *testing.T) {
	t.Parallel()

	start, err := core.NewState("run-1", core.Limits{MaxSteps: 3, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	effectSaved1 := walk(t, start,
		core.RequestFrozen{},
		core.AttemptStarted{Step: 1, Attempt: 1},
		core.ResponseSaved{Step: 1, Attempt: 1},
		core.ResponseLoaded{Step: 1, Attempt: 1, Body: execBody},
	)
	effect1 := effectSaved1.EffectID
	idle2 := walk(t, effectSaved1,
		core.JobBound{EffectID: effect1, JobID: "job-1"},
		core.JobStarted{EffectID: effect1, JobID: "job-1"},
		core.Observed{EffectID: effect1, Execution: core.ExecutionExited},
	)
	sent2 := walk(t, idle2, core.RequestFrozen{}, core.AttemptStarted{Step: 2, Attempt: 1})
	saved2 := walk(t, sent2, core.ResponseSaved{Step: 2, Attempt: 1})
	effectSaved2 := walk(t, saved2, core.ResponseLoaded{Step: 2, Attempt: 1, Body: execBody})
	effect2 := effectSaved2.EffectID
	if effect1 == effect2 {
		t.Fatal("steps share an effect identity")
	}
	bound2 := walk(t, effectSaved2, core.JobBound{EffectID: effect2, JobID: "job-2"})
	awaiting2 := walk(t, bound2, core.JobStarted{EffectID: effect2, JobID: "job-2"})

	echoes := []struct {
		name  string
		from  core.State
		event core.Event
	}{
		{"lost attempt of step 1", sent2, core.AttemptStarted{Step: 1, Attempt: 2}},
		{"response of step 1", sent2, core.ResponseSaved{Step: 1, Attempt: 1}},
		{"incomplete response of step 1", sent2, core.ResponseIncomplete{Step: 1, Attempt: 1}},
		{"loaded response of step 1", saved2, core.ResponseLoaded{Step: 1, Attempt: 1, Body: execBody}},
		{"bind of step 1", effectSaved2, core.JobBound{EffectID: effect1, JobID: "job-1"}},
		{"failed start of step 1", effectSaved2, core.Observed{EffectID: effect1, Execution: core.ExecutionFailedToStart}},
		{"unknown outcome of step 1", effectSaved2, core.OutcomeUnknown{EffectID: effect1}},
		{"start of step 1", bound2, core.JobStarted{EffectID: effect1, JobID: "job-1"}},
		{"observation of step 1", awaiting2, core.Observed{EffectID: effect1, Execution: core.ExecutionExited}},
	}
	for _, tt := range echoes {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.from.Next(tt.event)
			if !errors.Is(err, core.ErrInvalidEvent) {
				t.Fatalf("error = %v, want invalid event", err)
			}
			if got != tt.from {
				t.Fatalf("echo changed state to %#v", got)
			}
		})
	}
}

func TestTrajectoryRepairsProtocolFailureWithNewRequest(t *testing.T) {
	t.Parallel()

	start, err := core.NewState("run-1", core.Limits{MaxSteps: 3, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	repaired := walk(t, start,
		core.RequestFrozen{},
		core.AttemptStarted{Step: 1, Attempt: 1},
		core.ResponseSaved{Step: 1, Attempt: 1},
		core.ResponseLoaded{Step: 1, Attempt: 1, Body: invalidBody},
	)
	if repaired.Phase != core.PhaseIdle || repaired.RequestRev != 1 || repaired.Attempt != 2 {
		t.Fatalf("repair position = %#v, want idle at revision 1, attempt 2", repaired)
	}
	decided := walk(t, repaired,
		core.RequestFrozen{},
		core.AttemptStarted{Step: 1, Attempt: 2},
		core.ResponseSaved{Step: 1, Attempt: 2},
		core.ResponseLoaded{Step: 1, Attempt: 2, Body: execBody},
	)
	if decided.Phase != core.PhaseEffectSaved || decided.RequestRev != 2 || decided.Step != 1 {
		t.Fatalf("decided = %#v, want an effect from the second revision of step 1", decided)
	}
}

func TestIllegalPairsReportIllegalWhateverTheClaims(t *testing.T) {
	t.Parallel()

	start, err := core.NewState("run-1", core.Limits{MaxSteps: 3, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	stopped := walk(t, start, core.StopRequested{Reason: core.StopCancelled})
	tests := []struct {
		name  string
		from  core.State
		event core.Event
	}{
		{"mismatched bind in awaiting observation", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob, withStarted), core.JobBound{EffectID: "effect-1", JobID: "job-2"}},
		{"job bound for a stopped run", stopped, core.JobBound{EffectID: "effect-1", JobID: "job-1"}},
		{"job started for a stopped run", stopped, core.JobStarted{EffectID: "effect-1", JobID: "job-1"}},
		{"observation for a stopped run", stopped, core.Observed{EffectID: "effect-1", Execution: core.ExecutionExited}},
		{"response for a stopped run", stopped, core.ResponseSaved{Step: 1, Attempt: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.from.Next(tt.event)
			if !errors.Is(err, core.ErrIllegalTransition) {
				t.Fatalf("error = %v, want illegal transition", err)
			}
			if got != tt.from {
				t.Fatalf("illegal event changed state to %#v", got)
			}
		})
	}
}

func TestNextEnforcesBudgets(t *testing.T) {
	t.Parallel()

	tests := []transition{
		{"transport attempts exhausted", at(core.PhaseRequestSent, withRev(1), withAttempt(2)), core.ResponseIncomplete{Step: 1, Attempt: 2}, at(core.PhaseStopped, withRev(1), withAttempt(2), withReason(core.StopAttemptsExhausted))},
		{"lost attempts exhausted", at(core.PhaseRequestSent, withRev(1), withAttempt(2)), core.AttemptStarted{Step: 1, Attempt: 3}, at(core.PhaseStopped, withRev(1), withAttempt(2), withReason(core.StopAttemptsExhausted))},
		{"protocol attempts exhausted", at(core.PhaseResponseSaved, withRev(1), withAttempt(2)), core.ResponseLoaded{Step: 1, Attempt: 2, Body: invalidBody}, at(core.PhaseStopped, withRev(1), withAttempt(2), withReason(core.StopProtocolFailure))},
		{"steps exhausted after observation", at(core.PhaseAwaitingObservation, withRev(1), withStep(3), withExec, withJob, withStarted), core.Observed{EffectID: "effect-1", Execution: core.ExecutionExited}, at(core.PhaseStopped, withRev(1), withStep(3), withExec, withJob, withStarted, withReason(core.StopStepsExhausted))},
		{"steps exhausted after failed start", at(core.PhaseEffectSaved, withRev(1), withStep(3), withExec), core.Observed{EffectID: "effect-1", Execution: core.ExecutionFailedToStart}, at(core.PhaseStopped, withRev(1), withStep(3), withExec, withReason(core.StopStepsExhausted))},
		{"steps exhausted after a performed effect", at(core.PhaseEffectSaved, withRev(1), withStep(3), withExec), core.Performed{EffectID: "effect-1"}, at(core.PhaseStopped, withRev(1), withStep(3), withExec, withReason(core.StopStepsExhausted))},
		{"steps exhausted after resolution", at(core.PhaseNeedsResolution, withRev(1), withStep(3), withExec, withJob), core.Resolved{EffectID: "effect-1", Execution: core.ExecutionExited}, at(core.PhaseStopped, withRev(1), withStep(3), withExec, withJob, withReason(core.StopStepsExhausted))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.from.Next(tt.event)
			if err != nil {
				t.Fatalf("Next() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("Next() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestIntent(t *testing.T) {
	t.Parallel()

	tests := map[core.Phase]core.Intent{
		core.PhaseIdle:                core.IntentPrepareRequest,
		core.PhaseRequestFrozen:       core.IntentSendRequest,
		core.PhaseRequestSent:         core.IntentSendRequest,
		core.PhaseResponseSaved:       core.IntentDecode,
		core.PhaseEffectSaved:         core.IntentBind,
		core.PhaseJobBound:            core.IntentStart,
		core.PhaseAwaitingObservation: core.IntentInspect,
		core.PhaseCancelling:          core.IntentCancel,
		core.PhaseReturned:            core.IntentHalt,
		core.PhaseNeedsResolution:     core.IntentHalt,
		core.PhaseStopped:             core.IntentHalt,
		"":                            core.IntentHalt,
		"future_phase":                core.IntentHalt,
	}
	for phase, want := range tests {
		if got := at(phase).Intent(); got != want {
			t.Errorf("Intent(%q) = %q, want %q", phase, got, want)
		}
	}
}

func TestCompatibleSemanticsVersions(t *testing.T) {
	bounded := core.State{Limits: core.Limits{MaxSteps: 3, MaxAttempts: 1}}
	unlimited := core.State{Limits: core.Limits{MaxSteps: 0, MaxAttempts: 1}}
	purposed := bounded
	purposed.Decision = core.Decision{Kind: core.Exec, Purpose: "check", Payload: "true"}
	for _, tc := range []struct {
		version int
		state   core.State
		ok      bool
	}{
		{0, bounded, false}, {1, bounded, true}, {1, unlimited, false}, {1, purposed, false},
		{2, bounded, true}, {2, unlimited, true}, {2, purposed, false},
		{3, unlimited, true}, {3, purposed, true}, {core.SemanticsVersion + 1, bounded, false},
	} {
		if err := core.Compatible(tc.version, tc.state); (err == nil) != tc.ok {
			t.Errorf("Compatible(%d, %+v) = %v, want ok %v", tc.version, tc.state, err, tc.ok)
		}
		if err := core.KnownSemantics(tc.version); (err == nil) != (tc.version >= 1 && tc.version <= core.SemanticsVersion) {
			t.Errorf("KnownSemantics(%d) = %v", tc.version, err)
		}
	}
}

func TestCompatibleEventsExistInTheirSemanticsVersion(t *testing.T) {
	for _, e := range core.Events() {
		for version := 1; version <= core.SemanticsVersion+1; version++ {
			_, performed := e.(core.Performed)
			want := version <= core.SemanticsVersion && (!performed || version >= 4)
			if err := core.CompatibleEvent(version, e); (err == nil) != want {
				t.Errorf("CompatibleEvent(%d, %s) = %v, want ok %v", version, e.Kind(), err, want)
			}
		}
	}
}

func TestClosedPhases(t *testing.T) {
	for _, p := range []core.Phase{core.PhaseIdle, core.PhaseRequestFrozen, core.PhaseRequestSent, core.PhaseResponseSaved, core.PhaseEffectSaved,
		core.PhaseJobBound, core.PhaseAwaitingObservation, core.PhaseCancelling, core.PhaseNeedsResolution, core.PhaseReturned, core.PhaseStopped} {
		if got := (core.State{Phase: p}).Closed(); got != (p == core.PhaseReturned || p == core.PhaseStopped) {
			t.Errorf("phase %s closed = %v", p, got)
		}
	}
}
