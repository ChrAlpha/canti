package core_test

import (
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

const legalPairs = 23

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
		{name: "zero steps", runID: "run-1", limits: core.Limits{MaxSteps: 0, MaxAttempts: 1}},
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

func legalTransitions() []transition {
	return []transition{
		{"idle freezes request", at(core.PhaseIdle), core.RequestFrozen{}, at(core.PhaseRequestFrozen, withRev(1))},
		{"repair freezes a new request", at(core.PhaseIdle, withRev(1), withAttempt(2)), core.RequestFrozen{}, at(core.PhaseRequestFrozen, withRev(2), withAttempt(2))},
		{"idle stops", at(core.PhaseIdle), core.StopRequested{Reason: core.StopCancelled}, at(core.PhaseStopped, withReason(core.StopCancelled))},

		{"frozen request starts attempt", at(core.PhaseRequestFrozen, withRev(1)), core.AttemptStarted{Attempt: 1}, at(core.PhaseRequestSent, withRev(1))},
		{"frozen request stops", at(core.PhaseRequestFrozen, withRev(1)), core.StopRequested{Reason: core.StopTimedOut}, at(core.PhaseStopped, withRev(1), withReason(core.StopTimedOut))},

		{"sent request counts lost attempt", at(core.PhaseRequestSent, withRev(1)), core.AttemptStarted{Attempt: 2}, at(core.PhaseRequestSent, withRev(1), withAttempt(2))},
		{"sent request saves response", at(core.PhaseRequestSent, withRev(1)), core.ResponseSaved{Attempt: 1}, at(core.PhaseResponseSaved, withRev(1))},
		{"sent request retries incomplete response", at(core.PhaseRequestSent, withRev(1)), core.ResponseIncomplete{Attempt: 1}, at(core.PhaseRequestFrozen, withRev(1), withAttempt(2))},
		{"sent request stops", at(core.PhaseRequestSent, withRev(1)), core.StopRequested{Reason: core.StopTimedOut}, at(core.PhaseStopped, withRev(1), withReason(core.StopTimedOut))},

		{"saved response decodes return", at(core.PhaseResponseSaved, withRev(1)), core.ResponseLoaded{Attempt: 1, Body: returnBody}, at(core.PhaseReturned, withRev(1), func(s *core.State) {
			s.Decision = core.Decision{Kind: core.Return, Payload: "done"}
		})},
		{"saved response repairs invalid decision with a new request", at(core.PhaseResponseSaved, withRev(1)), core.ResponseLoaded{Attempt: 1, Body: invalidBody}, at(core.PhaseIdle, withRev(1), withAttempt(2))},
		{"saved response stops", at(core.PhaseResponseSaved, withRev(1)), core.StopRequested{Reason: core.StopCancelled}, at(core.PhaseStopped, withRev(1), withReason(core.StopCancelled))},

		{"saved effect starts job", at(core.PhaseEffectSaved, withRev(1), withExec), core.JobStarted{EffectID: "effect-1", JobID: "job-1"}, at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob)},
		{"saved effect observes failed start", at(core.PhaseEffectSaved, withRev(1), withExec), core.Observed{EffectID: "effect-1", Execution: core.ExecutionFailedToStart}, at(core.PhaseIdle, withStep(2))},
		{"saved effect keeps unknown outcome", at(core.PhaseEffectSaved, withRev(1), withExec), core.OutcomeUnknown{EffectID: "effect-1"}, at(core.PhaseNeedsResolution, withRev(1), withExec)},
		{"saved effect cancels", at(core.PhaseEffectSaved, withRev(1), withExec), core.StopRequested{Reason: core.StopCancelled}, at(core.PhaseCancelling, withRev(1), withExec, withReason(core.StopCancelled))},

		{"awaiting observation advances", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob), core.Observed{EffectID: "effect-1", Execution: core.ExecutionExited}, at(core.PhaseIdle, withStep(2))},
		{"awaiting observation keeps unknown outcome", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob), core.OutcomeUnknown{EffectID: "effect-1"}, at(core.PhaseNeedsResolution, withRev(1), withExec, withJob)},
		{"awaiting observation cancels", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob), core.StopRequested{Reason: core.StopBudgetExhausted}, at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopBudgetExhausted))},

		{"cancelling finds job", at(core.PhaseCancelling, withRev(1), withExec, withReason(core.StopCancelled)), core.JobStarted{EffectID: "effect-1", JobID: "job-1"}, at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"cancelling confirms known job", at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.JobStarted{EffectID: "effect-1", JobID: "job-1"}, at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"cancelling records missing job as cancelled", at(core.PhaseCancelling, withRev(1), withExec, withReason(core.StopCancelled)), core.Observed{EffectID: "effect-1", Execution: core.ExecutionCancelled}, at(core.PhaseStopped, withRev(1), withExec, withReason(core.StopCancelled))},
		{"cancelling records true result", at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.Observed{EffectID: "effect-1", Execution: core.ExecutionExited}, at(core.PhaseStopped, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"cancelling keeps unknown outcome", at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.OutcomeUnknown{EffectID: "effect-1"}, at(core.PhaseNeedsResolution, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"cancelling keeps first stop reason", at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.StopRequested{Reason: core.StopTimedOut}, at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled))},

		{"resolution continues", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob), core.Resolved{EffectID: "effect-1", Execution: core.ExecutionFailedToStart}, at(core.PhaseIdle, withStep(2))},
		{"resolution completes pending stop", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.Resolved{EffectID: "effect-1", Execution: core.ExecutionExited}, at(core.PhaseStopped, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"unresolved run records stop", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob), core.StopRequested{Reason: core.StopCancelled}, at(core.PhaseNeedsResolution, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
		{"unresolved run keeps first stop reason", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.StopRequested{Reason: core.StopTimedOut}, at(core.PhaseNeedsResolution, withRev(1), withExec, withJob, withReason(core.StopCancelled))},
	}
}

func TestNextLegalTransitions(t *testing.T) {
	t.Parallel()

	for _, tt := range legalTransitions() {
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

func TestNextDerivesEffectFromExecDecision(t *testing.T) {
	t.Parallel()

	from := at(core.PhaseResponseSaved, withRev(1))
	got, err := from.Next(core.ResponseLoaded{Attempt: 1, Body: execBody})
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

func TestEffectIDIsStableAndDistinct(t *testing.T) {
	t.Parallel()

	effectID := func(runID string, step int) string {
		from := at(core.PhaseResponseSaved, withRev(1), withStep(step))
		from.RunID = runID
		from.Limits.MaxSteps = step
		got, err := from.Next(core.ResponseLoaded{Attempt: 1, Body: execBody})
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
		core.PhaseEffectSaved, core.PhaseAwaitingObservation, core.PhaseCancelling, core.PhaseReturned,
		core.PhaseNeedsResolution, core.PhaseStopped, "", "future_phase",
	}
	events := []core.Event{
		core.RequestFrozen{}, core.AttemptStarted{Attempt: 1}, core.ResponseSaved{Attempt: 1}, core.ResponseIncomplete{Attempt: 1},
		core.ResponseLoaded{Attempt: 1, Body: execBody}, core.JobStarted{EffectID: "effect-1", JobID: "job-1"},
		core.Observed{EffectID: "effect-1", Execution: core.ExecutionFailedToStart}, core.OutcomeUnknown{EffectID: "effect-1"},
		core.StopRequested{Reason: core.StopCancelled}, core.Resolved{EffectID: "effect-1", Execution: core.ExecutionExited},
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
		{"zero attempt started", at(core.PhaseRequestFrozen, withRev(1)), core.AttemptStarted{}},
		{"stale attempt started", at(core.PhaseRequestFrozen, withRev(1), withAttempt(2)), core.AttemptStarted{Attempt: 1}},
		{"redelivered attempt start after send", at(core.PhaseRequestSent, withRev(1)), core.AttemptStarted{Attempt: 1}},
		{"skipped attempt start", at(core.PhaseRequestSent, withRev(1)), core.AttemptStarted{Attempt: 3}},
		{"stale response saved", at(core.PhaseRequestSent, withRev(1), withAttempt(2)), core.ResponseSaved{Attempt: 1}},
		{"stale incomplete response", at(core.PhaseRequestSent, withRev(1), withAttempt(2)), core.ResponseIncomplete{Attempt: 1}},
		{"empty response body", at(core.PhaseResponseSaved, withRev(1)), core.ResponseLoaded{Attempt: 1}},
		{"stale response loaded", at(core.PhaseResponseSaved, withRev(1), withAttempt(2)), core.ResponseLoaded{Attempt: 1, Body: execBody}},
		{"empty job", at(core.PhaseEffectSaved, withRev(1), withExec), core.JobStarted{EffectID: "effect-1"}},
		{"job without effect", at(core.PhaseEffectSaved, withRev(1), withExec), core.JobStarted{JobID: "job-1"}},
		{"job for foreign effect", at(core.PhaseEffectSaved, withRev(1), withExec), core.JobStarted{EffectID: "effect-2", JobID: "job-1"}},
		{"different job while cancelling", at(core.PhaseCancelling, withRev(1), withExec, withJob, withReason(core.StopCancelled)), core.JobStarted{EffectID: "effect-1", JobID: "job-2"}},
		{"observation without effect", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob), core.Observed{Execution: core.ExecutionExited}},
		{"observation for foreign effect", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob), core.Observed{EffectID: "effect-2", Execution: core.ExecutionExited}},
		{"unknown outcome without effect", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob), core.OutcomeUnknown{}},
		{"unknown outcome for foreign effect", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob), core.OutcomeUnknown{EffectID: "effect-2"}},
		{"resolution for foreign effect", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob), core.Resolved{EffectID: "effect-2", Execution: core.ExecutionExited}},
		{"exit observed without job", at(core.PhaseEffectSaved, withRev(1), withExec), core.Observed{EffectID: "effect-1", Execution: core.ExecutionExited}},
		{"cancel observed without job before dispatch", at(core.PhaseEffectSaved, withRev(1), withExec), core.Observed{EffectID: "effect-1", Execution: core.ExecutionCancelled}},
		{"exit observed without job while cancelling", at(core.PhaseCancelling, withRev(1), withExec, withReason(core.StopCancelled)), core.Observed{EffectID: "effect-1", Execution: core.ExecutionExited}},
		{"failed start observed without job while cancelling", at(core.PhaseCancelling, withRev(1), withExec, withReason(core.StopCancelled)), core.Observed{EffectID: "effect-1", Execution: core.ExecutionFailedToStart}},
		{"unsupported execution observed without job", at(core.PhaseEffectSaved, withRev(1), withExec), core.Observed{EffectID: "effect-1", Execution: "bogus"}},
		{"unknown execution observed", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob), core.Observed{EffectID: "effect-1", Execution: core.ExecutionUnknown}},
		{"unsupported execution observed", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob), core.Observed{EffectID: "effect-1", Execution: "bogus"}},
		{"empty execution observed", at(core.PhaseAwaitingObservation, withRev(1), withExec, withJob), core.Observed{EffectID: "effect-1"}},
		{"unknown execution resolved", at(core.PhaseNeedsResolution, withRev(1), withExec, withJob), core.Resolved{EffectID: "effect-1", Execution: core.ExecutionUnknown}},
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

func TestNextEnforcesBudgets(t *testing.T) {
	t.Parallel()

	tests := []transition{
		{"transport attempts exhausted", at(core.PhaseRequestSent, withRev(1), withAttempt(2)), core.ResponseIncomplete{Attempt: 2}, at(core.PhaseStopped, withRev(1), withAttempt(2), withReason(core.StopAttemptsExhausted))},
		{"lost attempts exhausted", at(core.PhaseRequestSent, withRev(1), withAttempt(2)), core.AttemptStarted{Attempt: 3}, at(core.PhaseStopped, withRev(1), withAttempt(2), withReason(core.StopAttemptsExhausted))},
		{"protocol attempts exhausted", at(core.PhaseResponseSaved, withRev(1), withAttempt(2)), core.ResponseLoaded{Attempt: 2, Body: invalidBody}, at(core.PhaseStopped, withRev(1), withAttempt(2), withReason(core.StopProtocolFailure))},
		{"steps exhausted after observation", at(core.PhaseAwaitingObservation, withRev(1), withStep(3), withExec, withJob), core.Observed{EffectID: "effect-1", Execution: core.ExecutionExited}, at(core.PhaseStopped, withRev(1), withStep(3), withExec, withJob, withReason(core.StopStepsExhausted))},
		{"steps exhausted after failed start", at(core.PhaseEffectSaved, withRev(1), withStep(3), withExec), core.Observed{EffectID: "effect-1", Execution: core.ExecutionFailedToStart}, at(core.PhaseStopped, withRev(1), withStep(3), withExec, withReason(core.StopStepsExhausted))},
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
		core.PhaseEffectSaved:         core.IntentDispatch,
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
