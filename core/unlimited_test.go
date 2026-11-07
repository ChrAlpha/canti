package core_test

import (
	"math"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

func TestUnlimitedSteps(t *testing.T) {
	s, err := core.NewState("unlimited", core.Limits{MaxSteps: 0, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	for step := 1; step <= 25; step++ {
		s = walk(t, s, core.RequestFrozen{}, core.AttemptStarted{Step: step, Attempt: 1}, core.ResponseSaved{Step: step, Attempt: 1}, core.ResponseLoaded{Step: step, Attempt: 1, Body: execBody})
		s = walk(t, s, core.Observed{EffectID: s.EffectID, Execution: core.ExecutionExited})
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
		if s.Step != step+1 || s.Phase != core.PhaseIdle {
			t.Fatalf("did not advance: %+v", s)
		}
	}
	s = walk(t, s, core.RequestFrozen{}, core.AttemptStarted{Step: 26, Attempt: 1}, core.ResponseSaved{Step: 26, Attempt: 1}, core.ResponseLoaded{Step: 26, Attempt: 1, Body: returnBody})
	if s.Phase != core.PhaseReturned {
		t.Fatalf("did not return: %+v", s)
	}
}

func TestUnlimitedStepsRetainRetriesAndCancellation(t *testing.T) {
	for _, tc := range legalTransitions() {
		tc.from.Limits.MaxSteps = 0
		tc.want.Limits.MaxSteps = 0
		got, err := tc.from.Next(tc.event)
		if err != nil || got != tc.want {
			t.Errorf("%s: got %+v, %v; want %+v", tc.name, got, err, tc.want)
		}
	}
}

func TestUnlimitedStepIntegerLimit(t *testing.T) {
	s := at(core.PhaseEffectSaved, withRev(1), withStep(math.MaxInt), withExec)
	s.Limits.MaxSteps = 0
	s = walk(t, s, core.Observed{EffectID: s.EffectID, Execution: core.ExecutionExited})
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	if s.Step != math.MaxInt || s.Phase != core.PhaseStopped || s.Reason != core.StopUnrecoverable || s.EffectID == "" {
		t.Fatalf("integer limit lost committed identity: %+v", s)
	}
}

func TestUnlimitedStepsExhaustAttempts(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		s, err := core.NewState("unlimited", core.Limits{MaxSteps: 0, MaxAttempts: 2})
		if err != nil {
			t.Fatal(err)
		}
		for attempt := 1; attempt <= 2; attempt++ {
			if s.Phase == core.PhaseIdle {
				s = walk(t, s, core.RequestFrozen{})
			}
			s = walk(t, s, core.AttemptStarted{Step: 1, Attempt: attempt})
			if invalid {
				s = walk(t, s, core.ResponseSaved{Step: 1, Attempt: attempt}, core.ResponseLoaded{Step: 1, Attempt: attempt, Body: invalidBody})
			} else {
				s = walk(t, s, core.ResponseIncomplete{Step: 1, Attempt: attempt})
			}
		}
		want := core.StopAttemptsExhausted
		if invalid {
			want = core.StopProtocolFailure
		}
		if s.Phase != core.PhaseStopped || s.Reason != want {
			t.Fatalf("unbounded retries: %+v", s)
		}
	}
}
