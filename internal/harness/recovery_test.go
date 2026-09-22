package harness

import "testing"

func TestRunStateResumable(t *testing.T) {
	for _, phase := range []Phase{PhaseIdle, PhaseRequestFrozen, PhaseResponseSaved,
		PhaseDecisionSaved, PhaseEffectSaved, PhaseDispatching, PhaseAwaitingObservation, PhaseObservationSaved} {
		t.Run(string(phase), func(t *testing.T) {
			state := RunState{Phase: phase}
			if !state.Resumable() {
				t.Fatal("known active phase should permit recovery")
			}
			state.NeedsInput = true
			if state.Resumable() {
				t.Fatal("must wait for required input")
			}
		})
	}
	for _, phase := range []Phase{"", "future_phase", PhaseReturned, PhaseNeedsResolution} {
		t.Run(string(phase), func(t *testing.T) {
			if (RunState{Phase: phase}).Resumable() {
				t.Fatal("unknown or terminal phase must not continue automatically")
			}
		})
	}
}
