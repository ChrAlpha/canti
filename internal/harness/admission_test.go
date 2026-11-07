package harness_test

import (
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestBlocksWhileOpenOrAbandonedUnknown(t *testing.T) {
	stopped := core.State{Phase: core.PhaseStopped, Reason: core.StopCancelled, EffectID: "effect-1"}
	for _, tc := range []struct {
		name  string
		state core.State
		last  core.Event
		want  bool
	}{
		{"new run", core.State{Phase: core.PhaseIdle}, nil, true},
		{"open step", core.State{Phase: core.PhaseEffectSaved}, core.ResponseLoaded{}, true},
		{"needs resolution", core.State{Phase: core.PhaseNeedsResolution}, core.OutcomeUnknown{}, true},
		{"returned", core.State{Phase: core.PhaseReturned}, core.ResponseLoaded{}, false},
		{"stopped", stopped, core.StopRequested{Reason: core.StopCancelled}, false},
		{"resolved as known", stopped, core.Resolved{EffectID: "effect-1", Execution: core.ExecutionExited}, false},
		{"abandoned unknown", stopped, core.Resolved{EffectID: "effect-1", Execution: core.ExecutionUnknown}, true},
	} {
		if got := harness.Blocks(tc.state, tc.last); got != tc.want {
			t.Errorf("%s: Blocks = %v, want %v", tc.name, got, tc.want)
		}
	}
}
