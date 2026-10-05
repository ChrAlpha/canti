package core

import "testing"

func TestPersistedNamesMatchCommittedLiterals(t *testing.T) {
	for _, tc := range []struct{ got, want string }{
		{string(PhaseIdle), "idle"},
		{string(PhaseRequestFrozen), "request_frozen"},
		{string(PhaseRequestSent), "request_sent"},
		{string(PhaseResponseSaved), "response_saved"},
		{string(PhaseEffectSaved), "effect_saved"},
		{string(PhaseJobBound), "job_bound"},
		{string(PhaseAwaitingObservation), "awaiting_observation"},
		{string(PhaseCancelling), "cancelling"},
		{string(PhaseReturned), "returned"},
		{string(PhaseNeedsResolution), "needs_resolution"},
		{string(PhaseStopped), "stopped"},
		{string(StopCancelled), "cancelled"},
		{string(StopTimedOut), "timed_out"},
		{string(StopBudgetExhausted), "budget_exhausted"},
		{string(StopUnrecoverable), "unrecoverable"},
		{string(StopStepsExhausted), "steps_exhausted"},
		{string(StopAttemptsExhausted), "attempts_exhausted"},
		{string(StopProtocolFailure), "protocol_failure"},
		{string(StopUnresolved), "unresolved"},
		{string(ExecutionExited), "exited"},
		{string(ExecutionTimedOut), "timed_out"},
		{string(ExecutionCancelled), "cancelled"},
		{string(ExecutionFailedToStart), "failed_to_start"},
		{string(ExecutionUnknown), "unknown"},
		{RequestFrozen{}.Kind(), "request_frozen"},
		{AttemptStarted{}.Kind(), "attempt_started"},
		{ResponseSaved{}.Kind(), "response_saved"},
		{ResponseIncomplete{}.Kind(), "response_incomplete"},
		{ResponseLoaded{}.Kind(), "response_loaded"},
		{JobBound{}.Kind(), "job_bound"},
		{JobStarted{}.Kind(), "job_started"},
		{Observed{}.Kind(), "observed"},
		{OutcomeUnknown{}.Kind(), "outcome_unknown"},
		{StopRequested{}.Kind(), "stop_requested"},
		{Resolved{}.Kind(), "resolved"},
		{string(Exec), "exec"},
		{string(Return), "return"},
	} {
		if tc.got != tc.want {
			t.Errorf("persisted name %q is now %q", tc.want, tc.got)
		}
	}
}
