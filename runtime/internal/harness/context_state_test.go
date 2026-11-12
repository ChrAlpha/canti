package harness

import (
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

func handoverEventState(t *testing.T, payload string) (core.State, core.State) {
	t.Helper()
	from := core.State{RunID: "run-1", Step: 2, Attempt: 1, RequestRev: 1, Phase: core.PhaseEffectSaved,
		EffectID: "effect-2", Decision: core.Decision{Kind: core.Exec, Payload: payload},
		Limits: core.Limits{MaxSteps: 10, MaxAttempts: 3}}
	to, err := from.Next(core.Performed{EffectID: "effect-2"})
	if err != nil {
		t.Fatal(err)
	}
	return from, to
}

func TestCheckContextMutationAcceptsOnlyTheHandoverShapeOnPerformed(t *testing.T) {
	payload := "canti:handover verified the parser"
	from, to := handoverEventState(t, payload)
	rejection := &RejectionRef{Seq: 3, Step: 1, MaxAcceptedInput: 10, MaxAcceptedReported: true}
	current := ContextState{FirstStep: 1, Rejection: rejection}
	head := ContextState{AnchorSeq: 7, FirstStep: to.Step, Rejection: rejection}
	done := &Outcome{Status: OutcomeDone}
	event := core.Performed{EffectID: from.EffectID}
	for _, tc := range []struct {
		name  string
		next  *ContextState
		facts Facts
		ok    bool
	}{
		{"the committed head", &head, Facts{Outcome: done, ContextState: &head}, true},
		{"no head", nil, Facts{Outcome: done}, false},
		{"a retrieval with a head", &head, Facts{Outcome: &Outcome{Status: OutcomeDone, Retrieval: &archive.Retrieval{}}, ContextState: &head}, false},
		{"failed outcome with a head", &head, Facts{Outcome: &Outcome{Status: OutcomeFailed, Detail: "x"}, ContextState: &head}, false},
		{"another anchor sequence", func() *ContextState { h := head; h.AnchorSeq++; return &h }(), Facts{Outcome: done, ContextState: &head}, false},
		{"another first step", func() *ContextState { h := head; h.FirstStep++; return &h }(), Facts{Outcome: done, ContextState: &head}, false},
		{"a minimal head", func() *ContextState { h := head; h.Minimal = true; return &h }(), Facts{Outcome: done, ContextState: &head}, false},
		{"another rejection", func() *ContextState {
			h := head
			h.Rejection = &RejectionRef{Seq: 6, Step: 1, MaxAcceptedInput: 1}
			return &h
		}(), Facts{Outcome: done, ContextState: &head}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			facts := tc.facts
			tr := Transition{Seq: 7, From: from, To: to, Event: event, Facts: facts}
			if tc.next != nil {
				tr.Facts.ContextState = tc.next
			}
			err := CheckContextMutation(tr, current)
			if (err == nil) != tc.ok {
				t.Fatalf("CheckContextMutation = %v, want ok %v (next %+v to.Step %d)", err, tc.ok, tr.Facts.ContextState, to.Step)
			}
		})
	}
}

func TestCheckContextMutationRejectsAContextChangeOnEveryOtherEvent(t *testing.T) {
	head := &ContextState{AnchorSeq: 7, FirstStep: 3}
	current := InitialContext()
	for _, tc := range []struct {
		name  string
		from  core.State
		event core.Event
	}{
		{"frozen request", core.State{RunID: "run-1", Step: 1, Attempt: 1, Phase: core.PhaseIdle, Limits: core.Limits{MaxSteps: 5, MaxAttempts: 2}}, core.RequestFrozen{}},
		{"bound instructions", core.State{RunID: "run-1", Step: 1, Attempt: 1, Phase: core.PhaseIdle, Limits: core.Limits{MaxSteps: 5, MaxAttempts: 2}}, core.InputsBound{Count: 1}},
		{"attempt", core.State{RunID: "run-1", Step: 1, Attempt: 1, RequestRev: 1, Phase: core.PhaseRequestFrozen, Limits: core.Limits{MaxSteps: 5, MaxAttempts: 2}}, core.AttemptStarted{Step: 1, Attempt: 1}},
		{"saved response", core.State{RunID: "run-1", Step: 1, Attempt: 1, RequestRev: 1, Phase: core.PhaseRequestSent, Limits: core.Limits{MaxSteps: 5, MaxAttempts: 2}}, core.ResponseSaved{Step: 1, Attempt: 1}},
		{"observation", core.State{RunID: "run-1", Step: 1, Attempt: 1, RequestRev: 1, Phase: core.PhaseEffectSaved, EffectID: "effect-1", Limits: core.Limits{MaxSteps: 5, MaxAttempts: 2}}, core.Observed{EffectID: "effect-1", Execution: core.ExecutionExited}},
		{"resolution", core.State{RunID: "run-1", Step: 1, Attempt: 1, RequestRev: 1, Phase: core.PhaseNeedsResolution, EffectID: "effect-1", Limits: core.Limits{MaxSteps: 5, MaxAttempts: 2}}, core.Resolved{EffectID: "effect-1", Execution: core.ExecutionExited}},
		{"recovery", core.State{RunID: "run-1", Step: 1, Attempt: 1, RequestRev: 1, Phase: core.PhaseNeedsResolution, EffectID: "effect-1", Reason: core.StopCancelled, Limits: core.Limits{MaxSteps: 5, MaxAttempts: 2}}, core.EffectRecovered{EffectID: "effect-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			to, err := tc.from.Next(tc.event)
			if err != nil {
				t.Fatalf("fixture event is illegal: %v", err)
			}
			facts := Facts{ContextState: head}
			if _, ok := tc.event.(core.RequestFrozen); ok {
				facts.Request = &Request{Body: []byte("bytes"), ContextState: &current}
			}
			tr := Transition{Seq: 7, From: tc.from, To: to, Event: tc.event, Facts: facts}
			if err := CheckContextMutation(tr, current); err == nil {
				t.Fatalf("%s carried a context change", tc.event.Kind())
			}
		})
	}
}

func rejectedRequest(t *testing.T, current ContextState, shape ContextRejection, opens bool) Transition {
	t.Helper()
	from := core.State{RunID: "run-1", Step: 2, Attempt: 1, RequestRev: 1, Phase: core.PhaseRequestSent,
		Limits: core.Limits{MaxSteps: 10, MaxAttempts: 3}}
	shape.Previous, shape.Signal = current, ContextLimitError{Status: 400, Code: "context_length_exceeded"}
	event := core.RequestRejected{Step: 2, Attempt: 1}
	to, err := from.Next(event)
	if err != nil {
		t.Fatal(err)
	}
	facts := Facts{Response: &Response{Body: []byte(`{}`)}, Failure: "confirmed overflow", Rejection: &shape}
	if opens {
		head, class := shape.Head(7, 2, current)
		if class != RejectionOpensHead {
			t.Fatalf("fixture does not open a head: %v", class)
		}
		facts.ContextState = &head
	}
	return Transition{Seq: 7, From: from, To: to, Event: event, Facts: facts}
}

func TestRejectionHeadTable(t *testing.T) {
	rejection := &RejectionRef{Seq: 3, Step: 1, MaxAcceptedInput: 10, MaxAcceptedReported: true}
	for _, tc := range []struct {
		name  string
		shape ContextRejection
		want  RejectionHead
		head  ContextState
	}{
		{"resident steps keep minimal", ContextRejection{EpochSteps: 2}, RejectionOpensHead,
			ContextState{AnchorSeq: 7, FirstStep: 2, Minimal: false, Rejection: &RejectionRef{Seq: 7, Step: 2, MaxAcceptedInput: 10, MaxAcceptedReported: true}}},
		{"resident steps keep a minimal floor", ContextRejection{EpochSteps: 1, Minimal: true}, RejectionOpensHead,
			ContextState{AnchorSeq: 7, FirstStep: 2, Minimal: true, Rejection: &RejectionRef{Seq: 7, Step: 2, MaxAcceptedInput: 10, MaxAcceptedReported: true}}},
		{"a repair keeps minimal and drops the repair", ContextRejection{Repair: true, Minimal: true}, RejectionOpensHead,
			ContextState{AnchorSeq: 7, FirstStep: 2, Minimal: true, Rejection: &RejectionRef{Seq: 7, Step: 2, MaxAcceptedInput: 10, MaxAcceptedReported: true}}},
		{"optional content opens the floor", ContextRejection{OptionalContent: true}, RejectionOpensHead,
			ContextState{AnchorSeq: 7, FirstStep: 2, Minimal: true, Rejection: &RejectionRef{Seq: 7, Step: 2, MaxAcceptedInput: 10, MaxAcceptedReported: true}}},
		{"a minimal request is terminal", ContextRejection{Minimal: true, OptionalContent: true}, RejectionTerminalMinimal, ContextState{}},
		{"orientation alone is terminal", ContextRejection{}, RejectionTerminalOrientation, ContextState{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := ContextState{FirstStep: 1, Rejection: rejection, Minimal: tc.shape.Minimal}
			shape := tc.shape
			shape.MaxAcceptedInput, shape.MaxAcceptedReported = 10, true
			head, class := shape.Head(7, 2, current)
			if class != tc.want {
				t.Fatalf("class = %v, want %v", class, tc.want)
			}
			if class == RejectionOpensHead {
				want := tc.head
				want.Rejection = &RejectionRef{Seq: 7, Step: 2, MaxAcceptedInput: 10, MaxAcceptedReported: true}
				if !head.Equal(want) {
					t.Fatalf("head = %+v, want %+v", head, want)
				}
			} else if !head.Equal(ContextState{}) {
				t.Fatalf("terminal head = %+v", head)
			}
			tr := rejectedRequest(t, current, shape, class == RejectionOpensHead)
			if err := CheckContextMutation(tr, current); (err == nil) != (class == RejectionOpensHead) {
				t.Fatalf("CheckContextMutation = %v for class %v", err, class)
			}
		})
	}
}

func TestContextStateValidationCoversTheInitialHeadAndRejection(t *testing.T) {
	good := ContextState{AnchorSeq: 3, FirstStep: 1, Rejection: &RejectionRef{Seq: 3, Step: 1, MaxAcceptedInput: 4, MaxAcceptedReported: true}}
	if err := good.Validate(1, 3); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []ContextState{
		{FirstStep: 1, Minimal: true},
		{FirstStep: 1, Rejection: &RejectionRef{Seq: 1, Step: 1}},
		{AnchorSeq: 5, FirstStep: 1, Rejection: &RejectionRef{Seq: 6, Step: 1}},
		{AnchorSeq: 5, FirstStep: 1, Rejection: &RejectionRef{Seq: 1, Step: 0}},
	} {
		if err := bad.Validate(1, 5); err == nil {
			t.Fatalf("invalid head accepted: %+v", bad)
		}
	}
}
