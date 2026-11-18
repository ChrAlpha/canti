package harness

import (
	"strings"
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

func TestTailStepKeepsTheLatestStepAndAQuarterOfTheBudget(t *testing.T) {
	steps := []ContextStepBytes{{1, 200}, {2, 200}, {3, 150}, {4, 5000}}
	for _, tc := range []struct {
		tokens int64
		want   int
	}{{1000, 3}, {599, 4}, {600, 3}, {2199, 2}, {2200, 1}, {1, 4}} {
		if got := TailStep(steps, tc.tokens); got != tc.want {
			t.Errorf("TailStep(%d) = %d, want %d", tc.tokens, got, tc.want)
		}
	}
}

// budgetAnchor returns a valid budget anchor frozen at step 5 over current,
// and the record it holds.
func budgetAnchor(t *testing.T, current ContextState) (Transition, *ContextBudget) {
	t.Helper()
	from := core.State{RunID: "run-1", Step: 5, Attempt: 1, Phase: core.PhaseIdle, Limits: core.Limits{MaxSteps: 10, MaxAttempts: 3}}
	to, err := from.Next(core.RequestFrozen{})
	if err != nil {
		t.Fatal(err)
	}
	b := &ContextBudget{Tokens: 1000, Source: BudgetConfigured, Input: 1000, InputSeq: 15, InputBytes: 5000, Bytes: 5400, Projected: 1400,
		Steps: []ContextStepBytes{{1, 200}, {2, 200}, {3, 150}, {4, 300}}, Replaced: &current}
	head := ContextState{AnchorSeq: 20, FirstStep: 3, Minimal: current.Minimal, Rejection: current.Rejection}
	frozen := head
	request := &Request{Body: []byte("bytes"), ContextState: &frozen, Context: &ContextCapture{Anchor: head, Preparation: "first_request",
		Bytes: ContextBytes{Total: 3000, Steps: []ContextStepBytes{{3, 150}, {4, 300}}}, Budget: b}}
	return Transition{Seq: 20, From: from, To: to, Event: core.RequestFrozen{}, Facts: Facts{Request: request, ContextState: &head}}, b
}

func checkBudgetTransition(t Transition, current ContextState) error {
	if err := ValidateTransition(t); err != nil {
		return err
	}
	return CheckContextMutation(t, current)
}

func TestBudgetAnchorIsExactlyWhatItsRecordPrescribes(t *testing.T) {
	current := InitialContext()
	valid, _ := budgetAnchor(t, current)
	if err := checkBudgetTransition(valid, current); err != nil {
		t.Fatalf("valid budget anchor refused: %v", err)
	}
	for _, tc := range []struct {
		name   string
		damage func(*Transition, *ContextBudget)
	}{
		{"another first step", func(tr *Transition, _ *ContextBudget) {
			tr.Facts.ContextState.FirstStep, tr.Facts.Request.ContextState.FirstStep = 2, 2
		}},
		{"an anchor before its request", func(tr *Transition, _ *ContextBudget) {
			tr.Facts.ContextState.AnchorSeq, tr.Facts.Request.ContextState.AnchorSeq = 19, 19
		}},
		{"frozen under another head", func(tr *Transition, _ *ContextBudget) {
			tr.Facts.Request.ContextState = &ContextState{AnchorSeq: 20, FirstStep: 4}
		}},
		{"a projection within the budget", func(_ *Transition, b *ContextBudget) { b.Input, b.Projected = 500, 900 }},
		{"a projection the record does not add up to", func(_ *Transition, b *ContextBudget) { b.Projected = 1500 }},
		{"a gap in the replaced steps", func(_ *Transition, b *ContextBudget) { b.Steps = []ContextStepBytes{{1, 200}, {3, 150}, {4, 300}} }},
		{"replaced steps that stop before the latest", func(_ *Transition, b *ContextBudget) { b.Steps = b.Steps[:3] }},
		{"a learned source without a rejection", func(_ *Transition, b *ContextBudget) { b.Source = BudgetRejection }},
		{"usage from before the replaced head", func(_ *Transition, b *ContextBudget) {
			b.Replaced = &ContextState{AnchorSeq: 16, FirstStep: 1, Rejection: &RejectionRef{Seq: 16, Step: 1}}
		}},
		{"a protocol repair", func(tr *Transition, _ *ContextBudget) { tr.Facts.Request.Context.Preparation = "protocol_repair" }},
		{"no record", func(tr *Transition, _ *ContextBudget) { tr.Facts.Request.Context.Budget = nil }},
		{"a capture without its retained steps", func(tr *Transition, _ *ContextBudget) { tr.Facts.Request.Context.Bytes.Steps = nil }},
		{"a capture of other retained bytes", func(tr *Transition, _ *ContextBudget) {
			tr.Facts.Request.Context.Bytes.Steps = []ContextStepBytes{{3, 151}, {4, 300}}
		}},
		{"a capture of another head", func(tr *Transition, _ *ContextBudget) { tr.Facts.Request.Context.Anchor = InitialContext() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr, b := budgetAnchor(t, current)
			tc.damage(&tr, b)
			if err := checkBudgetTransition(tr, current); err == nil {
				t.Fatal("damaged budget anchor accepted")
			}
		})
	}
	t.Run("another current head", func(t *testing.T) {
		tr, _ := budgetAnchor(t, current)
		if err := CheckContextMutation(tr, ContextState{AnchorSeq: 9, FirstStep: 2}); err == nil {
			t.Fatal("a budget anchor replaced a head that is not current")
		}
	})
	unanchored := func(t *testing.T) (Transition, *ContextBudget) {
		tr, b := budgetAnchor(t, current)
		tr.Facts.ContextState, tr.Facts.Request.ContextState, tr.Facts.Request.Context.Anchor = nil, &current, current
		tr.Facts.Request.Context.Bytes.Steps = b.Steps
		b.Steps, b.Replaced, b.Bytes, b.Projected = nil, nil, 3000, 1000
		return tr, b
	}
	t.Run("a request without an anchor records its own rendering", func(t *testing.T) {
		tr, b := unanchored(t)
		if err := checkBudgetTransition(tr, current); err != nil {
			t.Fatalf("a recorded decision without an anchor refused: %v", err)
		}
		b.Replaced = &current
		if err := checkBudgetTransition(tr, current); err == nil {
			t.Fatal("a request without an anchor named a replaced head")
		}
	})
	t.Run("a request over its budget cannot keep steps that could leave", func(t *testing.T) {
		tr, b := unanchored(t)
		b.InputBytes, b.Projected = 2999, 1001
		if err := checkBudgetTransition(tr, current); err == nil || !strings.Contains(err.Error(), "could leave") {
			t.Fatalf("a request skipped the anchor its tail required: %v", err)
		}
		latest := ContextState{AnchorSeq: 7, FirstStep: 4}
		tr.Facts.Request.ContextState, tr.Facts.Request.Context.Anchor = &latest, latest
		tr.Facts.Request.Context.Bytes.Steps = []ContextStepBytes{{4, 300}}
		if err := checkBudgetTransition(tr, latest); err != nil {
			t.Fatalf("a request with only its latest step resident refused: %v", err)
		}
	})
}
