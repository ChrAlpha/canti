package harness

import (
	"fmt"
	"slices"
)

// The sources of a request's context budget.
const (
	// BudgetConfigured is the run's stored Budget.ContextTokens.
	BudgetConfigured = "configured"
	// BudgetRejection is the largest input an accepted request reported
	// before the run's latest confirmed overflow.
	BudgetRejection = "rejection"
)

// ContextBudget records the context budget decision of one frozen request.
// Input and InputBytes are the provider-reported input tokens and rendered
// total of the latest request frozen under the same head, InputSeq its
// sequence; Bytes is the total this request rendered under that head, and
// Projected is Input plus the bytes it added. Steps and Replaced are present
// when the request opened a budget anchor: the resident step bytes of the
// rendering it replaced, and the head it replaced.
type ContextBudget struct {
	Tokens     int64
	Source     string
	Input      int64
	InputSeq   int64
	InputBytes int
	Bytes      int
	Projected  int64
	Steps      []ContextStepBytes `json:",omitempty"`
	Replaced   *ContextState      `json:",omitempty"`
}

// EffectiveBudget returns the context budget of a request under head: the
// smaller of configured, when positive, and the input learned from the
// head's rejection, when reported.
func EffectiveBudget(configured int64, head ContextState) (int64, string, bool) {
	var learned int64
	if r := head.Rejection; r != nil && r.MaxAcceptedReported && r.MaxAcceptedInput > 0 {
		learned = r.MaxAcceptedInput
	}
	switch {
	case configured > 0 && (learned == 0 || configured <= learned):
		return configured, BudgetConfigured, true
	case learned > 0:
		return learned, BudgetRejection, true
	}
	return 0, "", false
}

// projection is the input the request is projected to take: the comparable
// request's reported input plus the bytes this one adds. It is a projection
// from reported usage, not a bound; a provider rejection remains the fallback.
func (b ContextBudget) projection() int64 {
	return b.Input + int64(max(0, b.Bytes-b.InputBytes))
}

// TailStep returns the first step a budget anchor keeps resident. steps are
// the resident step blocks, oldest first, ending with the latest completed
// step, which always stays; before it, the newest steps whose bytes sum to at
// most a quarter of tokens stay too.
func TailStep(steps []ContextStepBytes, tokens int64) int {
	if len(steps) == 0 {
		return 0
	}
	first, used := len(steps)-1, int64(0)
	for first > 0 {
		next := used + int64(steps[first-1].Bytes)
		if next > tokens/4 {
			break
		}
		used, first = next, first-1
	}
	return steps[first].Step
}

// checkRequestBudget checks the budget record of a frozen request and, when
// the request carries a head, that the head is the budget anchor the record
// prescribes. It needs no other committed fact; the store checks the record
// against the run and the comparable request.
func checkRequestBudget(t Transition) error {
	r, head := t.Facts.Request, t.Facts.ContextState
	var b *ContextBudget
	if r.Context != nil {
		if err := checkCaptureShape(t); err != nil {
			return err
		}
		b = r.Context.Budget
	}
	if head == nil {
		return checkUnanchoredBudget(t, b)
	}
	return checkBudgetAnchor(t, *head, b)
}

// checkCaptureShape checks that a request's capture names the head it was
// frozen under.
func checkCaptureShape(t Transition) error {
	r := t.Facts.Request
	if !r.Context.Anchor.Equal(*r.ContextState) {
		return fmt.Errorf("%w: the context capture names another head", ErrFacts)
	}
	return nil
}

// CheckBudgetedCapture checks a request frozen while a context budget is in
// force: it has a capture, the capture renders exactly the resident steps of
// the head it was frozen under, and its preparation is the one its revision
// allows, where retried tells whether the request retries a rejection.
func CheckBudgetedCapture(t Transition, retried bool) error {
	r := t.Facts.Request
	c := r.Context
	if c == nil {
		return fmt.Errorf("%w: a request under a context budget records its capture", ErrFacts)
	}
	first, b := r.ContextState.FirstStep, c.Bytes
	if len(b.Steps) != t.From.Step-first {
		return fmt.Errorf("%w: the context capture does not render the resident epoch", ErrFacts)
	}
	total := b.System + b.Goal + b.Constraints + b.HeadInstructions + b.Environment + b.Anchor + b.Previous + b.Index +
		b.Notice + b.EpochHeader + b.EpochInstructions + b.Observations + b.Progress + b.Latest + b.Repair
	for i, s := range b.Steps {
		if s.Step != first+i || s.Bytes < 0 {
			return fmt.Errorf("%w: the context capture does not render the resident epoch", ErrFacts)
		}
		total += s.Bytes
	}
	if total != b.Total {
		return fmt.Errorf("%w: the context capture's sections sum to %d, not its total %d", ErrFacts, total, b.Total)
	}
	want := "protocol_repair"
	switch {
	case t.To.RequestRev == 1:
		want = "first_request"
	case retried:
		want = "request_rejected"
	}
	if c.Preparation != want {
		return fmt.Errorf("%w: preparation %q, want %q", ErrFacts, c.Preparation, want)
	}
	return nil
}

// checkUnanchoredBudget checks a request that opens no head: its record
// describes its own rendering, and a projection over the budget left no
// resident step that the tail would have let leave.
func checkUnanchoredBudget(t Transition, b *ContextBudget) error {
	r := t.Facts.Request
	if r.ContextState.AnchorSeq >= t.Seq {
		return fmt.Errorf("%w: frozen anchor must precede its request", ErrFacts)
	}
	if b == nil {
		return nil
	}
	if b.Replaced != nil || len(b.Steps) > 0 || b.Bytes != r.Context.Bytes.Total || r.Context.Preparation == "protocol_repair" {
		return fmt.Errorf("%w: a request without a budget anchor records its own rendering, and a repair none", ErrFacts)
	}
	if err := checkBudgetRecord(b, *r.ContextState, t.Seq); err != nil {
		return err
	}
	if b.Projected > b.Tokens && TailStep(r.Context.Bytes.Steps, b.Tokens) > r.ContextState.FirstStep {
		return fmt.Errorf("%w: a request over its budget kept steps that could leave", ErrFacts)
	}
	return nil
}

// checkBudgetAnchor checks that head is the budget anchor b prescribes and
// that the anchoring request renders exactly the steps it retains.
func checkBudgetAnchor(t Transition, head ContextState, b *ContextBudget) error {
	r := t.Facts.Request
	switch {
	case b == nil || b.Replaced == nil:
		return fmt.Errorf("%w: only a budget anchor opens a head on a frozen request", ErrFacts)
	case !head.Equal(*r.ContextState):
		return fmt.Errorf("%w: a budget anchor freezes its own head", ErrFacts)
	case r.Context.Preparation == "protocol_repair":
		return fmt.Errorf("%w: a protocol repair opens no budget anchor", ErrFacts)
	}
	replaced := *b.Replaced
	if err := checkBudgetRecord(b, replaced, t.Seq); err != nil {
		return err
	}
	if b.Projected <= b.Tokens {
		return fmt.Errorf("%w: a budget anchor needs a projection over its budget", ErrFacts)
	}
	if err := checkReplacedSteps(b.Steps, replaced.FirstStep, t.From.Step); err != nil {
		return err
	}
	want := ContextState{AnchorSeq: t.Seq, FirstStep: TailStep(b.Steps, b.Tokens), Minimal: replaced.Minimal, Rejection: replaced.Rejection}
	if want.FirstStep <= replaced.FirstStep || !head.Equal(want) {
		return fmt.Errorf("%w: budget anchor head differs from its tail", ErrFacts)
	}
	if !slices.Equal(r.Context.Bytes.Steps, b.Steps[want.FirstStep-replaced.FirstStep:]) {
		return fmt.Errorf("%w: the anchoring request renders other steps than it retains", ErrFacts)
	}
	return nil
}

// checkBudgetRecord checks what a budget record states about itself and the
// head it was computed under.
func checkBudgetRecord(b *ContextBudget, under ContextState, seq int64) error {
	if b == nil {
		return nil
	}
	tokens, source, ok := EffectiveBudget(0, under)
	switch {
	case b.Tokens < 1 || b.Input < 0 || b.InputBytes < 0 || b.Bytes < 0:
		return fmt.Errorf("%w: invalid context budget record", ErrFacts)
	case b.Source != BudgetConfigured && b.Source != BudgetRejection:
		return fmt.Errorf("%w: unknown context budget source %q", ErrFacts, b.Source)
	case b.Source == BudgetRejection && (!ok || source != BudgetRejection || tokens != b.Tokens):
		return fmt.Errorf("%w: a learned budget differs from its rejection", ErrFacts)
	case b.Source == BudgetConfigured && ok && tokens < b.Tokens:
		return fmt.Errorf("%w: a configured budget above the learned one is not in force", ErrFacts)
	case b.InputSeq < under.AnchorSeq || b.InputSeq < 1 || b.InputSeq >= seq:
		return fmt.Errorf("%w: budget usage comes from another head", ErrFacts)
	case b.Projected != b.projection():
		return fmt.Errorf("%w: budget projection differs from its record", ErrFacts)
	}
	return nil
}

// checkReplacedSteps checks that steps are exactly the resident step blocks
// from first to the step before step.
func checkReplacedSteps(steps []ContextStepBytes, first, step int) error {
	if len(steps) != step-first || len(steps) < 2 {
		return fmt.Errorf("%w: a budget anchor replaces the whole resident epoch", ErrFacts)
	}
	for i, s := range steps {
		if s.Step != first+i || s.Bytes < 0 {
			return fmt.Errorf("%w: invalid replaced step %d", ErrFacts, s.Step)
		}
	}
	return nil
}
