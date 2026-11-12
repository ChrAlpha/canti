package harness

import (
	"fmt"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/runtime/core"
)

// ContextState is the durable materialized head of a run's context epoch.
// AnchorSeq is the transition that opened it, a provider rejection or a
// successful handover; zero is the run's effective initial head. Handover is
// the carried note and is part of the head at all times. Minimal marks the
// degradation floor: only the authoritative orientation and one notice are
// rendered. Rejection is the run's most recent confirmed provider rejection.
type ContextState struct {
	AnchorSeq int64
	FirstStep int
	Handover  *HandoverRef  `json:",omitempty"`
	Minimal   bool          `json:",omitempty"`
	Rejection *RejectionRef `json:",omitempty"`
}

// HandoverRef names the successful handover completion a head carries.
// A reference of another run names a run in the permitted line chain; the
// store checks that provenance.
type HandoverRef struct {
	RunID string
	Seq   int64
	Step  int
}

// RejectionRef names the run's most recent confirmed provider rejection and
// the largest input usage that any accepted request before it reported.
type RejectionRef struct {
	Seq                 int64
	Step                int
	MaxAcceptedInput    int64
	MaxAcceptedReported bool
}

// InitialContext is the effective head of a fresh run, and of a run that
// inherits no handover.
func InitialContext() ContextState { return ContextState{FirstStep: 1} }

// Equal reports whether h and o name the same handover, treating nil as the
// absent reference.
func (h *HandoverRef) Equal(o *HandoverRef) bool {
	if h == nil || o == nil {
		return h == o
	}
	return *h == *o
}

// Equal reports whether r and o name the same rejection record.
func (r *RejectionRef) Equal(o *RejectionRef) bool {
	if r == nil || o == nil {
		return r == o
	}
	return *r == *o
}

// Equal reports whether two heads select the same context.
func (c ContextState) Equal(other ContextState) bool {
	return c.AnchorSeq == other.AnchorSeq && c.FirstStep == other.FirstStep &&
		c.Handover.Equal(other.Handover) && c.Minimal == other.Minimal &&
		c.Rejection.Equal(other.Rejection)
}

// Validate checks the head's shape against the position it is read at. The run
// and line provenance of Handover and Rejection belongs to the store.
func (c ContextState) Validate(step int, seq int64) error {
	if c.AnchorSeq < 0 || c.AnchorSeq > seq || c.FirstStep < 1 || c.FirstStep > step {
		return fmt.Errorf("harness: invalid context anchor at step %d sequence %d", step, seq)
	}
	if c.AnchorSeq == 0 && (c.FirstStep != 1 || c.Minimal || c.Rejection != nil) {
		return fmt.Errorf("harness: the initial head holds only its first step")
	}
	if c.Handover != nil {
		if c.Handover.RunID == "" || !utf8.ValidString(c.Handover.RunID) || c.Handover.Seq < 1 || c.Handover.Step < 1 {
			return fmt.Errorf("harness: invalid carried handover reference")
		}
	}
	if c.Rejection != nil {
		r := c.Rejection
		if c.AnchorSeq == 0 || r.Seq < 1 || r.Seq > c.AnchorSeq || r.Step < 1 || r.MaxAcceptedInput < 0 {
			return fmt.Errorf("harness: invalid rejection reference")
		}
	}
	return nil
}

// ContextRejection ties the provider signal to the exact context it rejected
// and records what the rejected request showed. Head is a pure function of
// these fields and the head it rejected.
type ContextRejection struct {
	Previous ContextState
	Signal   ContextLimitError
	// EpochSteps is the number of resident epoch step blocks the rejected
	// request rendered; Repair reports a protocol repair; OptionalContent
	// reports a carried handover, an index, previous runs or observations;
	// Minimal is the rejected head's degradation floor.
	EpochSteps          int
	Repair              bool
	OptionalContent     bool
	Minimal             bool
	MaxAcceptedInput    int64
	MaxAcceptedReported bool
}

// RejectionHead classifies what a rejection opens.
type RejectionHead int

const (
	// RejectionOpensHead opens a new head at the same unfinished step.
	RejectionOpensHead RejectionHead = iota
	// RejectionTerminalMinimal rejects the minimal context.
	RejectionTerminalMinimal
	// RejectionTerminalOrientation rejects authoritative orientation alone.
	RejectionTerminalOrientation
)

// Head returns the head a rejection opens and its class. The table is
// exhaustive over resident steps, repair, optional content and Minimal.
func (r ContextRejection) Head(seq int64, step int, current ContextState) (ContextState, RejectionHead) {
	switch {
	case r.EpochSteps > 0 || r.Repair:
		return r.head(seq, step, current, current.Minimal), RejectionOpensHead
	case r.Minimal:
		return ContextState{}, RejectionTerminalMinimal
	case r.OptionalContent:
		return r.head(seq, step, current, true), RejectionOpensHead
	default:
		return ContextState{}, RejectionTerminalOrientation
	}
}

func (r ContextRejection) head(seq int64, step int, current ContextState, minimal bool) ContextState {
	return ContextState{
		AnchorSeq: seq, FirstStep: step, Handover: current.Handover, Minimal: minimal,
		Rejection: &RejectionRef{Seq: seq, Step: step, MaxAcceptedInput: r.MaxAcceptedInput, MaxAcceptedReported: r.MaxAcceptedReported},
	}
}

// CheckContextMutation validates a context-bearing transition against the
// current materialized head, within the same transaction that extends it.
func CheckContextMutation(t Transition, current ContextState) error {
	if r := t.Facts.Request; r != nil {
		if r.ContextState == nil {
			return fmt.Errorf("%w: frozen context is required", ErrFacts)
		}
		if !current.Equal(*r.ContextState) {
			return fmt.Errorf("%w: frozen context differs from current head", ErrFacts)
		}
	}
	next := t.Facts.ContextState
	if next != nil {
		if err := next.Validate(t.To.Step, t.Seq); err != nil {
			return fmt.Errorf("%w: %v", ErrFacts, err)
		}
	}
	switch t.Event.(type) {
	case core.RequestRejected:
		return checkRejectedContext(t, current)
	case core.StopRequested:
		if rejection := t.Facts.Rejection; rejection != nil && (t.Facts.Response == nil || !rejection.Signal.Confirmed() || !rejection.Previous.Equal(current) || rejection.Minimal != current.Minimal) {
			return fmt.Errorf("%w: terminal rejection differs from context head", ErrFacts)
		}
		if next != nil {
			return fmt.Errorf("%w: a terminal failure cannot mutate context", ErrFacts)
		}
	case core.Performed:
		return checkHandoverHead(t, current)
	default:
		if next != nil {
			return fmt.Errorf("%w: context mutation does not belong to %s", ErrFacts, t.Event.Kind())
		}
	}
	return nil
}

// checkHandoverHead enforces the bidirectional handover invariant: a
// successful handover outcome carries exactly its head, and no other action
// carries one.
func checkHandoverHead(t Transition, current ContextState) error {
	next, o := t.Facts.ContextState, t.Facts.Outcome
	if next == nil {
		if o != nil && o.Handover != nil {
			return fmt.Errorf("%w: a successful handover must open its head", ErrFacts)
		}
		return nil
	}
	if o == nil || o.Status != OutcomeDone || o.Handover == nil {
		return fmt.Errorf("%w: only a successful handover changes context", ErrFacts)
	}
	want := ContextState{
		AnchorSeq: t.Seq, FirstStep: t.To.Step,
		Handover:  &HandoverRef{RunID: t.From.RunID, Seq: t.Seq, Step: t.From.Step},
		Rejection: current.Rejection,
	}
	if !next.Equal(want) {
		return fmt.Errorf("%w: handover head differs from its committed position", ErrFacts)
	}
	return nil
}

func checkRejectedContext(t Transition, current ContextState) error {
	r := t.Facts.Rejection
	if r == nil || !r.Signal.Confirmed() || !r.Previous.Equal(current) {
		return fmt.Errorf("%w: rejection context differs from head", ErrFacts)
	}
	if r.EpochSteps < 0 || r.Minimal != current.Minimal {
		return fmt.Errorf("%w: rejection shape differs from the rejected head", ErrFacts)
	}
	if t.To.Phase != core.PhaseIdle {
		if t.Facts.ContextState != nil {
			return fmt.Errorf("%w: exhausted rejection cannot open a context", ErrFacts)
		}
		return nil
	}
	want, class := r.Head(t.Seq, t.From.Step, current)
	if class != RejectionOpensHead {
		return fmt.Errorf("%w: a terminal rejection must stop the run", ErrFacts)
	}
	if t.Facts.ContextState == nil || !t.Facts.ContextState.Equal(want) {
		return fmt.Errorf("%w: rejection must open exactly its head", ErrFacts)
	}
	return nil
}

// HandoverText returns the text of a handover payload, the part after the
// action word and its one separator, verbatim.
func HandoverText(payload string) (string, error) {
	word, args, reserved := actionWord(payload)
	if !reserved || word != handoverName {
		return "", fmt.Errorf("harness: payload is not a handover")
	}
	if args > len(payload) {
		return "", fmt.Errorf("harness: handover arguments are outside the payload")
	}
	return payload[args:], nil
}

// newIntegrityError reports committed evidence that no longer matches the
// reference that names it: damage to the journal, which no other selection
// repairs and nothing reads again from its source, so it stops the run.
func newIntegrityError(format string, args ...any) error {
	return contextError{fmt.Errorf("harness: journal integrity: "+format, args...)}
}
