package harness

import (
	"fmt"
	"slices"

	"github.com/ChrAlpha/canti/runtime/core"
)

// ContextState is the durable selection of an epoch and its explicit evidence.
// AnchorSeq is the rejection that opened it; zero names the initial epoch.
type ContextState struct {
	AnchorSeq int64
	FirstStep int
	Focus     []EvidenceRef `json:",omitempty"`
}

func InitialContext() ContextState { return ContextState{FirstStep: 1} }

func (c ContextState) Equal(other ContextState) bool {
	return c.AnchorSeq == other.AnchorSeq && c.FirstStep == other.FirstStep && slices.Equal(c.Focus, other.Focus)
}

func (c ContextState) Validate(step int, seq int64) error {
	if c.AnchorSeq < 0 || c.AnchorSeq > seq || c.FirstStep < 1 || c.FirstStep > step || (c.AnchorSeq == 0 && c.FirstStep != 1) {
		return fmt.Errorf("harness: invalid context anchor at step %d sequence %d", step, seq)
	}
	if err := validateFocus(c.Focus, step); err != nil {
		return err
	}
	for _, r := range c.Focus {
		if r.SourceSeq >= seq {
			return fmt.Errorf("harness: focus must name prior committed evidence")
		}
	}
	return nil
}

// ContextRejection ties the provider signal to the exact context it rejected.
type ContextRejection struct {
	Previous ContextState
	Signal   ContextLimitError
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
		if rejection := t.Facts.Rejection; rejection != nil && (t.Facts.Response == nil || !rejection.Signal.Confirmed() || !rejection.Previous.Equal(current)) {
			return fmt.Errorf("%w: terminal rejection differs from context head", ErrFacts)
		}
		if next != nil {
			return fmt.Errorf("%w: a terminal failure cannot mutate context", ErrFacts)
		}
	case core.Performed:
		if next != nil {
			if next.AnchorSeq != current.AnchorSeq || next.FirstStep != current.FirstStep {
				return fmt.Errorf("%w: focus cannot change anchor", ErrFacts)
			}
			if t.Facts.Outcome == nil || t.Facts.Outcome.Focus == nil || !slices.Equal(next.Focus, t.Facts.Outcome.Focus.Sources) {
				return fmt.Errorf("%w: focus differs from outcome", ErrFacts)
			}
		}
	default:
		if next != nil {
			return fmt.Errorf("%w: context mutation does not belong to %s", ErrFacts, t.Event.Kind())
		}
	}
	return nil
}

func checkRejectedContext(t Transition, current ContextState) error {
	next := t.Facts.ContextState
	rejection := t.Facts.Rejection
	if rejection == nil || !rejection.Signal.Confirmed() || !rejection.Previous.Equal(current) {
		return fmt.Errorf("%w: rejection context differs from head", ErrFacts)
	}
	if t.To.Phase == core.PhaseIdle {
		want := ContextState{AnchorSeq: t.Seq, FirstStep: t.From.Step}
		if next == nil || !next.Equal(want) {
			return fmt.Errorf("%w: rejection must atomically open its empty context", ErrFacts)
		}
	} else if next != nil {
		return fmt.Errorf("%w: exhausted rejection cannot open a context", ErrFacts)
	}
	return nil
}

// FocusSelection is the complete replacement set a successful focus action
// recorded, including an explicitly empty set that releases prior evidence.
type FocusSelection struct{ Sources []EvidenceRef }
