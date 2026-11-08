package harness

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ChrAlpha/canti/runtime/core"
)

// EffectTransition identifies commits whose accepted recovery and execution
// evidence must be checked inside the store transaction.
func EffectTransition(t Transition) bool {
	switch t.Event.(type) {
	case core.ResponseLoaded:
		return t.To.Phase == core.PhaseEffectSaved
	case core.EffectRecovered, core.Observed, core.Performed, core.OutcomeUnknown, core.Resolved:
		return t.From.EffectID != ""
	}
	return false
}

// CheckEffect validates completion/recovery against committed acceptance and
// immutable configuration bytes, without backend I/O or adapter interpretation.
func CheckEffect(t Transition, accepted Accepted, source *Window, config []byte) error {
	if err := t.Facts.check(t.Event, t.To); err != nil {
		return err
	}
	var w Window
	if source != nil {
		w = *source
	}
	if _, ok := t.Event.(core.ResponseLoaded); ok {
		return checkAcceptance(t, w, config)
	}
	if _, err := Acceptance(accepted.Semantics, &accepted.Action, accepted.Execution); err != nil {
		return err
	}
	if accepted.Local() {
		return checkLocalEffect(t, accepted.Action, w)
	}
	if accepted.Execution.ConfigDigest != ConfigDigest(config) {
		return errors.New("harness: committed execution identity names another configuration")
	}
	effect := Effect{ID: t.From.EffectID, Command: t.From.Decision.Payload, Spec: accepted.Execution.Spec}
	if receipt := t.Facts.Receipt; receipt != nil {
		if err := receipt.Validate(effect); err != nil {
			return err
		}
	}
	switch t.Event.(type) {
	case core.OutcomeUnknown:
		return nil
	case core.EffectRecovered:
		if r := t.Facts.Receipt; r == nil || r.Status != JobPending && r.Status != JobRunning {
			return errors.New("harness: recovery requires pending or running owner evidence")
		}
		return nil
	case core.Observed:
		return checkReceipt(t.Facts)
	case core.Resolved:
		return checkResolution(t.Facts)
	}
	return fmt.Errorf("harness: %s cannot perform external work", t.Event.Kind())
}

func checkAcceptance(t Transition, w Window, config []byte) error {
	a, p := t.Facts.Action, t.To.Decision.Payload
	if a == nil || *a != NormalizeAction(p, w.Requested, true) && *a != NormalizeAction(p, w.Requested, false) {
		return errors.New("harness: action does not match the accepted decision")
	}
	if e := t.Facts.Execution; e != nil && e.ConfigDigest != ConfigDigest(config) {
		return errors.New("harness: accepted execution identity names another configuration")
	}
	return nil
}

func checkLocalEffect(t Transition, a Action, w Window) error {
	switch t.Event.(type) {
	case core.Performed:
		return checkPerformed(t, a, w)
	case core.Observed:
		if o := t.Facts.Observation; t.From.Phase == core.PhaseCancelling && t.Facts.Receipt == nil && o.Execution == core.ExecutionCancelled && o.ExitCode == nil && o.Output == (ArtifactRef{}) && o.Preview == "Context action cancelled." {
			return nil
		}
	}
	return fmt.Errorf("harness: %s cannot complete or recover local work", t.Event.Kind())
}

func checkReceipt(f Facts) error {
	r, o := f.Receipt, f.Observation
	if r == nil || o == nil {
		return errors.New("harness: external completion requires receipt and observation")
	}
	valid := false
	switch o.Execution {
	case core.ExecutionExited:
		valid = r.Status == JobSucceeded || r.Status == JobFailed
	case core.ExecutionCancelled:
		valid = r.Status == JobCancelled
	case core.ExecutionTimedOut:
		valid = r.Status == JobCancelled && r.ID != "" && !r.StartedAt.IsZero()
	case core.ExecutionFailedToStart:
		valid = r.Status == JobFailedToStart
	}
	if !valid || !sameCode(r.ExitCode, o.ExitCode) || r.Preview != o.Preview || r.Output != o.Output {
		return errors.New("harness: observation contradicts its terminal receipt")
	}
	return nil
}

func sameCode(a, b *int) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }

func checkResolution(f Facts) error {
	p, o := f.Resolution, f.Observation
	if p == nil || o == nil {
		return errors.New("harness: resolution evidence is required")
	}
	switch p.Source {
	case ResolutionInspection:
		if f.Receipt == nil || p.OwnerID != f.Receipt.ID {
			return errors.New("harness: inspected resolution has inconsistent owner provenance")
		}
		return checkReceipt(f)
	case ResolutionOperator:
		if f.Receipt != nil {
			return errors.New("harness: operator assertion is not an executor receipt")
		}
		if o.Execution != core.ExecutionUnknown && strings.TrimSpace(o.Preview) == "" {
			return errors.New("harness: known operator resolution requires an evidence note")
		}
		if (o.Execution == core.ExecutionExited || o.Execution == core.ExecutionTimedOut) && strings.TrimSpace(p.OwnerID) == "" {
			return errors.New("harness: exited or timed-out operator resolution requires an explicit owner")
		}
		if (o.Execution == core.ExecutionExited) != (o.ExitCode != nil) {
			return errors.New("harness: only exited operator resolution holds an exit code")
		}
		if o.Output != (ArtifactRef{Detail: "resolved by operator"}) {
			return errors.New("harness: operator assertion cannot fabricate retained output")
		}
		return nil
	}
	return errors.New("harness: unknown resolution source")
}

// checkPerformed reports whether the outcome and window of t, which performs
// a, agree with a and with w, the window of its request.
func checkPerformed(t Transition, a Action, w Window) error {
	o := t.Facts.Outcome
	if o == nil {
		return errors.New("harness: a performed effect has no outcome")
	}
	if err := o.Validate(); err != nil {
		return err
	}
	ok := true
	switch {
	case a.Refused != "":
		ok = o.Status == OutcomeRefused && o.Detail == problemText(a.Refused)
	case a.Invalid != "":
		ok = o.Status == OutcomeFailed && o.Detail == problemText(a.Invalid) && o.Retrieval == nil
	case a.Name == ActionRetrieve:
		ok = o.Status != OutcomeRefused && o.Handover == nil
	case a.Name.handover():
		ok = o.Status != OutcomeRefused && o.Retrieval == nil
		if ok && o.Status == OutcomeDone {
			ok = w.Requested && acceptedHandover(t, a, w.FirstStep)
		}
	default:
		ok = false
	}
	if !ok {
		return fmt.Errorf("harness: %s outcome does not follow from its %s action", o.Status, a.Name)
	}
	if want := windowAfter(t.From.Step, w, *o); !sameWindow(want, t.Facts.Window) {
		return fmt.Errorf("harness: the committed window %+v does not follow from the window %+v of its request", t.Facts.Window, w)
	}
	return nil
}

// acceptedHandover reports whether the handover of t's done outcome is one
// that performing a, under a window whose first step is first, accepts: for
// the run and window of t, with the verbatim text of a plain handover and no
// carry, or, for a carry, whose arguments only performance decodes, a text
// and a carry valid for the next step whose transitions precede t.
func acceptedHandover(t Transition, a Action, first int) bool {
	h := t.Facts.Outcome.Handover
	if h == nil || h.RunID != t.From.RunID || h.FirstStep != first {
		return false
	}
	if a.Name == ActionHandover {
		text, err := a.arguments(t.From.Decision.Payload)
		return err == nil && h.Text == text && len(h.Carry) == 0
	}
	if strings.TrimSpace(h.Text) == "" || len(h.Carry) == 0 || validateCarry(h.Carry, t.From.Step+1) != nil {
		return false
	}
	return h.Carry[len(h.Carry)-1].SourceSeq < t.Seq
}
