package harness

import (
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/core"
)

// EffectTransition reports whether CheckEffect applies to t: the accepting
// ResponseLoaded of an exec decision and every event that binds, starts,
// completes, or settles its effect.
func EffectTransition(t Transition) bool {
	switch t.Event.(type) {
	case core.ResponseLoaded:
		return t.To.Phase == core.PhaseEffectSaved
	case core.JobBound, core.JobStarted, core.Observed, core.Performed, core.OutcomeUnknown, core.Resolved:
		return t.From.EffectID != ""
	}
	return false
}

// CheckEffect reports whether t, a transition EffectTransition accepts,
// agrees with accepted, the committed acceptance of its effect, which an
// accepting ResponseLoaded carries itself, and with source, the window of
// the request the effect was accepted under, nil when it has none. A store
// applies it inside the transaction that commits t: committed rows never
// change, so the rule holds for every writer.
func CheckEffect(t Transition, accepted Accepted, source *Window) error {
	var w Window
	if source != nil {
		w = *source
	}
	if _, ok := t.Event.(core.ResponseLoaded); ok {
		a, p := t.Facts.Action, t.To.Decision.Payload
		if a == nil || *a != NormalizeAction(p, w.Requested, true) && *a != NormalizeAction(p, w.Requested, false) {
			return fmt.Errorf("harness: action %+v is not the reading of the accepted decision", a)
		}
		return nil
	}
	legacy, recovery := accepted.Provenance == ProvenanceLegacy, accepted.Action.Recovery
	switch e := t.Event.(type) {
	case core.Performed:
		if legacy || !recovery.Local() {
			return fmt.Errorf("harness: effect %s of a %s acceptance with recovery %q cannot be performed", e.EffectID, accepted.Provenance, recovery)
		}
		return checkPerformed(t, accepted.Action, w)
	case core.Observed:
		switch {
		case t.From.JobID == "" && t.From.Phase == core.PhaseCancelling:
			return nil
		case t.From.JobID == "" && !legacy:
			return errors.New("harness: only an effect accepted before actions were recorded is observed unstarted without a job")
		}
	}
	if !legacy && recovery != RecoveryExternal {
		return fmt.Errorf("harness: %s of an effect whose action has recovery %q", t.Event.Kind(), recovery)
	}
	return nil
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
	case a.Name == ActionHandover:
		ok = o.Status != OutcomeRefused && o.Retrieval == nil
		if ok && o.Status == OutcomeDone {
			text, err := a.arguments(t.From.Decision.Payload)
			ok = err == nil && w.Requested && o.Handover != nil && *o.Handover == Handover{RunID: t.From.RunID, FirstStep: w.FirstStep, Text: text}
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
