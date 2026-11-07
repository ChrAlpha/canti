package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
)

// OutcomeStatus is how an action the harness performed itself ended.
type OutcomeStatus string

const (
	OutcomeDone    OutcomeStatus = "done"
	OutcomeFailed  OutcomeStatus = "failed"
	OutcomeRefused OutcomeStatus = "refused"
)

// Outcome is the result of an action the harness performed itself. No
// process ran, so it has no execution state, exit code, or output. Detail
// is a failure or refusal, clipped to actionFailureBytes and a truncation
// marker; Retrieval is a performed read, also one the archive reports an
// error for; Handover is an accepted handover.
type Outcome struct {
	Status    OutcomeStatus
	Detail    string             `json:",omitempty"`
	Retrieval *archive.Retrieval `json:",omitempty"`
	Handover  *Handover          `json:",omitempty"`
}

const actionFailure, actionFailureBytes = "Context action failed: ", 2048

// handoverNotice is the preview of an accepted handover.
const handoverNotice = "Handover recorded; the next request starts a new context window."

// maxDetailBytes bounds Detail: the clipped text and its marker.
var maxDetailBytes = actionFailureBytes + len("…[truncated "+strconv.Itoa(1<<62)+" bytes]")

// failed returns the outcome of an action that err stopped.
func failed(err error) Outcome {
	return Outcome{Status: OutcomeFailed, Detail: clip(err.Error(), actionFailureBytes)}
}

// Validate checks that o is a shape some action's performance yields.
func (o Outcome) Validate() error {
	shape := fmt.Sprintf("%s outcome with detail %v, retrieval %v, handover %v", o.Status, o.Detail != "", o.Retrieval != nil, o.Handover != nil)
	switch {
	case len(o.Detail) > maxDetailBytes:
		return fmt.Errorf("harness: outcome detail of %d bytes exceeds %d", len(o.Detail), maxDetailBytes)
	case o.Status == OutcomeDone && o.Detail == "" && (o.Retrieval != nil) != (o.Handover != nil) && (o.Retrieval == nil || o.Retrieval.Error == ""):
	case o.Status == OutcomeFailed && o.Handover == nil && (o.Detail != "") != (o.Retrieval != nil) && (o.Retrieval == nil || o.Retrieval.Error != ""):
	case o.Status == OutcomeRefused && o.Detail != "" && o.Retrieval == nil && o.Handover == nil:
	default:
		return fmt.Errorf("harness: invalid %s", shape)
	}
	return nil
}

// presented is the observation that context version 12 recorded for the
// same step, which the prompt and the published step records show for a
// local outcome: done as an exit code of 0, failed as 1, and refused as a
// failed start, with the preview that version composed. It is a rendering
// rule, not a fact.
func (r StepRecord) presented() Observation {
	o := r.Outcome
	if o == nil {
		return r.Observation
	}
	p := Observation{Retrieval: o.Retrieval, Handover: o.Handover}
	if o.Status == OutcomeRefused {
		p.Execution, p.Preview = core.ExecutionFailedToStart, actionFailure+o.Detail
		return p
	}
	code := 0
	if o.Status == OutcomeFailed {
		code = 1
	}
	p.Execution, p.ExitCode = core.ExecutionExited, &code
	switch {
	case o.Detail != "":
		p.Preview = actionFailure + o.Detail
	case o.Retrieval != nil:
		refs := *o.Retrieval
		refs.Evidence = nil
		body, _ := json.Marshal(refs)
		p.Preview = string(body)
	default:
		p.Preview = handoverNotice
	}
	return p
}

// Validate checks the shape of a completed step that a transition of
// semantics recorded, performed when performed is set: a performed step has
// a valid outcome, any window valid for the next step, and no observation;
// any other step has an observation and no outcome or window, and, from
// ActionSemantics on, no retrieval or handover in its observation, which only
// earlier versions recorded there. Reads apply it to every step they return.
func (r StepRecord) Validate(semantics int, performed bool) error {
	if performed {
		if r.Outcome == nil || r.Observation != (Observation{}) {
			return errors.New("harness: a performed step needs an outcome and no observation")
		}
		if err := r.Outcome.Validate(); err != nil {
			return err
		}
		if r.Window != nil {
			return r.Window.Validate(r.Step + 1)
		}
		return nil
	}
	switch {
	case r.Outcome != nil || r.Window != nil || r.Observation.Execution == "":
		return errors.New("harness: an observed or resolved step needs an observation and no outcome or window")
	case semantics >= ActionSemantics && (r.Observation.Retrieval != nil || r.Observation.Handover != nil):
		return errors.New("harness: an observation of this version holds no retrieval or handover")
	}
	return nil
}

// retrieval returns the read that step r performed, recorded as an outcome
// or, by earlier versions, in an observation.
func (r StepRecord) retrieval() *archive.Retrieval {
	if r.Outcome != nil {
		return r.Outcome.Retrieval
	}
	return r.Observation.Retrieval
}

// acceptedHandover returns the handover step r accepted, or nil.
func (r StepRecord) acceptedHandover() *Handover {
	if r.Outcome != nil {
		if r.Outcome.Status == OutcomeDone {
			return r.Outcome.Handover
		}
		return nil
	}
	return r.Observation.acceptedHandover()
}

// windowAfter returns the window that the step after step renders, when a
// local action performed at step, under the window source of its request,
// ended with outcome o: the window an accepted handover opens, with the
// evidence it carries; otherwise,
// when source requires a handover, source with one more failure; and nil
// when the window carries on unchanged.
func windowAfter(step int, source Window, o Outcome) *Window {
	if o.Status == OutcomeDone && o.Handover != nil {
		return &Window{FirstStep: step + 1, SourceStep: step, Handover: o.Handover.Text, Carry: o.Handover.Carry}
	}
	if source.Requested {
		source.Failures++
		return &source
	}
	return nil
}

// sameWindow reports whether a and b are both absent or equal.
func sameWindow(a, b *Window) bool {
	return a == nil && b == nil || a != nil && b != nil && a.Equal(*b)
}
