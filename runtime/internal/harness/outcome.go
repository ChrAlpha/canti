package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

// OutcomeStatus is how an action the harness performed itself ended.
type OutcomeStatus string

const (
	OutcomeDone   OutcomeStatus = "done"
	OutcomeFailed OutcomeStatus = "failed"
)

// Outcome is the result of an action the harness performed itself. No
// process ran, so it has no execution state, exit code, or output. Detail
// is a failure or refusal, clipped to actionFailureBytes and a truncation
// marker; Retrieval is a performed read, also one the archive reports an
// error for; Focus is an accepted replacement selection.
type Outcome struct {
	Status    OutcomeStatus
	Detail    string             `json:",omitempty"`
	Retrieval *archive.Retrieval `json:",omitempty"`
	Focus     *FocusSelection    `json:",omitempty"`
}

const actionFailure, actionFailureBytes = "Context action failed: ", 2048

// maxDetailBytes bounds Detail: the clipped text and its marker.
var maxDetailBytes = actionFailureBytes + len("…[truncated "+strconv.Itoa(1<<62)+" bytes]")

// failed returns the outcome of an action that err stopped.
func failed(err error) Outcome {
	return Outcome{Status: OutcomeFailed, Detail: clip(err.Error(), actionFailureBytes)}
}

// Validate checks that o is a shape some action's performance yields.
func (o Outcome) Validate() error {
	if len(o.Detail) > maxDetailBytes {
		return errors.New("harness: outcome detail exceeds resource limit")
	}
	switch o.Status {
	case OutcomeDone:
		if o.Detail == "" && (o.Retrieval != nil) != (o.Focus != nil) && (o.Retrieval == nil || o.Retrieval.Error == "") {
			return nil
		}
	case OutcomeFailed:
		if o.Focus == nil && (o.Detail != "") != (o.Retrieval != nil) && (o.Retrieval == nil || o.Retrieval.Error != "") {
			return nil
		}
	}
	return errors.New("harness: invalid local outcome")
}

// presented renders a local outcome using the current context's execution
// vocabulary. It is a presentation rule, never a physical execution fact.
func (r StepRecord) presented() Observation {
	if r.Superseded != nil {
		return Observation{Execution: core.ExecutionSuperseded}
	}
	o := r.Outcome
	if o == nil {
		return r.Observation
	}
	p := Observation{Retrieval: o.Retrieval}
	code := 0
	if o.Status == OutcomeFailed {
		code = 1
	}
	p.Execution, p.ExitCode = core.ExecutionExited, &code
	switch {
	case o.Detail != "":
		p.Preview = actionFailure + o.Detail
	case o.Focus != nil:
		p.Preview = fmt.Sprintf("Focus replaced with %d frozen source ranges.", len(o.Focus.Sources))
	case o.Retrieval != nil:
		refs := *o.Retrieval
		refs.Evidence = nil
		body, _ := json.Marshal(refs)
		p.Preview = string(body)
	}
	return p
}

// Validate checks a current-format completion: a performed step has an
// Outcome and optional next ContextState; external steps have only an Observation.
func (r StepRecord) Validate(semantics int, performed bool) error {
	if err := core.KnownSemantics(semantics); err != nil {
		return err
	}
	if r.Superseded != nil {
		if performed || r.Outcome != nil || r.Observation != (Observation{}) || r.Resolved {
			return errors.New("harness: a superseded step has no outcome or observation")
		}
		if r.Superseded.Instruction < 1 || (r.Superseded.Kind != core.Exec && r.Superseded.Kind != core.Return) {
			return errors.New("harness: invalid superseded decision")
		}
		return nil
	}
	if performed {
		if r.Outcome == nil || r.Observation != (Observation{}) {
			return errors.New("harness: a performed step needs an outcome and no observation")
		}
		if err := r.Outcome.Validate(); err != nil {
			return err
		}
		return nil
	}
	switch {
	case r.Outcome != nil || r.Observation.Execution == "":
		return errors.New("harness: an observed or resolved step needs an observation and no outcome or context mutation")
	case r.Observation.Retrieval != nil:
		return errors.New("harness: an observation of this version holds no retrieval or focus")
	}
	return nil
}

// retrieval returns the read that the local outcome records.
func (r StepRecord) retrieval() *archive.Retrieval {
	if r.Outcome != nil {
		return r.Outcome.Retrieval
	}
	return nil
}
