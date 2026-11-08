package harness

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/runtime/core"
)

// Window is the context window a request renders: its first step, the
// handover that opened it and the step that proposed it, whether a handover
// is required, the failed proposals since, and the committed evidence that
// handover carried into it.
type Window struct {
	FirstStep  int
	Handover   string
	SourceStep int
	Requested  bool
	Failures   int
	Carry      []EvidenceRef `json:",omitempty"`
}

// Equal reports whether w and o are the same window.
func (w Window) Equal(o Window) bool {
	return w.FirstStep == o.FirstStep && w.Handover == o.Handover && w.SourceStep == o.SourceStep &&
		w.Requested == o.Requested && w.Failures == o.Failures && slices.Equal(w.Carry, o.Carry)
}

func (w Window) Validate(step int) error {
	if w.FirstStep < 1 || w.FirstStep > step || w.SourceStep < 0 || w.SourceStep >= w.FirstStep || w.Failures < 0 || w.Failures > 2 {
		return fmt.Errorf("harness: invalid context window at step %d", step)
	}
	if !utf8.ValidString(w.Handover) || (w.FirstStep == 1 && (w.SourceStep != 0 || w.Handover != "")) || (w.FirstStep > 1 && (w.SourceStep != w.FirstStep-1 || strings.TrimSpace(w.Handover) == "")) {
		return fmt.Errorf("harness: invalid handover provenance at step %d", step)
	}
	if len(w.Carry) > 0 && w.FirstStep == 1 {
		return fmt.Errorf("harness: the first window carries evidence at step %d", step)
	}
	return validateCarry(w.Carry, w.FirstStep)
}

// Handover is an accepted handover: its run, the first step of the window
// it closes, its text, and the evidence it carries into the next window.
type Handover struct {
	RunID     string        `json:"run_id"`
	FirstStep int           `json:"first_step"`
	Text      string        `json:"text"`
	Carry     []EvidenceRef `json:"carry,omitempty"`
}

func (h Handover) Validate(runID string, firstStep, limit int) error {
	switch {
	case h.RunID != runID:
		return fmt.Errorf("harness: handover run_id must be %q", runID)
	case h.FirstStep != firstStep:
		return fmt.Errorf("harness: handover first_step must be %d, got %d", firstStep, h.FirstStep)
	case !utf8.ValidString(h.Text):
		return fmt.Errorf("harness: handover text must be valid UTF-8")
	case strings.TrimSpace(h.Text) == "":
		return fmt.Errorf("harness: handover text must not be empty")
	case len(h.Text) > limit:
		return fmt.Errorf("harness: handover text is %d UTF-8 bytes; maximum %d. Shorten the text and retry", len(h.Text), limit)
	}
	return nil
}

// acceptedHandover returns the handover o recorded when its action succeeded,
// or nil.
func (o Observation) acceptedHandover() *Handover {
	if o.Execution != core.ExecutionExited || o.ExitCode == nil || *o.ExitCode != 0 {
		return nil
	}
	return o.Handover
}
