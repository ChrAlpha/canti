package harness

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

type Window struct {
	FirstStep       int
	Handover        string
	SourceStep      int
	Requested       bool
	Failures        int
	WithoutHandover bool
}

func (w Window) Validate(step int) error {
	if w.FirstStep < 1 || w.FirstStep > step || w.SourceStep < 0 || w.SourceStep >= w.FirstStep || w.Failures < 0 || w.Failures > 2 {
		return fmt.Errorf("harness: invalid context window at step %d", step)
	}
	if !utf8.ValidString(w.Handover) || w.WithoutHandover || (w.FirstStep == 1 && (w.SourceStep != 0 || w.Handover != "")) || (w.FirstStep > 1 && (w.SourceStep != w.FirstStep-1 || strings.TrimSpace(w.Handover) == "")) {
		return fmt.Errorf("harness: invalid handover provenance at step %d", step)
	}
	return nil
}

type Handover struct {
	RunID     string `json:"run_id"`
	FirstStep int    `json:"first_step"`
	Text      string `json:"text"`
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
