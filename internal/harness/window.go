package harness

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

type Window struct {
	FirstStep  int
	Handover   string
	SourceStep int
	Requested  bool
}

func (w Window) Validate(step int) error {
	if w.FirstStep < 1 || w.FirstStep > step || w.SourceStep < 0 || w.SourceStep >= w.FirstStep {
		return fmt.Errorf("harness: invalid context window at step %d", step)
	}
	if !utf8.ValidString(w.Handover) || (w.FirstStep == 1 && (w.SourceStep != 0 || w.Handover != "")) || (w.FirstStep > 1 && (w.SourceStep != w.FirstStep-1 || strings.TrimSpace(w.Handover) == "")) {
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
	if h.RunID != runID || h.FirstStep != firstStep || strings.TrimSpace(h.Text) == "" || !utf8.ValidString(h.Text) || len(h.Text) > limit {
		return fmt.Errorf("harness: handover must bind this run and window and contain 1..%d UTF-8 bytes", limit)
	}
	return nil
}
