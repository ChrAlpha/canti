package harness

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Frame is the input assembled by the harness for one model request:
// committed facts of the run. Prompt renders it into
// one independent, bounded Prompt. The core never receives a Frame or any
// other history representation.
type Frame struct {
	Goal, Constraints, Environment                 string
	Step, MaxSteps, Attempt, MaxAttempts           int
	RunID                                          string
	Rejected                                       *Rejection
	ContextState                                   *ContextState
	Receipts                                       []StepReceipt
	Latest                                         *StepRecord
	DecisionTool, RetrievalEnabled, ImportsEnabled bool
	SessionContext                                 string
	Focused                                        []FocusedEvidence
	capture                                        *ContextCapture
	sessionNavigation                              string
}

// Rejection is a response of the current step that failed to decode, with
// the decode error.
type Rejection struct{ Error, Response string }

// Validate checks the minimum information needed to construct a model input
// and that receipts account for every completed step since the anchor.
func (f Frame) Validate() error {
	if strings.TrimSpace(f.Goal) == "" || strings.TrimSpace(f.Environment) == "" || strings.TrimSpace(f.RunID) == "" {
		return errors.New("harness: frame goal, environment and run ID are required")
	}
	if !utf8.ValidString(f.Goal) || !utf8.ValidString(f.Constraints) || !utf8.ValidString(f.Environment) {
		return errors.New("harness: authoritative orientation must be valid UTF-8")
	}
	if f.MaxSteps < 0 || f.Step < 1 || f.MaxSteps > 0 && f.Step > f.MaxSteps || f.Attempt < 1 || f.Attempt > f.MaxAttempts {
		return errors.New("harness: invalid frame execution position")
	}
	if f.Rejected != nil && f.Attempt < 2 {
		return errors.New("harness: repair requires a prior attempt")
	}
	if f.ContextState == nil {
		return errors.New("harness: frame context state is required")
	}
	c := f.contextState()
	if c.FirstStep < 1 || c.FirstStep > f.Step {
		return errors.New("harness: invalid frame anchor")
	}
	if len(f.Receipts) != f.Step-c.FirstStep {
		return fmt.Errorf("harness: missing receipts between anchor step %d and step %d", c.FirstStep, f.Step)
	}
	for i, r := range f.Receipts {
		if r.Step != c.FirstStep+i || r.Seq <= c.AnchorSeq {
			return errors.New("harness: invalid receipt identity or anchor boundary")
		}
	}
	if f.Latest != nil && f.Latest.Step != f.Step-1 {
		return errors.New("harness: latest result is not the previous completed step")
	}
	return nil
}

func (f Frame) contextState() ContextState {
	return *f.ContextState
}
