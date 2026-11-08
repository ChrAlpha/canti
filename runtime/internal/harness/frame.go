package harness

import (
	"errors"
	"fmt"
	"strings"
)

// Frame is the input assembled by the harness for one model request:
// committed facts of the run. Prompt renders it into
// one independent, bounded Prompt. The core never receives a Frame or any
// other history representation.
type Frame struct {
	Goal, Constraints, Environment       string
	Step, MaxSteps, Attempt, MaxAttempts int
	Earlier                              int          // completed steps before Steps that the prompt does not show
	Steps                                []StepRecord // most recent completed steps, oldest first
	RunID                                string       // the run, whose step records are under archive.RunsRoot/RunID
	Rejected                             *Rejection   // set when the previous response of this step failed to decode
	ContextState                         *ContextState
	Receipts                             []StepReceipt
	Latest                               *StepRecord
	Window                               *Window
	DecisionTool                         bool // decisions arrive as canti_decision tool input instead of reply text
	RetrievalEnabled                     bool
	ImportsEnabled                       bool
	HandoverBytes                        int
	SessionContext                       string
	Carried                              []CarriedEvidence // the evidence Window.Carry names, in its order
	capture                              *ContextCapture
	sessionSources                       []ContextSessionSource
	sessionNavigation                    string
}

// Rejection is a response of the current step that failed to decode, with
// the decode error.
type Rejection struct{ Error, Response string }

// Validate checks the minimum information needed to construct a model input
// and that Earlier and Steps account for exactly the steps before Step,
// oldest first, as every completed step is observed or resolved.
func (f Frame) Validate() error {
	if strings.TrimSpace(f.Goal) == "" || strings.TrimSpace(f.Environment) == "" || strings.TrimSpace(f.RunID) == "" {
		return errors.New("harness: frame goal, environment and run ID are required")
	}
	if f.MaxSteps < 0 || f.Step < 1 || f.MaxSteps > 0 && f.Step > f.MaxSteps || f.Attempt < 1 || f.Attempt > f.MaxAttempts {
		return errors.New("harness: invalid frame execution position")
	}
	if f.Rejected != nil && f.Attempt < 2 {
		return errors.New("harness: repair requires a prior attempt")
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
	if f.ContextState == nil {
		return InitialContext()
	}
	return *f.ContextState
}
