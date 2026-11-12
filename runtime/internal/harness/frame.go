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
	NativeArchive                                  bool
	Goal, Constraints, Environment                 string
	Step, MaxSteps, Attempt, MaxAttempts           int
	RunID                                          string
	Rejected                                       *Rejection
	ContextState                                   *ContextState
	Steps                                          []StepRecord
	Instructions                                   []Instruction
	Handover                                       *HandoverNotice
	Index                                          []StepReceipt
	IndexTotal                                     int
	Latest                                         *StepRecord
	DecisionTool, RetrievalEnabled, ImportsEnabled bool
	Observations                                   []ObservationInput
	Delivered                                      []Input
	Previous                                       []PreviousRun
	PreviousMore                                   int
	PreviousUsage                                  *int64
	capture                                        *ContextCapture
	previousRun                                    string
	preparation                                    string
}

// Instruction is one principal instruction bound to the run, with the
// transition that bound it and the step in progress at that moment.
type Instruction struct {
	Index   int
	Text    string
	Pos     int64
	Seq     int64
	Step    int
	InputID string
}

// Rejection is a response of the current step that failed to decode, with
// the decode error.
type Rejection struct{ Error, Response string }

// HandoverNotice is the carried handover resolved from its committed step
// record: its reference and the exact text it recorded.
type HandoverNotice struct {
	Ref  HandoverRef
	Text string
}

// Validate checks the minimum information needed to construct a model input
// and that the resident steps account for every completed step since the
// anchor.
func (f Frame) Validate() error {
	if err := f.validatePosition(); err != nil {
		return err
	}
	if err := f.validateResident(); err != nil {
		return err
	}
	if err := f.validateHandover(); err != nil {
		return err
	}
	if err := f.validateIndex(); err != nil {
		return err
	}
	return f.validateInputs()
}

func (f Frame) validatePosition() error {
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
	return nil
}

func (f Frame) validateResident() error {
	if f.ContextState == nil {
		return errors.New("harness: frame context state is required")
	}
	c := f.contextState()
	if c.FirstStep < 1 || c.FirstStep > f.Step {
		return errors.New("harness: invalid frame anchor")
	}
	if len(f.Steps) != f.Step-c.FirstStep {
		return fmt.Errorf("harness: missing resident steps between anchor step %d and step %d", c.FirstStep, f.Step)
	}
	for i, r := range f.Steps {
		if r.Step != c.FirstStep+i || r.Seq <= c.AnchorSeq {
			return errors.New("harness: invalid resident step identity or anchor boundary")
		}
	}
	if f.Latest != nil && f.Latest.Step != f.Step-1 {
		return errors.New("harness: latest result is not the previous completed step")
	}
	return nil
}

func (f Frame) validateHandover() error {
	c := f.contextState()
	switch {
	case c.Handover == nil && f.Handover != nil:
		return errors.New("harness: a handover notice requires a carried reference")
	case c.Handover == nil || c.Minimal:
		return nil
	case f.Handover == nil || !f.Handover.Ref.Equal(c.Handover):
		return errors.New("harness: handover notice differs from the carried reference")
	}
	if err := (Handover{Text: f.Handover.Text}).Validate(); err != nil {
		return fmt.Errorf("harness: carried handover: %w", err)
	}
	return nil
}

func (f Frame) validateIndex() error {
	c := f.contextState()
	if c.AnchorSeq == 0 && (len(f.Index) > 0 || f.IndexTotal != 0) {
		return errors.New("harness: the initial head holds no index")
	}
	if c.Minimal && len(f.Index) > 0 {
		return errors.New("harness: a minimal head reads no index")
	}
	last := 0
	for _, r := range f.Index {
		if r.Step <= last || r.Step >= c.FirstStep || r.Seq >= c.AnchorSeq || r.Step < 1 {
			return errors.New("harness: invalid earlier-step entry")
		}
		last = r.Step
	}
	if f.IndexTotal < len(f.Index) || f.IndexTotal > 0 && len(f.Index) == 0 {
		return fmt.Errorf("harness: invalid earlier-step total %d for %d entries", f.IndexTotal, len(f.Index))
	}
	return nil
}

// validateInputs checks the bound instructions, the observation keys, and the
// previous runs a request may include.
func (f Frame) validateInputs() error {
	last := int64(0)
	for i, in := range f.Instructions {
		if in.Index != i+1 || in.Seq <= 0 || in.Seq < last || in.Step < 1 || in.Step > f.Step || in.Pos < 1 || !utf8.ValidString(in.Text) || in.Text == "" {
			return errors.New("harness: invalid bound instruction")
		}
		last = in.Seq
	}
	if len(f.Observations) > MaxObservationKeys {
		return errors.New("harness: too many observation keys")
	}
	keys := map[string]bool{}
	for _, o := range f.Observations {
		key := o.Source + "\x00" + o.Subject
		if o.Pos < 1 || !utf8.ValidString(o.Text) || keys[key] {
			return errors.New("harness: invalid observation")
		}
		keys[key] = true
	}
	if len(f.Previous) > MaxPreviousRuns || f.PreviousMore < 0 {
		return errors.New("harness: invalid previous runs")
	}
	return nil
}

func (f Frame) contextState() ContextState {
	return *f.ContextState
}
