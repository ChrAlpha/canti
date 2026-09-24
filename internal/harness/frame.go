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
	ArtifactDir                          string       // container path under which ArtifactRef.Path is readable
	RunsDir                              string       // container path holding the step records of every run by run ID, as PublishHistory lays them out
	RunID                                string       // the run, whose step records are under RunsDir/RunID
	Rejected                             *Rejection   // set when the previous response of this step failed to decode
	Window                               *Window
	RetrievalEnabled                     bool
	ImportsEnabled                       bool
	HandoverBytes                        int
	Retrieved                            map[int]string
}

// Rejection is a response of the current step that failed to decode, with
// the decode error.
type Rejection struct{ Error, Response string }

// Validate checks the minimum information needed to construct a model input
// and that Earlier and Steps account for exactly the steps before Step,
// oldest first, as every completed step is observed or resolved.
func (f Frame) Validate() error {
	switch {
	case strings.TrimSpace(f.Goal) == "":
		return errors.New("harness: frame goal is required")
	case strings.TrimSpace(f.Environment) == "":
		return errors.New("harness: frame environment is required")
	case strings.TrimSpace(f.ArtifactDir) == "":
		return errors.New("harness: frame artifact directory is required")
	case strings.TrimSpace(f.RunsDir) == "" || strings.TrimSpace(f.RunID) == "":
		return errors.New("harness: frame runs directory and run ID are required")
	case f.Step < 1 || f.Step > f.MaxSteps:
		return fmt.Errorf("harness: frame step %d is outside 1..%d", f.Step, f.MaxSteps)
	case f.Attempt < 1 || f.Attempt > f.MaxAttempts:
		return fmt.Errorf("harness: frame attempt %d is outside 1..%d", f.Attempt, f.MaxAttempts)
	case f.Rejected != nil && f.Attempt < 2:
		return errors.New("harness: frame rejects a response before any attempt")
	case f.Earlier < 0 || f.Earlier+len(f.Steps) != f.Step-1:
		return fmt.Errorf("harness: frame has %d earlier and %d recent steps before step %d", f.Earlier, len(f.Steps), f.Step)
	case f.Window == nil:
		return errors.New("harness: frame context window is required")
	case f.Earlier != f.Window.FirstStep-1:
		return errors.New("harness: frame steps do not begin at the context window boundary")
	}
	for i, s := range f.Steps {
		if want := f.Earlier + 1 + i; s.Step != want {
			return fmt.Errorf("harness: frame recent step %d is step %d, want %d", i, s.Step, want)
		}
	}
	if f.Window != nil {
		if err := f.Window.Validate(f.Step); err != nil {
			return err
		}
		if len(f.Steps) > 0 && f.Steps[0].Step < f.Window.FirstStep {
			return errors.New("harness: recent steps cross the context window boundary")
		}
	}
	return nil
}
