package harness

import (
	"errors"
	"strings"
)

// Frame is the bounded input assembled by the harness for one model request.
// The core never receives a Frame or any other history representation.
type Frame struct {
	Goal            string
	Constraints     string
	Environment     string
	LastObservation string
	Notes           string
	HistoryRef      string
}

// Validate checks the minimum information needed to construct a model input.
func (f Frame) Validate() error {
	if strings.TrimSpace(f.Goal) == "" {
		return errors.New("harness: frame goal is required")
	}
	if strings.TrimSpace(f.Environment) == "" {
		return errors.New("harness: frame environment is required")
	}
	return nil
}
