// Package core defines the stateless run semantics the harness interprets:
// decision decoding and the pure transitions of a run State.
package core

import (
	"errors"
	"fmt"
	"strings"
)

// Kind identifies the only two decisions understood by the core protocol.
type Kind string

const (
	// Exec asks the harness to run Payload in its configured execution environment.
	Exec Kind = "exec"
	// Return gives control back to the caller. It does not assert task success.
	Return Kind = "return"
)

// Decision is the complete result of decoding one model response. Purpose
// is the model's optional note on what the step is for; it is advisory text
// and never executed.
type Decision struct {
	Kind    Kind   `json:"kind"`
	Purpose string `json:"purpose,omitempty"`
	Payload string `json:"payload"`
}

// Validate applies the decoder's rules to an already-decoded decision.
func (d Decision) Validate() error {
	if d.Kind != Exec && d.Kind != Return {
		return fmt.Errorf("core: %w: unsupported kind %q", errInvalidDecision, d.Kind)
	}
	if d.Kind == Exec && strings.TrimSpace(d.Payload) == "" {
		return errors.New("core: invalid decision: exec payload is blank")
	}
	return nil
}
