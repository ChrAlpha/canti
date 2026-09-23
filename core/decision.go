// Package core defines the stateless run semantics the harness interprets:
// decision decoding and the pure transitions of a run State.
package core

// Kind identifies the only two decisions understood by the core protocol.
type Kind string

const (
	// Exec asks the harness to run Payload in its configured execution environment.
	Exec Kind = "exec"
	// Return gives control back to the caller. It does not assert task success.
	Return Kind = "return"
)

// Decision is the complete result of decoding one model response.
type Decision struct {
	Kind    Kind   `json:"kind"`
	Payload string `json:"payload"`
}
