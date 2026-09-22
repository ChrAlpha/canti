// Package core contains the stateless decision protocol used by the harness.
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
//
// The core deliberately does not model sessions, tools, execution state, or
// success. Those concerns belong to the harness and its execution environment.
type Decision struct {
	Kind    Kind   `json:"kind"`
	Payload string `json:"payload"`
}
