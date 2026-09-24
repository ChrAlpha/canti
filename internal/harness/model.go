package harness

import (
	"context"
	"strings"

	"github.com/ChrAlpha/canti/core"
)

// Model freezes provider-facing request bytes, performs one independent
// attempt with exactly those bytes, and extracts the decision text from a
// saved raw response.
//
// Complete returns a nil error exactly when the response is complete. A
// *StopError reports a failure that resending the same bytes cannot fix; any
// other error is a failed or truncated attempt, possibly with a partial
// response. Once ctx is done Complete returns its error.
type Model interface {
	Freeze(Prompt) (Request, error)
	Complete(context.Context, Request) (Response, error)
	Decision(Response) []byte
}

// Prompt is the provider-neutral input built from a Frame: a system prompt
// and a user message in non-empty blocks. Blocks[:Stable] is the stable
// prefix, which the next prompt of the run repeats byte for byte unless its
// oldest shown step differs, as when a step is dropped or shorter notes let a
// dropped step be shown again; the blocks after it change with every request.
type Prompt struct {
	System string
	Blocks []string
	Stable int
}

// User returns the user message, the blocks joined.
func (p Prompt) User() string { return strings.Join(p.Blocks, "") }

// Request is the frozen, provider-facing input for one step. Its identity
// comes from the run state it is committed with.
type Request struct {
	Body     []byte
	Model    string
	Protocol string
	Adapter  string
	Window   *Window `json:",omitempty"`
}

// Response is the raw model result before core decoding. Complete reports
// transport completeness; valid JSON alone does not prove it.
type Response struct {
	Body     []byte
	Complete bool
	Model    string
	Protocol string
	Adapter  string
	Usage    Usage
}

// Usage is the provider-reported token count of one attempt.
type Usage struct {
	InputTokens  int64
	OutputTokens int64
}

// StopError reports a model failure that no retry of the same bytes can fix;
// the loop reports Reason through core.StopRequested, so it must be a reason
// that event accepts: StopUnrecoverable or StopBudgetExhausted.
type StopError struct {
	Reason core.StopReason
	Err    error
}

func (e *StopError) Error() string { return string(e.Reason) + ": " + e.Err.Error() }

func (e *StopError) Unwrap() error { return e.Err }
