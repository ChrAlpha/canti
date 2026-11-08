package harness

import (
	"context"
	"fmt"
	"strings"

	"github.com/ChrAlpha/canti/runtime/core"
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
// prefix, which the next prompt of the same context window extends byte for
// byte as steps complete. The blocks after it change with every request.
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
	Window   *Window         `json:",omitempty"`
	Context  *ContextCapture `json:",omitempty"`
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

// ContextLimitError is a confirmed provider context rejection. Only a tested,
// structured provider signal may construct it; output truncation is distinct.
type ContextLimitError struct {
	Status     int
	Type       string
	Code       string
	StopReason string
	RequestID  string
}

func (e *ContextLimitError) Error() string {
	return fmt.Sprintf("provider context limit: status=%d type=%s code=%s stop_reason=%s request_id=%s", e.Status, e.Type, e.Code, e.StopReason, e.RequestID)
}
