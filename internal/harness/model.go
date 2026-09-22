package harness

import "context"

// Model is the harness boundary for one independent model request.
// Implementations must return the complete raw response and its provenance.
type Model interface {
	Complete(context.Context, Request) (Response, error)
}

// Request is the frozen, provider-facing input for one model attempt.
type Request struct {
	RunID    string
	StepID   string
	Body     []byte
	Model    string
	Protocol string
	Adapter  string
}

// Response is the raw model result before core decoding.
type Response struct {
	Body     []byte
	Complete bool
	Model    string
	Protocol string
	Adapter  string
}
