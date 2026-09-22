package harness

import "context"

// Model performs one independent model attempt and returns the complete raw
// response with its provenance.
type Model interface {
	Complete(context.Context, Request) (Response, error)
}

// Request is the frozen, provider-facing input for one step. Its identity
// comes from the run state it is committed with.
type Request struct {
	Body     []byte
	Model    string
	Protocol string
	Adapter  string
}

// Response is the raw model result before core decoding. Complete reports
// transport completeness; valid JSON alone does not prove it.
type Response struct {
	Body     []byte
	Complete bool
	Model    string
	Protocol string
	Adapter  string
}
