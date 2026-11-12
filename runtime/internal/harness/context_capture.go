package harness

import "github.com/ChrAlpha/canti/runtime/internal/archive"

// ContextCapture records exactly what the frozen input included and why.
// Inclusion is not proof that the provider consumed or acted on evidence.
type ContextCapture struct {
	Bytes            ContextBytes
	Anchor           ContextState
	LatestSeq        int64
	LatestStep       int
	Preparation      string
	OrientationOnly  bool
	InstructionCount int                `json:",omitempty"`
	ObservationPos   int64              `json:",omitempty"`
	ObservationKeys  int                `json:",omitempty"`
	PreviousRun      string             `json:",omitempty"`
	Retrievals       []ContextRetrieval `json:",omitempty"`
	Sources          []ContextSource    `json:",omitempty"`
}

// ContextBytes accounts the rendered bytes of one request per section and per
// resident step. System and every section sum to Total; instruction bytes are
// reported both in total and where they render.
type ContextBytes struct {
	Total, Limit, System                             int
	Goal, Constraints, HeadInstructions, Environment int
	Anchor, Previous, Index, Notice, EpochHeader     int
	Instructions, EpochInstructions                  int
	Observations, Progress, Latest, Repair           int
	Steps                                            []ContextStepBytes `json:",omitempty"`
}

type ContextStepBytes struct{ Step, Bytes int }

// ContextRetrieval records one resident retrieval's committed operation and
// continuation, so the capture proves what the request showed.
type ContextRetrieval struct {
	Seq              int64
	Step             int
	Operation        string `json:",omitempty"`
	More, Incomplete bool
	NextOffset       int64  `json:",omitempty"`
	NextCursor       string `json:",omitempty"`
	Error            string `json:",omitempty"`
}

func (r ContextRetrieval) OperationName() string {
	switch r.Operation {
	case "read", "list", "search", "discover":
		return r.Operation
	}
	return "invalid"
}

// ContextSource records one exact range a resident retrieval froze.
type ContextSource struct {
	Seq    int64
	Step   int
	Index  int
	Source archive.SourceRange
}
