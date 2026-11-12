package harness

import "github.com/ChrAlpha/canti/runtime/internal/archive"

// ContextCapture records exactly what the frozen input included and why.
// Inclusion is not proof that the provider consumed or acted on evidence.
type ContextCapture struct {
	Bytes             ContextBytes
	Anchor            ContextState
	Focus             []EvidenceRef `json:",omitempty"`
	LatestSeq         int64
	LatestStep        int
	Preparation       string
	OrientationOnly   bool
	InstructionCount  int                `json:",omitempty"`
	InstructionsBytes int                `json:",omitempty"`
	ObservationPos    int64              `json:",omitempty"`
	ObservationKeys   int                `json:",omitempty"`
	PreviousRun       string             `json:",omitempty"`
	Retrievals        []ContextRetrieval `json:",omitempty"`
	Sources           []ContextSource    `json:",omitempty"`
}

type ContextBytes struct {
	Total, Limit, System, Goal, Constraints, Environment               int
	Anchor, Previous, ReceiptHeader, Instructions, Focus, Observations int
	Progress, Latest, Repair                                           int
	Steps                                                              []ContextStepBytes `json:",omitempty"`
}

type ContextStepBytes struct{ Step, Bytes int }

type ContextRetrieval struct {
	Seq              int64
	Step             int
	Operation        string `json:",omitempty"`
	More, Incomplete bool
	Error            string `json:",omitempty"`
}

func (r ContextRetrieval) OperationName() string {
	switch r.Operation {
	case "read", "list", "search", "discover":
		return r.Operation
	}
	return "invalid"
}

type ContextSource struct {
	Seq      int64
	Step     int
	Index    int
	Source   archive.SourceRange
	Selected bool
}
