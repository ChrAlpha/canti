package harness

import "github.com/ChrAlpha/canti/internal/archive"

type ContextCapture struct {
	Entries           []ContextEntryRef `json:",omitempty"`
	CheckpointID      string            `json:",omitempty"`
	Through           int64
	SourceEnd         int64
	Bytes             ContextBytes
	Session           []ContextSessionSource `json:",omitempty"`
	SessionNavigation string                 `json:",omitempty"`
	Retrievals        []ContextRetrieval     `json:",omitempty"`
	Sources           []ContextSource        `json:",omitempty"`
	Compaction        CompactionCheck
	// Prefix records a candidate exact source prefix already marked for caching.
	Prefix *ContextPrefix `json:",omitempty"`
}

type ContextBytes struct {
	Total, Limit, System, Goal, Constraints, Environment int
	Session, Window, Summary, StepHeader, EmptySteps     int
	Progress, Repair                                     int
	Steps                                                []ContextStepBytes `json:",omitempty"`
}

type ContextStepBytes struct {
	Step, Bytes int
}

type ContextSessionSource struct {
	RunID        string
	SessionSeq   int64
	Status       string
	IncludedGoal bool
}

type ContextRetrieval struct {
	Step       int
	Operation  string                  `json:",omitempty"`
	Query      *archive.RetrievalQuery `json:",omitempty"` // Legacy frozen captures only.
	More       bool
	Incomplete bool
	Error      string `json:",omitempty"`
}

func (r ContextRetrieval) OperationName() string {
	operation := r.Operation
	if operation == "" && r.Query != nil {
		operation = r.Query.Operation
	}
	switch operation {
	case "read", "list", "search", "discover":
		return operation
	default:
		return "invalid"
	}
}

type ContextSource struct {
	Step      int
	Source    archive.SourceRange
	Available bool
	Error     string `json:",omitempty"`
}

type CompactionCheck struct {
	Status                          string
	Used, Capacity, Growth          int
	Steps, WindowSteps, WindowBytes int
	Reasons                         []string `json:",omitempty"`
}

type ContextEntryRef struct {
	Index     int64
	RunID     string
	SourceSeq int64
	Kind      string
	// BlockEnd is the exclusive content-block boundary representing this source.
	// A pinned input may share the preceding boundary instead of duplicating it.
	BlockEnd int `json:",omitempty"`
}

type ContextPrefix struct {
	Through int64
	Blocks  int
}
