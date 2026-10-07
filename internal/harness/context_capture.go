package harness

import "github.com/ChrAlpha/canti/internal/archive"

type ContextCapture struct {
	Bytes             ContextBytes
	Session           []ContextSessionSource `json:",omitempty"`
	SessionNavigation string                 `json:",omitempty"`
	Retrievals        []ContextRetrieval     `json:",omitempty"`
	Sources           []ContextSource        `json:",omitempty"`
	Handover          HandoverCheck
}

type ContextBytes struct {
	Total, Limit, System, Goal, Constraints, Environment int
	Session, Window, Handover, StepHeader, EmptySteps    int
	Progress, HandoverRequired, Repair                   int
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
	HandoverStep int `json:",omitempty"`
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

type HandoverCheck struct {
	Status                          string
	Used, Capacity, Growth          int
	Steps, WindowSteps, WindowBytes int
	Reasons                         []string `json:",omitempty"`
}
