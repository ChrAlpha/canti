package harness

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
}

type ContextRetrieval struct {
	Step       int
	Query      RetrievalQuery
	More       bool
	Incomplete bool
	Error      string `json:",omitempty"`
}

type ContextSource struct {
	Step      int
	Source    SourceRange
	Available bool
	Error     string `json:",omitempty"`
}

type HandoverCheck struct {
	Status                          string
	Used, Capacity, Growth          int
	Steps, WindowSteps, WindowBytes int
	Reasons                         []string `json:",omitempty"`
}
