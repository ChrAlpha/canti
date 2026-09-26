package harness

import (
	"errors"
	"math"
	"path"
	"strconv"
	"strings"

	"github.com/ChrAlpha/canti/core"
)

func retrievalExpansionReserve() int {
	const sources = 32
	// Directory source references fit 8192 JSON bytes; the rendered headers
	// can add at most 16 bytes per source. File search has one repeated path.
	return len("Retrieved original evidence:\n") + RetrievalMaxBytes + sources*16 +
		RetrievalMaxBytes + sources*(len("Evidence unavailable: ")+512+truncationBytes()+1) +
		2*(RetrievalMaxBytes+1) + 2
}

func truncationBytes() int {
	return len("…[truncated " + strconv.Itoa(math.MaxInt) + " bytes]")
}

func quoteBytes(label string, limit int) int {
	return len(label+":\n```\n\n```\n") + limit + truncationBytes()
}

func (f Frame) stepShellBytes(l PromptLimits) int {
	f.Retrieved = nil
	exit := math.MinInt
	s := StepRecord{Step: math.MaxInt, Resolved: true, Observation: Observation{
		Execution: core.ExecutionFailedToStart, ExitCode: &exit,
		Output: ArtifactRef{Available: true, Bytes: math.MaxInt64,
			Path: strings.Repeat("x", refLimit), Detail: strings.Repeat("x", refLimit)},
	}}
	quotes := quoteBytes("Command", l.Command) - len(quote("Command", "", l.Command))
	preview := quoteBytes("Preview", l.Preview) - len(quote("Preview", "", l.Preview))
	output := len(f.step(s, l)) + quotes + preview + truncationBytes() +
		len(inline(f.ArtifactDir)) + 1 + refLimit + truncationBytes() -
		len(path.Join(inline(f.ArtifactDir), s.Observation.Output.Path))
	s.Observation.Retrieval = &Retrieval{NextOffset: math.MaxInt64, Incomplete: true,
		NextCursor: strings.Repeat("x", 1024), Error: strings.Repeat("\x00", 512)}
	retrieval := len(f.step(s, l)) + quotes + preview + truncationBytes() + 2*(len(strconv.Itoa(math.MaxInt))-1)
	s.Observation.Retrieval = &Retrieval{Query: RetrievalQuery{Operation: "list"}, NextOffset: math.MaxInt64}
	list := len(f.step(s, l)) + quotes + RetrievalMaxBytes - len("null") +
		2*(len(strconv.Itoa(math.MaxInt))-1)
	return max(output, retrieval, list)
}

func (f Frame) failedHandoverShellBytes(l PromptLimits) int {
	f.Retrieved = nil
	exit := 1
	s := StepRecord{Step: math.MaxInt, Observation: Observation{Execution: core.ExecutionExited, ExitCode: &exit}}
	preview := min(l.Preview, len("Context action failed: ")+2048+truncationBytes())
	return len(f.step(s, l)) +
		quoteBytes("Command", l.Command) - len(quote("Command", "", l.Command)) +
		quoteBytes("Preview", preview) - len(quote("Preview", "", l.Preview))
}

func (f Frame) tailBytes(l PromptLimits) int {
	f.Step, f.MaxSteps, f.Attempt, f.MaxAttempts = math.MaxInt, math.MaxInt, math.MaxInt, math.MaxInt
	f.Window = &Window{FirstStep: math.MaxInt, Requested: true}
	f.Rejected = &Rejection{}
	return len("None.\n\n") + len(f.tail(l, f.progress(math.MaxInt))) +
		quoteBytes("Error", l.Rejected) - len(quote("Error", "", l.Rejected)) +
		quoteBytes("Rejected reply", l.Rejected) - len(quote("Rejected reply", "", l.Rejected))
}

func (f Frame) workCapacity(l PromptLimits) (int, error) {
	evidence := 0
	if f.RetrievalEnabled {
		evidence = retrievalExpansionReserve()
	}
	capacity := l.Total - len(f.contextSystem()) - len(f.head(l)) - evidence -
		f.stepShellBytes(l) - f.failedHandoverShellBytes(l) - f.tailBytes(l)
	if capacity <= 0 {
		return 0, errors.New("harness: fixed context leaves no room for work and handover reserve")
	}
	return capacity, nil
}

func (f Frame) ValidateContext(l PromptLimits) error {
	if f.HandoverBytes < 1 || f.HandoverBytes > RetrievalMaxBytes {
		return errors.New("harness: handover bytes must be 1..8192")
	}
	next := f
	next.Window = &Window{FirstStep: math.MaxInt, Handover: strings.Repeat("`", f.HandoverBytes)}
	if _, err := next.workCapacity(l); err != nil {
		return err
	}
	_, err := f.Prompt(l)
	return err
}
