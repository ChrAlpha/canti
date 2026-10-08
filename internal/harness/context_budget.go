package harness

import (
	"errors"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
)

// evidenceReserve bounds the evidence block of a retrieval step: its text
// is at most the largest query limit, its source references fit that many
// JSON bytes and render at most 16 bytes more per source, and its fence may
// widen to the longest run of backticks in the text.
func evidenceReserve() int {
	return len("Retrieved original evidence:\n") + archive.MaxBytes + archive.MaxBytes + archive.MaxSources*16 +
		2*(archive.MaxBytes+1) + 2
}

func truncationBytes() int {
	return len("…[truncated " + strconv.Itoa(math.MaxInt) + " bytes]")
}

func quoteBytes(label string, limit int) int {
	return len(label+":\n```\n\n```\n") + limit + truncationBytes()
}

func purposeBytes(l PromptLimits) int {
	return len("Purpose: \n") + l.Purpose + truncationBytes()
}

func (f Frame) stepShellBytes(l PromptLimits) int {
	exit := math.MinInt
	s := StepRecord{Step: math.MaxInt, Resolved: true, Observation: Observation{
		Execution: core.ExecutionFailedToStart, ExitCode: &exit,
		Output: ArtifactRef{Available: true, Bytes: math.MaxInt64,
			Path: strings.Repeat("x", refLimit), Detail: strings.Repeat("x", refLimit)},
	}}
	quotes := quoteBytes("Command", l.Command) - len(quote("Command", "", l.Command))
	preview := quoteBytes("Preview", l.Preview) - len(quote("Preview", "", l.Preview))
	output := len(f.step(s, l)) + quotes + preview + truncationBytes() +
		len(archive.ArtifactsRoot) + 1 + refLimit + truncationBytes() -
		len(path.Join(archive.ArtifactsRoot, s.Observation.Output.Path))
	s.Observation.Retrieval = &archive.Retrieval{NextOffset: math.MaxInt64, Incomplete: true,
		NextCursor: strings.Repeat("x", archive.MaxCursorBytes), Error: strings.Repeat("\x00", 512),
		SkippedCount: 3 * archive.MaxEntries, Skipped: []archive.Skip{
			{Step: archive.MaxEntries, Field: "artifact", Reason: "utf8"},
			{Step: archive.MaxEntries, Field: "artifact", Reason: "utf8"},
		}}
	retrieval := len(f.step(s, l)) + quotes + preview + truncationBytes() + 2*(len(strconv.Itoa(math.MaxInt))-1)
	s.Observation.Retrieval = &archive.Retrieval{Query: archive.RetrievalQuery{Operation: "list"}, NextOffset: math.MaxInt64}
	list := len(f.step(s, l)) + quotes + archive.MaxBytes - len("null") +
		2*(len(strconv.Itoa(math.MaxInt))-1)
	return max(output, retrieval, list) + purposeBytes(l)
}

func (f Frame) failedHandoverShellBytes(l PromptLimits) int {
	exit := 1
	s := StepRecord{Step: math.MaxInt, Observation: Observation{Execution: core.ExecutionExited, ExitCode: &exit}}
	preview := min(l.Preview, 3*(len(actionFailure)+actionFailureBytes+truncationBytes()))
	return len(f.step(s, l)) + purposeBytes(l) +
		quoteBytes("Command", l.Command) - len(quote("Command", "", l.Command)) +
		quoteBytes("Preview", preview) - len(quote("Preview", "", l.Preview))
}

func (f Frame) tailBytes(l PromptLimits) int {
	f.Step, f.MaxSteps, f.Attempt, f.MaxAttempts = math.MaxInt, math.MaxInt, math.MaxInt, math.MaxInt
	f.Window = &Window{FirstStep: math.MaxInt, Requested: true, Failures: 1}
	f.Rejected = &Rejection{}
	return len("None.\n\n") + len(f.tail(l, f.progress(math.MaxInt))) +
		quoteBytes("Error", l.Rejected) - len(quote("Error", "", l.Rejected)) +
		quoteBytes("Rejected reply", l.Rejected) - len(quote("Rejected reply", "", l.Rejected))
}

// Fit accounts, in bytes under normalized limits, for the context of a run:
// its first page, and the fixed context of a window that its largest legal
// handover opens, with the reserves for one more step, its retrieved
// evidence, a failed handover, and the tail of a repair page.
type Fit struct {
	Limit    int // PromptLimits.Total
	Handover int // the largest legal handover
	Page     int // the first page
	System   int
	Head     int // the first block of that window
	Step     int
	Evidence int
	Failed   int
	Tail     int
}

// Required is the smallest Limit that admits the run: it holds the first
// page and leaves that window positive capacity.
func (f Fit) Required() int { return max(f.Page, f.fixed()+1) }

// Deficit is how many bytes Limit falls short of Required.
func (f Fit) Deficit() int { return max(0, f.Required()-f.Limit) }

// Check fails, naming Required, when Limit falls short of it.
func (f Fit) Check() error {
	if d := f.Deficit(); d > 0 {
		return fmt.Errorf("harness: fixed context leaves no room for work and handover reserve: it needs a total of at least %d bytes, %d more than %d", f.Required(), d, f.Limit)
	}
	return nil
}

func (f Fit) fixed() int { return f.System + f.Head + f.Step + f.Evidence + f.Failed + f.Tail }

func (f Frame) reserve(l PromptLimits) Fit {
	fit := Fit{Limit: l.Total, Handover: f.HandoverBytes, System: len(f.contextSystem()), Head: len(f.head(l)),
		Step: f.stepShellBytes(l), Failed: f.failedHandoverShellBytes(l), Tail: f.tailBytes(l)}
	if f.RetrievalEnabled {
		fit.Evidence = evidenceReserve()
	}
	return fit
}

func (f Frame) workCapacity(l PromptLimits) (int, error) {
	capacity := l.Total - f.reserve(l).fixed()
	if capacity <= 0 {
		return 0, errors.New("harness: fixed context leaves no room for work and handover reserve")
	}
	return capacity, nil
}

// fit accounts for f as the first page of its run.
func (f Frame) fit(l PromptLimits) (Fit, error) {
	if f.HandoverBytes < 1 || f.HandoverBytes > archive.MaxBytes {
		return Fit{}, errors.New("harness: handover bytes must be 1..8192")
	}
	page := l
	page.Total = math.MaxInt
	p, err := f.Prompt(page)
	if err != nil {
		return Fit{}, err
	}
	next := f
	next.Window = &Window{FirstStep: math.MaxInt, Handover: strings.Repeat("`", f.HandoverBytes)}
	fit := next.reserve(l)
	fit.Page = len(p.System)
	for _, b := range p.Blocks {
		fit.Page += len(b)
	}
	return fit, nil
}

func (f Frame) ValidateContext(l PromptLimits) error {
	fit, err := f.fit(l)
	if err != nil {
		return err
	}
	return fit.Check()
}
