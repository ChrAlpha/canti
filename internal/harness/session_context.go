package harness

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
)

const contextSourceReadBytes = 32 << 20

type contextSelection struct {
	Checkpoint Checkpoint
	Entries    []ContextEntry
	More       bool
}

func ContextSourceHash(entries []ContextEntry) string {
	if len(entries) == 0 {
		entries = nil
	}
	b, _ := json.Marshal(entries)
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

func (l *Loop) contextSources(ctx context.Context, j CompactionJournal, run Run, cp Checkpoint) (contextSelection, error) {
	selected := contextSelection{Checkpoint: cp}
	cursor, cost := cp.Through, 0
	bytesRead := 0
	for {
		page, more, err := j.ContextEntries(ctx, run.ContextOwner(), cursor, historyPage, contextSourceReadBytes)
		if err != nil {
			return selected, err
		}
		for i, e := range page {
			if e.Index != cursor+1 {
				return selected, contextError{fmt.Errorf("harness: context source gap after %d", cursor)}
			}
			if err := e.Validate(); err != nil {
				return selected, contextError{err}
			}
			raw, _ := json.Marshal(e)
			if bytesRead+len(raw) > contextSourceReadBytes && len(selected.Entries) > 0 {
				selected.More = true
				return selected, nil
			}
			bytesRead += len(raw)
			selected.Entries = append(selected.Entries, e)
			cost += len(l.entryText(e, ""))
			cursor = e.Index
			if cost/2 >= l.cfg.Limits.Total {
				selected.More = more || i < len(page)-1
				return selected, nil
			}
		}
		if !more {
			return selected, nil
		}
	}
}

func (l *Loop) entryText(e ContextEntry, currentRun string) string {
	if e.Kind == "input" && e.RunID == currentRun {
		return ""
	}
	source := fmt.Sprintf("Run %s; source %s/%s", inline(e.RunID), archive.RunsRoot, inline(e.RunID))
	var b strings.Builder
	fmt.Fprintf(&b, "### Session source %d (%s)\n", e.Index, source)
	switch e.Kind {
	case "input":
		b.WriteString("Earlier user goal:\n" + fullBlock(strings.TrimSpace(e.Goal)))
		if e.Constraints != "" {
			b.WriteString("Earlier explicit constraints:\n" + fullBlock(e.Constraints))
		}
	case "step":
		if e.Step != nil {
			b.WriteString((Frame{}).step(*e.Step, l.cfg.Limits))
		}
	case "result":
		if e.Phase == core.PhaseReturned {
			b.WriteString("Earlier assistant return:\n" + fullBlock(e.Result))
		}
	}
	if e.Phase != "" {
		fmt.Fprintf(&b, "Recorded run status: %s, reason %s; return does not establish success.\n", e.Phase, e.Reason)
		if e.Phase == core.PhaseStopped {
			b.WriteString("No assistant return.\n")
		}
	}
	return b.String()
}

func (l *Loop) selectedFrame(run Run, s core.State, selection contextSelection) Frame {
	f := l.frame(run, s)
	cp := selection.Checkpoint
	f.Window = &Window{FirstStep: s.Step, Through: cp.Through, Summary: cp.Summary}
	for _, entry := range selection.Entries {
		f.priorHistory = f.priorHistory || entry.RunID != run.ID
	}
	for _, entry := range selection.Entries {
		block := contextBlock{Text: l.entryText(entry, run.ID)}
		if f.priorHistory && entry.RunID == run.ID && entry.Kind == "input" {
			block.Text = "\n## Current window steps\n"
		}
		if entry.RunID == run.ID && entry.Step != nil {
			f.Steps = append(f.Steps, *entry.Step)
			block.Text, block.Step = f.step(*entry.Step, l.cfg.Limits), entry.Step.Step
		}
		f.contextBlocks = append(f.contextBlocks, block)
	}
	if len(f.Steps) > 0 {
		f.Window.FirstStep = f.Steps[0].Step
	}
	f.Earlier = f.Window.FirstStep - 1
	f.capture = &ContextCapture{CheckpointID: cp.ID, Through: cp.Through, SourceEnd: cp.Through, Compaction: CompactionCheck{Status: "evaluated"}}
	if n := len(selection.Entries); n > 0 {
		f.capture.SourceEnd = selection.Entries[n-1].Index
	}
	positions := map[string]int{}
	for _, e := range selection.Entries {
		f.capture.Entries = append(f.capture.Entries, ContextEntryRef{Index: e.Index, RunID: e.RunID, SourceSeq: e.SourceSeq, Kind: e.Kind})
		if e.RunID == run.ID {
			continue
		}
		pos, ok := positions[e.RunID]
		if !ok {
			pos = len(f.capture.Session)
			positions[e.RunID] = pos
			f.capture.Session = append(f.capture.Session, ContextSessionSource{RunID: e.RunID, SessionSeq: e.SessionSeq})
		}
		source := &f.capture.Session[pos]
		if e.Kind == "input" {
			source.IncludedGoal = true
		}
		if e.Phase != "" {
			source.Status = string(e.Phase)
		}
		f.capture.SessionNavigation = e.RunID
	}
	f.captureRetrievals()
	if cut := l.compactionCut(run, selection); cut > 0 {
		f.capture.Prefix = &ContextPrefix{Through: selection.Entries[cut-1].Index}
	}
	return f
}

func (f *Frame) captureRetrievals() {
	for _, s := range f.Steps {
		if r := s.Observation.Retrieval; r != nil {
			f.capture.Retrievals = append(f.capture.Retrievals, ContextRetrieval{Step: s.Step, Operation: (ContextRetrieval{Operation: r.Query.Operation}).OperationName(), More: r.More, Incomplete: r.Incomplete, Error: clip(r.Error, 512)})
		}
	}
}

func (l *Loop) workingEntryText(e ContextEntry, currentRun string) string {
	if e.RunID == currentRun && e.Step != nil {
		return (Frame{}).step(*e.Step, l.cfg.Limits)
	}
	return l.entryText(e, currentRun)
}
