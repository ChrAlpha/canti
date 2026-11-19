package chat

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

// The transcript and the reopened history share one retention contract: a
// reader sees at most this many entries and bytes, with an omission notice.
const (
	HistoryEntries = 256
	HistoryBytes   = 64 << 10
)

// The startup bounds: the TUI reads newest pages first and stops at the
// conversation bound, the page bound, or the run bound, whichever comes
// first, so its work does not grow with the line.
const (
	historyPageRows = 64
	historyPages    = 8
	historyRuns     = 8
	historyTimeout  = 5 * time.Second
)

// History is the bounded, whole-run tail of the current line that a new
// invocation shows before its own activity. Items are grouped by run in
// ascending ordinal order and by agent-log position within a run; Cursor is
// the snapshot log end, from which live progress continues without repeating
// or skipping a row; Line pins later pages to the same line. Runs holds the retained run summaries in the
// same order as their item groups. Latest is the line's latest run when a
// host reported it, even when that run's items were not retained. Loaded
// reports that this invocation actually read history; -fresh skips the read
// and therefore also the startup recheck and attachment.
type History struct {
	Loaded    bool
	Latest    *hostrpc.HistoryRun
	Runs      []hostrpc.HistoryRun
	Items     []hostrpc.Item
	Line      string
	LineStart int
	Cursor    string
	Omitted   bool
	Err       error
}

// collectHistory reads newest pages first, retains only whole runs, and
// groups them oldest first. A failed page keeps what was already read and
// reports the error.
func collectHistory(ctx context.Context, fetch func(context.Context, string, string, int) (hostrpc.HistoryPage, error)) History {
	h := History{Loaded: true}
	newest, more := collectHistoryPages(ctx, fetch, &h)
	if more {
		h.Omitted = true
	}
	return keepHistoryRuns(&h, newest)
}

// collectHistoryPages fetches the bounded page sequence and returns the rows
// in newest-first order. It records the first page's line, summaries and
// follow cursor on h.
func collectHistoryPages(ctx context.Context, fetch func(context.Context, string, string, int) (hostrpc.HistoryPage, error), h *History) ([]hostrpc.Item, bool) {
	var newest []hostrpc.Item
	before := ""
	first := true
	for page := 0; page < historyPages; page++ {
		p, err := fetch(ctx, before, h.Line, historyPageRows)
		if err != nil {
			h.Err = err
			h.Omitted = len(newest) > 0
			return newest, false
		}
		if first {
			h.Latest, h.Runs, h.Line, h.LineStart, h.Cursor = p.Latest, p.Runs, p.Line, p.LineStart, p.Cursor
			h.Omitted = p.Omitted
			first = false
		} else {
			h.Omitted = h.Omitted || p.Omitted
		}
		if !first && len(p.Runs) > 0 {
			seen := make(map[string]bool, len(h.Runs)+len(p.Runs))
			for _, run := range h.Runs {
				seen[run.RunID] = true
			}
			for _, run := range p.Runs {
				if !seen[run.RunID] {
					h.Runs = append(h.Runs, run)
				}
			}
		}
		for i := len(p.Items) - 1; i >= 0; i-- {
			newest = append(newest, p.Items[i])
		}
		if completeRuns(newest) >= historyRuns || historyEntries(newest) >= HistoryEntries || historyBytesOf(newest) >= HistoryBytes {
			return newest, p.Older != ""
		}
		if p.Older == "" {
			return newest, false
		}
		before = p.Older
		if page == historyPages-1 {
			return newest, true
		}
	}
	return newest, false
}

// keepHistoryRuns groups fetched rows by run, drops incomplete fragments,
// appends queued line inputs, and trims whole runs to the transcript bound.
func keepHistoryRuns(h *History, newest []hostrpc.Item) History {
	groups, queued := groupHistory(newest)
	runs := append([]hostrpc.HistoryRun(nil), h.Runs...)
	sort.Slice(runs, func(i, j int) bool { return runs[i].Ordinal > runs[j].Ordinal })
	for i, j := 0, len(runs)-1; i < j; i, j = i+1, j-1 {
		runs[i], runs[j] = runs[j], runs[i]
	}
	h.Items, h.Runs = nil, nil
	for _, run := range runs {
		items := groups[run.RunID]
		delete(groups, run.RunID)
		appendHistoryRun(h, run, items)
	}
	if len(groups) > 0 {
		h.Omitted = true
	}
	appendQueuedHistory(h, queued)
	for overHistoryBound(*h) && len(h.Runs) > 0 {
		dropOldestRun(h)
	}
	if len(h.Runs) == 0 {
		h.Items = nil
	}
	return *h
}

func groupHistory(newest []hostrpc.Item) (map[string][]hostrpc.Item, []hostrpc.Item) {
	groups := make(map[string][]hostrpc.Item)
	var queued []hostrpc.Item
	for i := len(newest) - 1; i >= 0; i-- {
		it := newest[i]
		if it.RunID != "" {
			groups[it.RunID] = append(groups[it.RunID], it)
			continue
		}
		if it.Kind == "withdrawn" || (it.Kind == "input" && it.Input != nil && it.Input.Authority == "principal") {
			queued = append(queued, it)
		}
	}
	return groups, queued
}

func appendHistoryRun(h *History, run hostrpc.HistoryRun, items []hostrpc.Item) {
	if len(items) == 0 {
		return
	}
	if !hasGoal(items) {
		h.Omitted = true
		return
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Pos < items[j].Pos })
	h.Runs = append(h.Runs, run)
	h.Items = append(h.Items, items...)
}

func appendQueuedHistory(h *History, queued []hostrpc.Item) {
	if len(queued) == 0 {
		return
	}
	if len(h.Runs) == 0 {
		h.Omitted = true
		return
	}
	sort.Slice(queued, func(i, j int) bool { return queued[i].Pos < queued[j].Pos })
	h.Items = append(h.Items, queued...)
}

// hasGoal reports a run whose opening principal input is retained. Without
// it the run is a fragment and is omitted rather than shown mid-run.
func hasGoal(items []hostrpc.Item) bool {
	for _, it := range items {
		if it.Kind == "input" && it.Input != nil && it.Input.Authority == "principal" && it.Input.Disposition == "opened" {
			return true
		}
	}
	return false
}

func completeRuns(items []hostrpc.Item) int {
	seen := map[string]bool{}
	for _, it := range items {
		if it.RunID != "" && it.Kind == "input" && it.Input != nil &&
			it.Input.Authority == "principal" && it.Input.Disposition == "opened" {
			seen[it.RunID] = true
		}
	}
	return len(seen)
}

func historyEntries(items []hostrpc.Item) int {
	seen := map[string]bool{}
	for _, it := range items {
		if it.RunID != "" {
			seen[it.RunID] = true
		}
	}
	return len(items) + len(seen)
}

func historyBytesOf(items []hostrpc.Item) int {
	bytes := 0
	for _, it := range items {
		bytes += historyItemBytes(it)
	}
	return bytes
}

func overHistoryBound(h History) bool {
	return len(h.Items)+len(h.Runs) > HistoryEntries || historyBytesOf(h.Items) > HistoryBytes
}

func dropOldestRun(h *History) {
	if len(h.Runs) == 0 {
		return
	}
	runID := h.Runs[0].RunID
	kept := h.Items[:0]
	for _, it := range h.Items {
		if it.RunID != runID {
			kept = append(kept, it)
		}
	}
	h.Items = kept
	h.Runs = h.Runs[1:]
	h.Omitted = true
	if len(h.Runs) == 0 {
		h.Items = nil
	}
}

func historyItemBytes(it hostrpc.Item) int {
	data, _ := json.Marshal(it)
	return len(data)
}
