package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ChrAlpha/canti/core"
)

func (l *Loop) planCompaction(run Run, snap Snapshot, s contextSelection, base string, before int, required bool) (*CompactPlan, error) {
	model, ok := l.model.(SummaryModel)
	if !ok {
		return nil, errors.New("harness: model does not support independent summaries")
	}
	cut := l.compactionCut(run, s)
	if cut == 0 && required {
		cut = l.protectedCut(run, s)
	}
	if cut == 0 && !(required && s.Checkpoint.ID != "") {
		if required {
			return nil, errors.New("harness: current request and protected recent step leave no compactable history")
		}
		return nil, nil
	}
	replaced := SummaryCost(s.Checkpoint.Summary)
	for _, e := range s.Entries[:cut] {
		replaced += len(l.workingEntryText(e, run.ID))
	}
	// Summary framing also takes space in the replacement working request.
	limit := min(l.cfg.SummaryBytes, replaced-256)
	if limit < 1 {
		if required {
			return nil, errors.New("harness: compactable history cannot free enough room")
		}
		return nil, nil
	}
	var history strings.Builder
	for _, e := range s.Entries[:cut] {
		history.WriteString(l.entryText(e, ""))
	}
	content := "Previous summary:\n" + fullBlock(s.Checkpoint.Summary) + "\nSource entries to replace:\n" + fullBlock(history.String()) +
		"\nCurrent run: " + run.ID + "\nCurrent request (also kept verbatim):\n" + fullBlock(run.Goal) + "\nEffective explicit constraints:\n" + fullBlock(run.Constraints) +
		fmt.Sprintf("\nThe summary must be at most %d UTF-8 bytes. Leave out superseded detail; preserve continuing obligations.\n", limit)
	prompt := Prompt{System: summarySystem, Blocks: []string{content}}
	// Bound the summary's input independently of the business prompt. Large
	// backlogs are consumed in consecutive durable passes at the same safe point.
	if len(content)+len(summarySystem) > l.cfg.Limits.Total {
		if cut <= 1 {
			return nil, errors.New("harness: one source unit does not fit the summary input budget")
		}
		shorter := s
		shorter.Entries = s.Entries[:cut/2]
		shorter.More = true
		return l.planCompaction(run, snap, shorter, base, before, required)
	}
	request, err := model.FreezeSummary(prompt, l.cfg.SummaryTokens)
	if err != nil {
		return nil, err
	}
	if err := l.admitRequest(request); err != nil {
		if cut <= 1 {
			return nil, err
		}
		shorter := s
		shorter.Entries = s.Entries[:cut/2]
		shorter.More = true
		return l.planCompaction(run, snap, shorter, base, before, required)
	}
	through := s.Checkpoint.Through
	if cut > 0 {
		through = s.Entries[cut-1].Index
	}
	p, err := NewCompactPlan(CompactPlan{RunID: run.ID, RunSeq: snap.Seq, Owner: run.ContextOwner(), Base: base, Parent: s.Checkpoint.ID,
		After: s.Checkpoint.Through, Through: through, SourceHash: ContextSourceHash(s.Entries[:cut]), Request: request,
		MaxAttempts: l.cfg.SummaryAttempts, MaxSummaryBytes: limit, BeforeBytes: before, ReplacedBytes: replaced, TokenReserve: RequestInputEstimate(request) + int64(l.outputReserve(request))})
	return &p, err
}

func (l *Loop) compactionCut(run Run, s contextSelection) int {
	if s.More {
		return len(s.Entries)
	}
	protected := l.protectedCut(run, s)
	cut, size := len(s.Entries), 0
	for cut > 0 {
		cost := len(l.entryText(s.Entries[cut-1], run.ID))
		if size+cost > l.cfg.RetainBytes {
			break
		}
		size += cost
		cut--
	}
	if cut == 0 && l.compactThreshold(s) {
		cut = protected
	}
	return min(cut, protected)
}

func (l *Loop) compact(ctx context.Context, run Run, snap Snapshot, j CompactionJournal, c Compaction) (Snapshot, error) {
	model, ok := l.model.(SummaryModel)
	if !ok {
		return l.contextStop(ctx, snap, errors.New("harness: summary model is unavailable"))
	}
	n := len(c.Attempts)
	if n > 0 && c.Attempts[n-1].Done && c.Attempts[n-1].Failure == "" {
		text, err := model.Summary(*c.Attempts[n-1].Response)
		if err == nil {
			err = c.Plan.ValidateResult(text)
		}
		if err != nil {
			return l.contextStop(ctx, snap, err)
		}
		return snap, j.FinishCompaction(ctx, c.Plan.ID, text)
	}
	if n >= c.Plan.MaxAttempts {
		return l.contextStop(ctx, snap, fmt.Errorf("harness: compaction exhausted %d attempts; original context retained", n))
	}
	if run.Budget.Tokens > 0 {
		totals, err := l.journal.Totals(ctx, run.ID)
		if err != nil {
			return snap, err
		}
		remaining := run.Budget.Tokens - totals.Usage.InputTokens - totals.Usage.OutputTokens - totals.ReservedTokens
		if c.Plan.TokenReserve > remaining {
			return l.runner.Commit(ctx, snap, core.StopRequested{Reason: core.StopBudgetExhausted}, Facts{Failure: "remaining token budget cannot reserve the summary request and output"})
		}
	}
	if err := j.StartCompactionAttempt(ctx, c.Plan.ID, n+1); err != nil {
		return snap, err
	}
	l.note(ctx, 0, "compacting session context", snap, "compaction_id", c.Plan.ID, "source_after", c.Plan.After, "source_through", c.Plan.Through, "attempt", n+1)
	call, cancel := ctx, context.CancelFunc(func() {})
	if w := run.Budget.WallTime; w > 0 {
		call, cancel = context.WithTimeout(ctx, run.Created.Add(w).Sub(l.cfg.Now()))
	}
	defer cancel()
	response, err := l.model.Complete(call, c.Plan.Request)
	if ctx.Err() != nil {
		return snap, ctx.Err()
	}
	if err == nil {
		var text string
		text, err = model.Summary(response)
		if err == nil {
			err = c.Plan.ValidateResult(text)
		}
	}
	result := CompactAttempt{Number: n + 1, Done: true, UnknownUsage: !response.UsageKnown}
	if len(response.Body) > 0 {
		result.Response = &response
	} else {
		result.UnknownUsage = true
	}
	if err != nil {
		result.Failure = err.Error()
	}
	if saveErr := j.SaveCompactionAttempt(ctx, c.Plan.ID, result); saveErr != nil {
		return snap, saveErr
	}
	if call.Err() != nil {
		return l.runner.Commit(ctx, snap, core.StopRequested{Reason: core.StopTimedOut}, Facts{Failure: "wall-time budget exhausted during compaction"})
	}
	var stop *StopError
	if errors.As(err, &stop) {
		return l.runner.Commit(ctx, snap, core.StopRequested{Reason: stop.Reason}, Facts{Failure: err.Error()})
	}
	if err != nil {
		return snap, sleep(ctx, l.backoff(n+1))
	}
	return snap, nil
}

func (l *Loop) protectedCut(run Run, s contextSelection) int {
	for i := len(s.Entries) - 1; i >= 0; i-- {
		if s.Entries[i].RunID == run.ID && s.Entries[i].Step != nil {
			return i
		}
	}
	return len(s.Entries)
}
