package harness

import (
	"context"
	"errors"
	"math"

	"github.com/ChrAlpha/canti/core"
)

type contextError struct{ error }

func (l *Loop) frame(run Run, s core.State) Frame {
	return Frame{Goal: run.Goal, Constraints: run.Constraints, Environment: l.cfg.Environment,
		Step: s.Step, MaxSteps: s.Limits.MaxSteps, Attempt: s.Attempt, MaxAttempts: s.Limits.MaxAttempts, RunID: s.RunID, SessionID: run.SessionID,
		DecisionTool: l.cfg.DecisionTool, RetrievalEnabled: l.cfg.Reader != nil, ImportsEnabled: l.cfg.Reader != nil && l.cfg.Reader.Imports != "", SummaryBytes: l.cfg.SummaryBytes}
}

func ContextFit(cfg LoopConfig, run Run, limits core.Limits) (Fit, error) {
	cfg, err := normalize(cfg)
	if err != nil {
		return Fit{}, err
	}
	state, err := core.NewState(run.ID, limits)
	if err != nil {
		return Fit{}, err
	}
	f := (&Loop{cfg: cfg}).frame(run, state)
	f.SessionID = "admission"
	f.Window = &Window{FirstStep: 1}
	return f.fit(cfg.Limits)
}

func (l *Loop) prepare(ctx context.Context, run Run, snap Snapshot) (Snapshot, error) {
	j := l.journal
	if err := l.validatePreparation(ctx, run, snap); err != nil {
		return l.refuse(ctx, snap, err)
	}
	cp, base, pending, err := l.preparationCheckpoint(ctx, run, snap)
	if err != nil {
		return snap, err
	}
	if pending != nil {
		return l.compact(ctx, run, snap, j, *pending)
	}
	selection, err := l.contextSources(ctx, j, run, cp)
	if err != nil {
		return l.refuse(ctx, snap, err)
	}
	if selection.More && snap.State.RequestRev == 0 {
		return l.compactBacklog(ctx, run, snap, selection, base)
	}
	f := l.selectedFrame(run, snap.State, selection)
	if err := l.prepareRepair(ctx, snap, &f); err != nil {
		return l.refuse(ctx, snap, err)
	}
	if err := PublishHistory(ctx, l.journal, l.cfg.History, run.ID); err != nil {
		return snap, err
	}
	unrestricted := l.cfg.Limits
	unrestricted.Total = math.MaxInt
	p, err := f.Prompt(unrestricted)
	if err != nil {
		return l.contextStop(ctx, snap, err)
	}
	f.capture.Bytes.Limit = l.cfg.Limits.Total
	used := len(p.System) + len(p.User())
	if snap.State.RequestRev == 0 {
		f.capture.Compaction = l.compactionCheck(selection, used, l.contextGrowth(f))
	}
	need := selection.More || used+l.contextGrowth(f) > l.cfg.Limits.Total
	requested := l.compactThreshold(selection)
	if snap.State.RequestRev == 0 && (need || requested) {
		plan, err := l.planCompaction(ctx, run, snap, selection, base, used, need)
		if err != nil {
			return l.contextStop(ctx, snap, err)
		}
		if plan != nil {
			return snap, j.PlanCompaction(ctx, *plan)
		}
	}
	if selection.More || used > l.cfg.Limits.Total {
		return l.contextStop(ctx, snap, errors.New("protected context does not fit after compaction"))
	}
	req, err := l.model.Freeze(p)
	if err != nil {
		return l.contextStop(ctx, snap, err)
	}
	if admissionErr := l.admitRequest(req); admissionErr != nil {
		if snap.State.RequestRev == 0 {
			plan, err := l.planCompaction(ctx, run, snap, selection, base, used, true)
			if err == nil && plan != nil {
				return snap, j.PlanCompaction(ctx, *plan)
			}
		}
		return l.contextStop(ctx, snap, admissionErr)
	}
	req.Window, req.Context, req.CheckpointID = f.Window, f.capture, cp.ID
	return l.runner.Commit(ctx, snap, core.RequestFrozen{}, Facts{Request: &req})
}

func (l *Loop) prepareRepair(ctx context.Context, snap Snapshot, f *Frame) error {
	if snap.State.RequestRev == 0 {
		return nil
	}
	req, err := l.journal.LatestRequest(ctx, snap.State.RunID)
	if err != nil {
		return err
	}
	if req.Window == nil || *req.Window != *f.Window || req.CheckpointID != f.capture.CheckpointID {
		return contextError{errors.New("frozen context changed during protocol repair")}
	}
	f.capture.Compaction = CompactionCheck{Status: "request_revision"}
	resp, err := l.journal.Response(ctx, snap.State.RunID, snap.State.Step, snap.State.Attempt-1)
	if err != nil {
		return err
	}
	body := l.decision(resp)
	if _, err := core.Decode(body); err != nil {
		f.Rejected = &Rejection{Error: err.Error(), Response: string(body)}
	}
	return nil
}

func (l *Loop) contextStop(ctx context.Context, snap Snapshot, err error) (Snapshot, error) {
	return l.runner.Commit(ctx, snap, core.StopRequested{Reason: core.StopUnrecoverable}, Facts{Failure: err.Error()})
}

func (l *Loop) contextGrowth(f Frame) int {
	growth := f.stepShellBytes(l.cfg.Limits) + f.tailBytes(l.cfg.Limits)
	if f.RetrievalEnabled {
		growth += evidenceReserve()
	}
	return growth
}

func (l *Loop) compactThreshold(s contextSelection) bool {
	steps, size := 0, 0
	for _, e := range s.Entries {
		if e.Step != nil {
			steps++
		}
		size += len(l.entryText(e, ""))
	}
	return l.cfg.CompactSteps > 0 && steps >= l.cfg.CompactSteps || l.cfg.CompactBytes > 0 && size >= l.cfg.CompactBytes
}

func (l *Loop) refuse(ctx context.Context, snap Snapshot, err error) (Snapshot, error) {
	if errors.As(err, new(contextError)) {
		return l.contextStop(ctx, snap, err)
	}
	return snap, err
}

func (l *Loop) validatePreparation(ctx context.Context, run Run, snap Snapshot) error {
	if snap.State.Step == 1 && snap.State.RequestRev == 0 {
		f := l.frame(run, snap.State)
		f.Window = &Window{FirstStep: 1}
		if err := f.ValidateContext(l.cfg.Limits); err != nil {
			return contextError{err}
		}
	}
	if run.SessionID != "" && run.SessionSeq < 1 {
		return contextError{errors.New("harness: session sequence is required")}
	}
	if snap.State.Step > 1 || snap.State.RequestRev > 0 {
		previous, err := l.journal.LatestRequest(ctx, run.ID)
		if errors.Is(err, ErrNotFound) {
			return contextError{errors.New("harness: durable context request is missing after progress")}
		}
		if err != nil {
			return err
		}
		if previous.Window == nil {
			return contextError{errors.New("harness: context metadata missing during a run")}
		}
		if err := previous.Window.Validate(snap.State.Step); err != nil {
			return contextError{err}
		}
	}
	return nil
}

func (l *Loop) preparationCheckpoint(ctx context.Context, run Run, snap Snapshot) (Checkpoint, string, *Compaction, error) {
	cp, err := l.journal.ActiveCheckpoint(ctx, run.ContextOwner())
	if err != nil {
		return cp, "", nil, err
	}
	base := cp.ID
	if snap.State.RequestRev > 0 {
		return cp, base, nil, nil
	}
	pending, err := l.journal.PendingCompaction(ctx, run.ID, snap.Seq)
	if errors.Is(err, ErrNotFound) {
		return cp, base, nil, nil
	}
	if err != nil {
		return cp, base, nil, err
	}
	if pending.Summary == "" {
		return cp, base, &pending, nil
	}
	return pending.Checkpoint(), base, nil, nil
}

func (l *Loop) compactBacklog(ctx context.Context, run Run, snap Snapshot, selection contextSelection, base string) (Snapshot, error) {
	size := SummaryCost(selection.Checkpoint.Summary)
	for _, e := range selection.Entries {
		size += len(l.workingEntryText(e, run.ID))
	}
	plan, err := l.planCompaction(ctx, run, snap, selection, base, size, true)
	if err != nil {
		return l.contextStop(ctx, snap, err)
	}
	return snap, l.journal.PlanCompaction(ctx, *plan)
}

func (l *Loop) compactionCheck(s contextSelection, used, growth int) CompactionCheck {
	c := CompactionCheck{Status: "evaluated", Used: used, Capacity: l.cfg.Limits.Total - growth, Growth: growth, WindowSteps: l.cfg.CompactSteps, WindowBytes: l.cfg.CompactBytes}
	size := 0
	for _, e := range s.Entries {
		if e.Step != nil {
			c.Steps++
		}
		size += len(l.entryText(e, ""))
	}
	if used > c.Capacity {
		c.Reasons = append(c.Reasons, "capacity")
	}
	if l.cfg.CompactSteps > 0 && c.Steps >= l.cfg.CompactSteps {
		c.Reasons = append(c.Reasons, "steps")
	}
	if l.cfg.CompactBytes > 0 && size >= l.cfg.CompactBytes {
		c.Reasons = append(c.Reasons, "bytes")
	}
	return c
}
