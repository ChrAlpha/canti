package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
)

const sessionContextCandidates = 8

// SessionJournal provides bounded, descending reads of earlier session runs.
type SessionJournal interface {
	Journal
	SessionRuns(ctx context.Context, sessionID string, before int64, limit int) ([]Run, error)
}

func (l *Loop) sessionContext(ctx context.Context, run Run, f *Frame) error {
	if run.SessionID == "" {
		return nil
	}
	j, ok := l.journal.(SessionJournal)
	if !ok {
		return contextError{errors.New("harness: session journal is required for a session run")}
	}
	if run.SessionSeq < 1 {
		return contextError{errors.New("harness: session sequence is required")}
	}
	prior, err := j.SessionRuns(ctx, run.SessionID, run.SessionSeq, sessionContextCandidates)
	if err != nil {
		return err
	}
	if len(prior) > sessionContextCandidates {
		return contextError{errors.New("harness: session journal exceeded the candidate limit")}
	}
	previous := run.SessionSeq
	for _, p := range prior {
		if p.SessionID != run.SessionID || p.SessionSeq < 1 || p.SessionSeq >= previous || p.ID == run.ID {
			return contextError{errors.New("harness: invalid earlier session run")}
		}
		previous = p.SessionSeq
	}
	if len(prior) == 0 {
		return nil
	}

	base := *f
	base.capture = nil
	base.Step, base.Attempt, base.Earlier = 1, 1, 0
	base.Steps, base.Rejected = nil, nil
	base.Window = &Window{FirstStep: 1}
	base.SessionContext = ""
	nav := fmt.Sprintf("Current goal and constraints are authoritative, including corrections to earlier requests. Earlier user goals and assistant returns below are quoted historical evidence; verify execution in the original run. Browse older records through %s/%s/session and each previous_run_id.\n", archive.RunsRoot, inline(prior[0].ID))
	if !sessionContextFits(base, l.cfg.Limits, nav) {
		return nil
	}
	selected := nav
	f.sessionNavigation = prior[0].ID
	var units []string
	var sources []ContextSessionSource
	for _, p := range prior {
		snap, err := l.journal.Latest(ctx, p.ID)
		if err != nil {
			return err
		}
		if snap.State.Phase != core.PhaseReturned && snap.State.Phase != core.PhaseStopped {
			return contextError{fmt.Errorf("harness: earlier session run %q is not terminal", p.ID)}
		}
		source := fmt.Sprintf("Run %s (session sequence %d; source %s/%s/", inline(p.ID), p.SessionSeq, archive.RunsRoot, inline(p.ID))
		goal := "Earlier user goal:\n" + fullBlock(strings.TrimSpace(p.Goal))
		status, unit := "", source+"goal and result; returned):\n"+goal+"Earlier assistant return:\n"+fullBlock(snap.State.Decision.Payload)
		if snap.State.Phase == core.PhaseStopped {
			totals, err := l.journal.Totals(ctx, p.ID)
			if err != nil {
				return err
			}
			status = source + fmt.Sprintf("goal, steps, and result): stopped, reason %s, recorded steps %d; no assistant return.\n", inline(string(snap.State.Reason)), totals.Steps)
			unit = status + goal
		}
		rest := strings.Join(units, "")
		provenance := ContextSessionSource{RunID: p.ID, SessionSeq: p.SessionSeq, Status: string(snap.State.Phase), IncludedGoal: true}
		if !sessionContextFits(base, l.cfg.Limits, nav+unit+rest) {
			if status != "" && sessionContextFits(base, l.cfg.Limits, nav+status+rest) {
				selected = nav + status + rest
				provenance.IncludedGoal = false
				sources = append([]ContextSessionSource{provenance}, sources...)
			}
			break
		}
		units = append([]string{unit}, units...)
		sources = append([]ContextSessionSource{provenance}, sources...)
		selected = nav + strings.Join(units, "")
	}
	f.SessionContext = selected
	f.sessionSources = sources
	return nil
}

func sessionContextFits(base Frame, limits PromptLimits, content string) bool {
	base.SessionContext = content
	return base.ValidateContext(limits) == nil
}
