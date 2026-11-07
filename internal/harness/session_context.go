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

func (l *Loop) sessionContext(ctx context.Context, run Run, f *Frame) error {
	if run.SessionID == "" {
		return nil
	}
	j := l.journal
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
	f.sessionNavigation = prior[0].ID
	var parts []sessionPart
	var sources []ContextSessionSource
	for _, p := range prior {
		snap, err := l.journal.Latest(ctx, p.ID)
		if err != nil {
			return err
		}
		if !snap.State.Closed() {
			return contextError{fmt.Errorf("harness: earlier session run %q is not terminal", p.ID)}
		}
		source := fmt.Sprintf("Run %s (session sequence %d; source %s/%s/", inline(p.ID), p.SessionSeq, archive.RunsRoot, inline(p.ID))
		goal := "Earlier user goal:\n" + fullBlock(strings.TrimSpace(p.Goal))
		stopped := snap.State.Phase == core.PhaseStopped
		part := sessionPart{head: source + "goal and result; returned):\n", body: goal + "Earlier assistant return:\n" + fullBlock(snap.State.Decision.Payload)}
		if stopped {
			totals, err := l.journal.Totals(ctx, p.ID)
			if err != nil {
				return err
			}
			part = sessionPart{head: source + fmt.Sprintf("goal, steps, and result): stopped, reason %s, recorded steps %d; no assistant return.\n", inline(string(snap.State.Reason)), totals.Steps), body: goal}
		}
		provenance := ContextSessionSource{RunID: p.ID, SessionSeq: p.SessionSeq, Status: string(snap.State.Phase), IncludedGoal: true}
		rest := sessionText(parts)
		if !sessionContextFits(base, l.cfg.Limits, nav+part.head+part.body+rest) {
			if stopped && sessionContextFits(base, l.cfg.Limits, nav+part.head+rest) {
				part.body, provenance.IncludedGoal = "", false
				parts, sources = append([]sessionPart{part}, parts...), append([]ContextSessionSource{provenance}, sources...)
			}
			break
		}
		parts, sources = append([]sessionPart{part}, parts...), append([]ContextSessionSource{provenance}, sources...)
	}
	if err := l.sessionHandovers(ctx, base, nav, parts, sources); err != nil {
		return err
	}
	f.SessionContext = nav + sessionText(parts)
	f.sessionSources = sources
	return nil
}

// sessionPart is the dialogue of an earlier session run as selected: its
// header, the path of its latest handover where one fits, and its goal and
// return unless only the header fits.
type sessionPart struct{ head, path, body string }

func sessionText(parts []sessionPart) string {
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(p.head + p.path + p.body)
	}
	return b.String()
}

// sessionHandovers adds to the selected parts, newest first, the path of the
// latest handover of each run where it still fits, so that a path never
// displaces dialogue.
func (l *Loop) sessionHandovers(ctx context.Context, base Frame, nav string, parts []sessionPart, sources []ContextSessionSource) error {
	for i := len(parts) - 1; i >= 0; i-- {
		step, err := latestHandover(ctx, l.journal, sources[i].RunID)
		if err != nil {
			return err
		}
		if step == 0 {
			continue
		}
		parts[i].path = fmt.Sprintf("Latest handover of this run, agent-written and possibly overtaken by later steps: %s/%s/%s/%s/%s\n",
			archive.RunsRoot, inline(sources[i].RunID), archive.StepsDir, archive.StepDir(step), archive.CommandFile)
		if !sessionContextFits(base, l.cfg.Limits, nav+sessionText(parts)) {
			parts[i].path = ""
			continue
		}
		sources[i].HandoverStep = step
	}
	return nil
}

// latestHandover returns the step of the latest handover that run accepted,
// or 0: its final step when that is one, otherwise the source of the window
// of its latest request.
func latestHandover(ctx context.Context, j Journal, runID string) (int, error) {
	last, err := j.Recent(ctx, runID, 0, 1)
	if err != nil {
		return 0, err
	}
	if len(last) == 1 && last[0].acceptedHandover() != nil {
		return last[0].Step, nil
	}
	req, err := j.LatestRequest(ctx, runID)
	switch {
	case errors.Is(err, ErrNotFound):
		return 0, nil
	case err != nil:
		return 0, err
	case req.Window == nil:
		return 0, nil
	}
	return req.Window.SourceStep, nil
}

func sessionContextFits(base Frame, limits PromptLimits, content string) bool {
	base.SessionContext = content
	return base.ValidateContext(limits) == nil
}
