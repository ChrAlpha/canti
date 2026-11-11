package harness

import (
	"context"
	"fmt"
	"path"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

// lineContext sets the closed runs of the run's line, newest first, which
// replace the earlier conversation context: a run links to the previous run of
// its state directory unless it started the line fresh. Prior runs are
// navigation, not instructions; their results stay in the archive.
func (l *Loop) lineContext(ctx context.Context, run Run, f *Frame) error {
	prior, more, err := l.journal.PreviousRuns(ctx, run.ID)
	if err != nil {
		return err
	}
	if len(prior) > MaxPreviousRuns {
		return contextError{fmt.Errorf("harness: previous runs page holds %d runs", len(prior))}
	}
	for i, p := range prior {
		if p.Run.ID == "" || p.Run.ID == run.ID || !p.Closed || (i > 0 && prior[i-1].Run.ID == p.Run.ID) {
			return contextError{fmt.Errorf("harness: invalid previous run %q", p.Run.ID)}
		}
	}
	f.Previous, f.PreviousMore = prior, more
	return nil
}

// previousLine renders one closed run of the line for the stable prefix.
func previousLine(p PreviousRun) string {
	outcome := "unresolved"
	switch p.State.Phase {
	case core.PhaseReturned:
		outcome = "returned"
	case core.PhaseStopped:
		outcome = "stopped: " + string(p.State.Reason)
	}
	goal, _ := cut(lineBreaks.Replace(p.Run.Goal), PreviousGoalBytes)
	return fmt.Sprintf("- %s: %s; %s; %s\n", p.Run.ID, goal, outcome, path.Join(archive.RunsRoot, p.Run.ID)+"/")
}
