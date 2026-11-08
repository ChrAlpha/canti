package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

const sessionContextCandidates = 8

func (l *Loop) sessionContext(ctx context.Context, run Run, f *Frame) error {
	if run.SessionID == "" {
		return nil
	}
	if run.SessionSeq < 1 {
		return contextError{errors.New("harness: session sequence required")}
	}
	prior, err := l.journal.SessionRuns(ctx, run.SessionID, run.SessionSeq, 1)
	if err != nil {
		return err
	}
	f.SessionContext = fmt.Sprintf("Session %s, sequence %d. Follow previous_run_id in %s/%s/session to discover prior goals and returns. Current goal and constraints remain authoritative.\n", run.SessionID, run.SessionSeq, archive.RunsRoot, run.ID)
	if len(prior) > 1 {
		return contextError{errors.New("harness: invalid prior session page")}
	}
	if len(prior) == 1 {
		p := prior[0]
		if p.SessionID != run.SessionID || p.SessionSeq < 1 || p.SessionSeq >= run.SessionSeq || p.ID == run.ID {
			return contextError{errors.New("harness: invalid prior session identity")}
		}
		f.sessionNavigation = p.ID
		f.SessionContext += "Previous run: " + archive.RunsRoot + "/" + p.ID + "/.\n"
	}
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
