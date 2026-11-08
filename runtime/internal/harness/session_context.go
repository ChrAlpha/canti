package harness

import (
	"context"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

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
