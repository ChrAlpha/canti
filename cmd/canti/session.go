package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/internal/harness/docker"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
	"github.com/ChrAlpha/canti/internal/host"
	"github.com/ChrAlpha/canti/internal/hostrpc"
)

func (a *app) submitSession(ctx context.Context, session, input, goal, constraints string, explicitConstraints bool, s host.Spec, url string, once bool) (int, error) {
	if code, err := a.checkSession(session, input, goal, constraints, s, url); err != nil {
		return code, err
	}
	if a.remoteMode() {
		return a.remoteSubmit(ctx, hostrpc.Submission{SessionID: session, InputID: input, Goal: goal, Constraints: constraints,
			ExplicitConstraints: explicitConstraints, Spec: hostrpc.SpecOf(s)}, a.binding(s.Config.API, url), once)
	}
	if err := os.MkdirAll(a.state, 0o700); err != nil {
		return exitError, err
	}
	h, err := a.openHost(ctx, true)
	if err != nil {
		return exitError, err
	}
	defer h.Close()
	drive, release := a.drive(ctx, once)
	defer release()
	b := a.binding(s.Config.API, url)
	ack, err := h.Submit(ctx, host.Submission{SessionID: session, InputID: input, Goal: goal, Constraints: constraints,
		ExplicitConstraints: explicitConstraints, Spec: s, Binding: b, Now: time.Now()})
	switch {
	case errors.Is(err, host.ErrConflict):
		return exitError, err
	case err != nil:
		return submitCode(err), err
	case ack.Replayed:
		a.noteDiffers(input, ack.Run.ID, ack.Differs)
		h.Close()
		return a.report(ack.Snapshot, nil, exitActive, ack.Config)
	}
	snap, err := drive(h, ack.Run.ID, b)
	if err != nil {
		label := "session " + session + " run "
		if session == "" {
			label = "input " + input + " run "
		}
		return exitError, fmt.Errorf("%s%s: %w", label, ack.Run.ID, err)
	}
	h.Close()
	return a.report(snap, nil, exitActive, ack.Config)
}

var inputChecked = func() {}

// checkSession refuses an invalid input, of a session or of none, before
// submitSession touches the state, and one that a state without a journal
// refuses before it creates the state. A journal that exists once a check
// fails may hold the input, whose retry the lookup under the lock reports.
func (a *app) checkSession(session, input, goal, constraints string, s host.Spec, url string) (int, error) {
	if input == "" {
		return exitUsage, errors.New("-session requires -input")
	}
	if session != "" && !sqlite.ValidKey(session) || !sqlite.ValidKey(input) {
		return exitUsage, errors.New("-session and -input must be 1 to 128 ASCII letters, digits, underscores, or hyphens")
	}
	if err := docker.CheckLayout(a.workspace, a.state); err != nil {
		return exitError, err
	}
	code, err := a.checkNewInput(goal, constraints, s, url)
	inputChecked()
	if _, statErr := os.Stat(filepath.Join(a.state, "canti.db")); err != nil && errors.Is(statErr, fs.ErrNotExist) {
		return code, err
	}
	return exitOK, nil
}

func (a *app) checkNewInput(goal, constraints string, s host.Spec, url string) (int, error) {
	if _, _, err := host.NewRun(goal, constraints, s, time.Now()); err != nil {
		return exitUsage, err
	}
	if err := a.checkCredential(context.Background(), s.Config, url); err != nil {
		return exitError, err
	}
	return exitOK, nil
}

// noteDiffers says on stderr, for a submission of input that replayed run,
// the settings the submission named otherwise, if any, which the run does
// not take: it keeps its own.
func (a *app) noteDiffers(input, run string, differs []string) {
	if len(differs) > 0 {
		fmt.Fprintf(a.stderr, "canti: input %s names run %s, which keeps its own settings; this submission differs in %s\n",
			input, run, strings.Join(differs, ", "))
	}
}
