package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/docker"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

func (a *app) submitSession(ctx context.Context, session, input, goal, constraints string, explicitConstraints bool, s spec, url string, once bool) (int, error) {
	if code, err := a.checkSession(session, input, goal, constraints, s, url); err != nil {
		return code, err
	}
	if err := os.MkdirAll(a.state, 0o700); err != nil {
		return exitError, err
	}
	store, done, err := a.open(ctx, true)
	if err != nil {
		return exitError, err
	}
	defer done()
	previous, err := store.SessionInput(ctx, session, input)
	if err == nil {
		if previous.Goal != goal || explicitConstraints && previous.Constraints != constraints {
			return exitError, errors.New("session input ID already exists with different content")
		}
		c, err := decodeConfig(previous, a.workspace)
		if err != nil {
			return exitError, err
		}
		snap, err := store.Latest(ctx, previous.ID)
		done()
		return a.report(snap, err, exitActive, c)
	}
	if !errors.Is(err, harness.ErrNotFound) {
		return exitError, err
	}
	if !explicitConstraints {
		prior, err := store.SessionRuns(ctx, session, 0, 1)
		if err != nil {
			return exitError, err
		}
		if len(prior) > 0 {
			constraints = prior[0].Constraints
		}
	}
	if err := a.closed(ctx, store); err != nil {
		return exitError, err
	}
	run, initial, err := newRun(goal, constraints, s, time.Now())
	if err != nil {
		return exitUsage, err
	}
	if a.credErr != nil {
		return exitError, a.credErr
	}
	m, err := newModel(s.config, url, a.cred)
	if err != nil {
		return exitError, err
	}
	x, err := newExecutor(s.config, a.state)
	if err != nil {
		return exitError, err
	}
	drive, release := a.drive(ctx, once)
	defer release()
	l, err := a.loop(store, x, m, s.config, run.Budget)
	if err != nil {
		return exitError, err
	}
	run.SessionID, run.InputID = session, input
	run, err = store.Submit(ctx, run, initial)
	if err != nil {
		return exitError, err
	}
	a.log.Info("accepted session input", "session_id", session, "session_seq", run.SessionSeq, "input_id", input, "run_id", run.ID)
	snap, err := drive(l, run.ID)
	if err != nil {
		return exitError, fmt.Errorf("session %s run %s: %w", session, run.ID, err)
	}
	done()
	return a.report(snap, nil, exitActive, s.config)
}

// checkSession refuses an invalid session input before submitSession
// touches the state, and one that a state without a journal refuses before
// it creates the state. A journal that exists once a check fails may hold
// the input, whose retry the lookup under the lock reports.
func (a *app) checkSession(session, input, goal, constraints string, s spec, url string) (int, error) {
	if session == "" || input == "" {
		return exitUsage, errors.New("-session and -input are required together")
	}
	if !sqlite.ValidKey(session) || !sqlite.ValidKey(input) {
		return exitUsage, errors.New("-session and -input must be 1 to 128 ASCII letters, digits, underscores, or hyphens")
	}
	if err := docker.CheckLayout(a.workspace, a.state); err != nil {
		return exitError, err
	}
	code, err := a.checkNewInput(goal, constraints, s, url)
	if _, statErr := os.Stat(filepath.Join(a.state, "canti.db")); err != nil && errors.Is(statErr, fs.ErrNotExist) {
		return code, err
	}
	return exitOK, nil
}

func (a *app) checkNewInput(goal, constraints string, s spec, url string) (int, error) {
	if _, _, err := newRun(goal, constraints, s, time.Now()); err != nil {
		return exitUsage, err
	}
	if a.credErr != nil {
		return exitError, a.credErr
	}
	if _, err := newModel(s.config, url, a.cred); err != nil {
		return exitError, err
	}
	return exitOK, nil
}
