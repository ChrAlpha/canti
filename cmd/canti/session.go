package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/docker"
)

func (a *app) submitSession(ctx context.Context, session, input, goal, constraints string, explicitConstraints bool, s spec, url string, once bool) (int, error) {
	if session == "" || input == "" {
		return exitUsage, errors.New("-session and -input are required together")
	}
	if err := docker.CheckLayout(a.workspace, a.state); err != nil {
		return exitError, err
	}
	if err := os.MkdirAll(a.state, 0o700); err != nil {
		return exitError, err
	}
	store, done, err := a.open(ctx, true, true)
	if err != nil {
		return exitError, err
	}
	defer done()
	previous, err := store.SessionInput(ctx, session, input)
	if err == nil {
		if previous.Goal != goal || explicitConstraints && previous.Constraints != constraints {
			return exitError, errors.New("session input ID already exists with different content")
		}
		if _, err := decodeConfig(previous, a.workspace); err != nil {
			return exitError, err
		}
		snap, err := store.Latest(ctx, previous.ID)
		return a.report(snap, err, exitActive)
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
	return a.report(snap, nil, exitActive)
}
