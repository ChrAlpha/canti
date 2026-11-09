package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
	"github.com/ChrAlpha/canti/runtime/internal/host"
	hostserver "github.com/ChrAlpha/canti/runtime/internal/hostrpc"
)

func statusCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	return func(ctx context.Context) (int, error) {
		store, run, c, err := a.read(ctx)
		if err != nil {
			return exitError, err
		}
		defer store.Close()
		snap, t, err := consistent(ctx, store, run.ID)
		if err != nil {
			return exitError, err
		}
		st, b := snap.State, run.Budget
		var decision *core.Decision
		if st.Decision.Kind != "" {
			decision = &st.Decision
		}
		out := map[string]any{
			"run": map[string]any{"id": run.ID, "goal": run.Goal, "constraints": run.Constraints, "created": run.Created,
				"session_id": run.SessionID, "session_seq": run.SessionSeq, "input_id": run.InputID,
				"budget": map[string]any{"wall_time": b.WallTime.String(), "tokens": b.Tokens, "output_bytes": b.OutputBytes, "job_timeout": b.JobTimeout.String()}},
			"execution": map[string]any{"ephemeral": c.Ephemeral, "environment": hostserver.EnvironmentOf(c)},
			"seq":       snap.Seq, "phase": st.Phase, "intent": st.Intent(), "step": st.Step, "attempt": st.Attempt,
			"request_rev": st.RequestRev, "reason": st.Reason, "decision": decision, "effect_id": st.EffectID,
			"totals": map[string]any{"input_tokens": t.Usage.InputTokens, "output_tokens": t.Usage.OutputTokens, "output_bytes": t.OutputBytes, "steps": t.Steps, "coverage": t.Coverage},
		}
		return exitOK, a.encoder().Encode(out)
	}
}

func consistent(ctx context.Context, j harness.Journal, id string) (harness.Snapshot, harness.Totals, error) {
	for range 5 {
		snap, err := j.Latest(ctx, id)
		var t harness.Totals
		if err == nil {
			t, err = j.Totals(ctx, id)
		}
		var again harness.Snapshot
		if err == nil {
			again, err = j.Latest(ctx, id)
		}
		if err != nil || again.Seq == snap.Seq {
			return snap, t, err
		}
	}
	return harness.Snapshot{}, harness.Totals{}, fmt.Errorf("run %s changed during every attempt to read its status", id)
}

const historyPage = 8

func historyCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	return func(ctx context.Context) (int, error) {
		store, run, _, err := a.read(ctx)
		if err != nil {
			return exitError, err
		}
		defer store.Close()
		enc := a.encoder()
		for after := int64(0); ; {
			page, err := store.Transitions(ctx, run.ID, after, historyPage)
			if err != nil {
				return exitError, err
			}
			for _, t := range page {
				line := map[string]any{"seq": t.Seq, "event_id": t.EventID, "kind": t.Event.Kind(), "semantics": t.Semantics, "event": t.Event,
					"from": t.From.Phase, "to": t.To.Phase, "state": t.To, "facts": t.Facts}
				if err := enc.Encode(line); err != nil {
					return exitError, err
				}
				after = t.Seq
			}
			if len(page) < historyPage {
				return exitOK, nil
			}
		}
	}
}

func (a *app) encoder() *json.Encoder {
	enc := json.NewEncoder(a.stdout)
	enc.SetEscapeHTML(false)
	return enc
}

// read opens the workspace journal read-only, without the lock, and finds
// the run and its stored config.
func (a *app) read(ctx context.Context) (*sqlite.Store, harness.Run, host.RunConfig, error) {
	path := filepath.Join(a.state, "canti.db")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, harness.Run{}, host.RunConfig{}, fmt.Errorf("no runs in workspace %s", a.workspace)
	}
	store, err := sqlite.OpenReadOnly(ctx, path)
	if err != nil {
		return nil, harness.Run{}, host.RunConfig{}, err
	}
	run, c, err := host.Find(ctx, store, a.workspace, a.runID)
	if err != nil {
		store.Close()
	}
	return store, run, c, err
}

// report prints the outcome of the run at snap, of config c, and returns its
// exit status, or active while the run is still active. Callers release the
// workspace lock first: behind a stderr queue each line waits up to
// stderrFinalTime.
func (a *app) report(snap harness.Snapshot, err error, active int, c host.RunConfig) (int, error) {
	s := snap.State
	out := a.stderr
	if a.display != nil {
		out = finalStderr{a.display}
	}
	switch {
	case err != nil:
		return exitError, err
	case s.Phase == core.PhaseReturned:
		fmt.Fprintln(a.stdout, s.Decision.Payload)
		return exitOK, nil
	case s.Phase == core.PhaseStopped:
		if c.Environment != "" {
			fmt.Fprintf(out, "canti: inspect the persistent environment gate with canti environment inspect -environment %q; start explicitly after an acknowledged stop.\n", c.Environment)
		}
		fmt.Fprintf(out, "canti: run %s stopped: %s\n", s.RunID, s.Reason)
		return exitStopped, nil
	case s.Phase == core.PhaseNeedsResolution:
		fmt.Fprintf(out, "canti: run %s needs resolution: the outcome of effect %s is unknown. Inspect it, then "+
			"record its job with \"canti resolve -run %[1]s -job\" or its outcome with \"canti resolve -run %[1]s -execution STATE\"; "+
			"-execution records the outcome only and leaves a job that may still run untouched.\n", s.RunID, s.EffectID)
		if s.Reason != "" {
			fmt.Fprintf(out, "canti: the stop is recorded (%s), but its job may still run until it is resolved; "+
				"after \"canti resolve -run %s -job\" records a job that still runs, \"canti stop\" cancels it.\n", s.Reason, s.RunID)
		}
		return exitResolution, nil
	}
	next := "resume"
	switch why := host.Unresumable(c); {
	case why != "":
		fmt.Fprintf(out, "canti: run %s is %s and %s.\n", s.RunID, s.Phase, why)
		return active, nil
	case s.Phase == core.PhaseCancelling:
		next = "stop"
	}
	fmt.Fprintf(out, "canti: run %s is %s; continue it with \"canti %s -run %[1]s\".\n", s.RunID, s.Phase, next)
	return active, nil
}
