package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

func statusCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	return func(ctx context.Context) (int, error) {
		store, run, done, err := a.read(ctx)
		if err != nil {
			return exitError, err
		}
		defer done()
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
			"seq": snap.Seq, "phase": st.Phase, "intent": st.Intent(), "step": st.Step, "attempt": st.Attempt,
			"request_rev": st.RequestRev, "reason": st.Reason, "decision": decision, "effect_id": st.EffectID, "job_id": st.JobID,
			"totals": map[string]any{"input_tokens": t.Usage.InputTokens, "output_tokens": t.Usage.OutputTokens, "output_bytes": t.OutputBytes, "steps": t.Steps},
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
		store, run, done, err := a.read(ctx)
		if err != nil {
			return exitError, err
		}
		defer done()
		enc := a.encoder()
		for after := int64(0); ; {
			page, err := store.Transitions(ctx, run.ID, after, historyPage)
			if err != nil {
				return exitError, err
			}
			for _, t := range page {
				line := map[string]any{"seq": t.Seq, "event_id": t.EventID, "kind": t.Event.Kind(), "event": t.Event,
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

// read opens the workspace journal without the lock and finds the run.
func (a *app) read(ctx context.Context) (*sqlite.Store, harness.Run, func(), error) {
	store, done, err := a.open(ctx, false, false)
	if err != nil {
		return nil, harness.Run{}, nil, err
	}
	run, _, err := a.find(ctx, store)
	if err != nil {
		done()
	}
	return store, run, done, err
}

// report prints the outcome of the run at snap and returns its exit status,
// or active while the run is still active. Callers release the workspace
// lock first: behind a stderr queue each line waits up to stderrFinalTime.
func (a *app) report(snap harness.Snapshot, err error, active int) (int, error) {
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
	if s.Phase == core.PhaseCancelling {
		next = "stop"
	}
	fmt.Fprintf(out, "canti: run %s is %s; continue it with \"canti %s -run %[1]s\".\n", s.RunID, s.Phase, next)
	return active, nil
}
