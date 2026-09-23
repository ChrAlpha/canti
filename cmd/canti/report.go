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
		snap, err := store.Latest(ctx, run.ID)
		if err != nil {
			return exitError, err
		}
		t, err := store.Totals(ctx, run.ID)
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
				"budget": map[string]any{"wall_time": b.WallTime.String(), "tokens": b.Tokens, "output_bytes": b.OutputBytes, "job_timeout": b.JobTimeout.String()}},
			"seq": snap.Seq, "phase": st.Phase, "intent": st.Intent(), "step": st.Step, "attempt": st.Attempt,
			"request_rev": st.RequestRev, "reason": st.Reason, "decision": decision, "effect_id": st.EffectID, "job_id": st.JobID,
			"totals": map[string]any{"input_tokens": t.Usage.InputTokens, "output_tokens": t.Usage.OutputTokens, "output_bytes": t.OutputBytes, "steps": t.Steps},
		}
		return exitOK, a.encoder().Encode(out)
	}
}

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
			page, err := store.Transitions(ctx, run.ID, after, 256)
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
			if len(page) < 256 {
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

// open opens the workspace journal, holding the workspace lock until done
// when lock is set. Unless create is set, a missing journal is an error.
// read opens the workspace journal without the lock and finds the run.
func (a *app) read(ctx context.Context) (*sqlite.Store, harness.Run, func(), error) {
	store, done, err := a.open(ctx, false, false)
	if err != nil {
		return nil, harness.Run{}, nil, err
	}
	run, err := a.find(ctx, store)
	if err != nil {
		done()
	}
	return store, run, done, err
}

// report prints the outcome of the run at snap and returns its exit status,
// or active while the run is still active.
func (a *app) report(snap harness.Snapshot, err error, active int) (int, error) {
	s := snap.State
	switch {
	case err != nil:
		return exitError, err
	case s.Phase == core.PhaseReturned:
		fmt.Fprintln(a.stdout, s.Decision.Payload)
		return exitOK, nil
	case s.Phase == core.PhaseStopped:
		fmt.Fprintf(a.stderr, "canti: run %s stopped: %s\n", s.RunID, s.Reason)
		return exitStopped, nil
	case s.Phase == core.PhaseNeedsResolution:
		fmt.Fprintf(a.stderr, "canti: run %s needs resolution: the outcome of effect %s is unknown. Inspect it, then "+
			"record its job with \"canti resolve -run %[1]s -job\" or its outcome with \"canti resolve -run %[1]s -execution STATE\".\n", s.RunID, s.EffectID)
		return exitResolution, nil
	}
	fmt.Fprintf(a.stderr, "canti: run %s is %s; continue it with \"canti resume -run %[1]s\".\n", s.RunID, s.Phase)
	return active, nil
}
