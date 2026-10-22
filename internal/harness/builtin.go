package harness

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
)

const RetrievePrefix = "canti:retrieve "
const DiscoveryMarker = "# canti:discovery"
const actionFailure, actionFailureBytes = "Context action failed: ", 2048

type route int

const (
	byExecutor route = iota
	inProcess
)

func effectRoute(s core.State) route {
	if s.JobID != "" {
		if s.JobID == "read_"+s.EffectID {
			return inProcess
		}
		return byExecutor
	}
	if strings.HasPrefix(s.Decision.Payload, "canti:") {
		return inProcess
	}
	return byExecutor
}

func decodeAction(text string, value any) error {
	d := json.NewDecoder(strings.NewReader(text))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("expected exactly one action object")
	}
	return nil
}

func (l *Loop) builtin(ctx context.Context, snap Snapshot) (Snapshot, bool, error) {
	s := snap.State
	var event core.Event
	facts := Facts{}
	switch s.Intent() {
	case core.IntentBind:
		event = core.JobBound{EffectID: s.EffectID, JobID: "read_" + s.EffectID}
	case core.IntentStart:
		event = core.JobStarted{EffectID: s.EffectID, JobID: s.JobID}
	case core.IntentInspect:
		obs, err := l.readAction(ctx, snap)
		if err != nil {
			return snap, true, err
		}
		event = core.Observed{EffectID: s.EffectID, Execution: obs.Execution}
		facts.Observation = &obs
	case core.IntentCancel:
		event = core.Observed{EffectID: s.EffectID, Execution: core.ExecutionCancelled}
		facts.Observation = &Observation{Execution: core.ExecutionCancelled, Preview: "Context action cancelled."}
	default:
		return snap, false, nil
	}
	next, err := l.runner.Commit(ctx, snap, event, facts)
	return next, true, err
}

func (l *Loop) readAction(ctx context.Context, snap Snapshot) (Observation, error) {
	code := 0
	obs := Observation{Execution: core.ExecutionExited, ExitCode: &code}
	if !strings.HasPrefix(snap.State.Decision.Payload, RetrievePrefix) {
		code = 1
		obs.Preview = actionFailure + "unknown context action"
		return obs, nil
	}
	var q archive.RetrievalQuery
	err := decodeAction(strings.TrimPrefix(snap.State.Decision.Payload, RetrievePrefix), &q)
	if l.cfg.Reader == nil {
		err = errors.New("retrieval is disabled for this run")
	}
	if err == nil {
		var r archive.Retrieval
		r, err = l.cfg.Reader.Retrieve(ctx, q)
		if err == nil {
			obs.Retrieval = &r
			refs := r
			refs.Evidence = nil
			body, _ := json.Marshal(refs)
			obs.Preview = string(body)
			if r.Error != "" {
				code = 1
			}
		}
	}
	if ctx.Err() != nil {
		return obs, ctx.Err()
	}
	if err != nil {
		code = 1
		obs.Preview = actionFailure + clip(err.Error(), actionFailureBytes)
	}
	return obs, nil
}
