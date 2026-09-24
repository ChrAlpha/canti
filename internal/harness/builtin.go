package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/ChrAlpha/canti/core"
)

const retrievePrefix = "canti:retrieve "
const handoverPrefix = "canti:handover "

func (l *Loop) isBuiltin(command string) bool {
	return strings.HasPrefix(command, retrievePrefix) || strings.HasPrefix(command, handoverPrefix)
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
	command := snap.State.Decision.Payload
	var err error
	if strings.HasPrefix(command, retrievePrefix) {
		var q RetrievalQuery
		err = decodeAction(strings.TrimPrefix(command, retrievePrefix), &q)
		if l.cfg.Reader == nil {
			err = errors.New("retrieval is disabled for this run")
		}
		if err == nil {
			var r Retrieval
			r, err = l.cfg.Reader.Retrieve(ctx, q)
			if err == nil {
				obs.Retrieval = &r
				body, _ := json.Marshal(r)
				obs.Preview = string(body)
				if r.Error != "" {
					code = 1
				}
			}
		}
	} else {
		var h Handover
		err = decodeAction(strings.TrimPrefix(command, handoverPrefix), &h)
		if err == nil {
			var req Request
			req, err = l.journal.Request(ctx, snap.State.RunID, snap.State.Step, snap.State.RequestRev)
			if err != nil {
				return obs, err
			}
			if req.Window == nil || !req.Window.Requested {
				err = errors.New("handover was not requested for this step")
			} else {
				err = h.Validate(snap.State.RunID, req.Window.FirstStep, l.cfg.HandoverBytes)
			}
			if err == nil {
				obs.Handover = &h
				obs.Preview = "Handover recorded; the next request starts a new context window."
			}
		}
	}
	if ctx.Err() != nil {
		return obs, ctx.Err()
	}
	if err != nil {
		code = 1
		obs.Preview = fmt.Sprintf("Context action failed: %s", clip(err.Error(), 2048))
	}
	return obs, nil
}
