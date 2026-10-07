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

// RetrievePrefix and HandoverPrefix begin the exec payloads that the harness
// performs itself instead of running them as commands. DiscoveryMarker
// begins a shell command that searches the step records; discovery skips the
// steps of all three.
const (
	RetrievePrefix  = "canti:retrieve "
	HandoverPrefix  = "canti:handover "
	DiscoveryMarker = "# canti:discovery"
)

const actionFailure, actionFailureBytes = "Context action failed: ", 2048

func (l *Loop) isBuiltin(ctx context.Context, snap Snapshot) (bool, error) {
	s := snap.State
	if strings.HasPrefix(s.Decision.Payload, RetrievePrefix) || strings.HasPrefix(s.Decision.Payload, HandoverPrefix) || s.JobID == "read_"+s.EffectID {
		return true, nil
	}
	if s.Intent() != core.IntentBind {
		return false, nil
	}
	req, err := l.journal.Request(ctx, s.RunID, s.Step, s.RequestRev)
	return req.Window != nil && req.Window.Requested, err
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
	req, err := l.journal.Request(ctx, snap.State.RunID, snap.State.Step, snap.State.RequestRev)
	if err != nil {
		return obs, err
	}
	if req.Window != nil && req.Window.Requested && !strings.HasPrefix(command, HandoverPrefix) {
		err = errors.New("handover is required before any more work; use canti:handover")
	} else if strings.HasPrefix(command, RetrievePrefix) {
		var q archive.RetrievalQuery
		err = decodeAction(strings.TrimPrefix(command, RetrievePrefix), &q)
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
	} else {
		var h Handover
		err = decodeAction(strings.TrimPrefix(command, HandoverPrefix), &h)
		if err == nil {
			if req.Window == nil || !req.Window.Requested {
				err = errors.New("handover was not requested for this step")
			} else {
				err = h.Validate(snap.State.RunID, req.Window.FirstStep, l.cfg.HandoverBytes)
			}
			if err == nil {
				run, loadErr := l.journal.Run(ctx, snap.State.RunID)
				if loadErr != nil {
					return obs, loadErr
				}
				f := l.frame(run, snap.State)
				f.Window = &Window{FirstStep: snap.State.Step + 1, SourceStep: snap.State.Step, Handover: h.Text}
				_, err = f.workCapacity(l.cfg.Limits)
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
		obs.Preview = actionFailure + clip(err.Error(), actionFailureBytes)
	}
	return obs, nil
}
