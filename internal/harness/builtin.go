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
	HandoverPrefix  = handoverName + " "
	DiscoveryMarker = "# canti:discovery"
)

const actionFailure, actionFailureBytes = "Context action failed: ", 2048

// HandoverSpace holds the bytes that may follow the name of a handover
// action, whose text is free, in place of the space of HandoverPrefix.
const handoverName, HandoverSpace = "canti:handover", " \t\n\r"

// handoverText reports whether payload is a handover action, its name alone
// or followed by a byte of HandoverSpace, and returns the text after that
// byte.
func handoverText(payload string) (string, bool) {
	rest, ok := strings.CutPrefix(payload, handoverName)
	if !ok || rest != "" && !strings.ContainsAny(rest[:1], HandoverSpace) {
		return "", false
	}
	if rest == "" {
		return "", true
	}
	return rest[1:], true
}

// route names who carries out the effect of an exec decision.
type route int

const (
	byExecutor route = iota
	inProcess
	refused
)

const handoverRequired = "handover is required before any more work; use canti:handover"

// route decides who carries out the effect of s. A bound job decides by its
// ID. Before binding, an effect that a requested handover forbids is
// refused, and the harness's own actions run in process.
func (l *Loop) route(ctx context.Context, s core.State) (route, error) {
	if s.JobID != "" {
		if s.JobID == "read_"+s.EffectID {
			return inProcess, nil
		}
		return byExecutor, nil
	}
	if s.Intent() == core.IntentBind {
		req, err := l.journal.Request(ctx, s.RunID, s.Step, s.RequestRev)
		if err != nil {
			return byExecutor, err
		}
		if _, handover := handoverText(s.Decision.Payload); req.Window != nil && req.Window.Requested && !handover {
			return refused, nil
		}
	}
	if _, handover := handoverText(s.Decision.Payload); handover || strings.HasPrefix(s.Decision.Payload, RetrievePrefix) {
		return inProcess, nil
	}
	return byExecutor, nil
}

// refuseWork records the effect of snap, which a requested handover forbids,
// as failed to start: no job is bound, started, or run.
func (l *Loop) refuseWork(ctx context.Context, snap Snapshot) (Snapshot, error) {
	obs := Observation{Execution: core.ExecutionFailedToStart, Preview: actionFailure + handoverRequired}
	return l.runner.Commit(ctx, snap, core.Observed{EffectID: snap.State.EffectID, Execution: core.ExecutionFailedToStart}, Facts{Observation: &obs})
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

// proposedHandover is the handover that payload proposes in window w: its
// text, kept verbatim, for the run and window that the harness names.
func proposedHandover(runID string, w *Window, payload string, limit int) (Handover, error) {
	text, _ := handoverText(payload)
	h := Handover{RunID: runID, Text: text}
	if w == nil || !w.Requested {
		return h, errors.New("handover was not requested for this step")
	}
	h.FirstStep = w.FirstStep
	return h, h.Validate(runID, w.FirstStep, limit)
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
	if strings.HasPrefix(command, RetrievePrefix) {
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
		h, err = proposedHandover(snap.State.RunID, req.Window, command, l.cfg.HandoverBytes)
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
	if ctx.Err() != nil {
		return obs, ctx.Err()
	}
	if err != nil {
		code = 1
		obs.Preview = actionFailure + clip(err.Error(), actionFailureBytes)
	}
	return obs, nil
}
