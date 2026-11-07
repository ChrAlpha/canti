package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
)

// effect carries out the committed effect of snap by its acceptance: the
// harness performs or cancels a local action itself, and the executor binds,
// starts, inspects, or cancels an external one. An effect accepted before
// actions were recorded is only cancelled, in process when it was carried
// out in process and otherwise through the executor.
func (l *Loop) effect(ctx context.Context, run Run, snap Snapshot) (Snapshot, error) {
	s := snap.State
	accepted, err := ActionOf(ctx, l.journal, s)
	if err != nil {
		return snap, err
	}
	intent := s.Intent()
	switch {
	case accepted.Provenance == ProvenanceLegacy && intent != core.IntentCancel:
		return snap, fmt.Errorf("%w: run %s is %s", errLegacyEffect, s.RunID, s.Phase)
	case accepted.Local() && intent == core.IntentBind:
		return l.perform(ctx, run, snap, accepted.Action)
	case accepted.Local() && intent == core.IntentCancel:
		obs := Observation{Execution: core.ExecutionCancelled, Preview: "Context action cancelled."}
		return l.runner.Commit(ctx, snap, core.Observed{EffectID: s.EffectID, Execution: core.ExecutionCancelled}, Facts{Observation: &obs})
	case accepted.Local():
		return snap, fmt.Errorf("harness: the local action of effect %s is %s", s.EffectID, s.Phase)
	}
	switch intent {
	case core.IntentBind:
		return l.runner.Bind(ctx, snap)
	case core.IntentStart:
		return l.runner.Start(ctx, snap)
	case core.IntentInspect:
		return l.watch(ctx, run, snap)
	}
	next, err := l.cancel(ctx, run, snap)
	if err != nil && accepted.Provenance == ProvenanceLegacy && ctx.Err() == nil {
		err = fmt.Errorf("stopping an effect accepted before actions were recorded needs a reachable executor: %w", err)
	}
	return next, err
}

// perform carries out the local action a of snap and commits its outcome,
// with the window it changes, and the step's progress as one Performed.
// Nothing is committed once ctx is done, so a resume performs it again: a
// read from its sources as they are then, a local change identically.
func (l *Loop) perform(ctx context.Context, run Run, snap Snapshot, a Action) (Snapshot, error) {
	s := snap.State
	req, err := l.journal.Request(ctx, s.RunID, s.Step, s.RequestRev)
	if err != nil {
		return snap, err
	}
	var source Window
	if req.Window != nil {
		source = *req.Window
	}
	o, err := l.outcome(ctx, run, s, a, source)
	if err != nil {
		return snap, err
	}
	if ctx.Err() != nil {
		return snap, ctx.Err()
	}
	return l.runner.Commit(ctx, snap, core.Performed{EffectID: s.EffectID}, Facts{Outcome: &o, Window: windowAfter(s.Step, source, o)})
}

// outcome performs a, accepted at s under the window source of its request.
// An error is a journal read that failed or a done context, never a failed
// action, which is an outcome.
func (l *Loop) outcome(ctx context.Context, run Run, s core.State, a Action, source Window) (Outcome, error) {
	switch {
	case a.Refused != "":
		return Outcome{Status: OutcomeRefused, Detail: problemText(a.Refused)}, nil
	case a.Invalid != "":
		return Outcome{Status: OutcomeFailed, Detail: problemText(a.Invalid)}, nil
	}
	args, err := a.arguments(s.Decision.Payload)
	if err != nil {
		return failed(err), nil
	}
	if a.Name == ActionRetrieve {
		return l.retrieve(ctx, args)
	}
	h, err := proposedHandover(s.RunID, &source, args, l.cfg.HandoverBytes)
	if err != nil {
		return failed(err), nil
	}
	f := l.frame(run, s)
	f.Window = &Window{FirstStep: s.Step + 1, SourceStep: s.Step, Handover: h.Text}
	if _, err := f.workCapacity(l.cfg.Limits); err != nil {
		return failed(err), nil
	}
	return Outcome{Status: OutcomeDone, Handover: &h}, nil
}

// proposedHandover is the handover of text in window w, kept verbatim, for
// the run and window that the harness names.
func proposedHandover(runID string, w *Window, text string, limit int) (Handover, error) {
	h := Handover{RunID: runID, Text: text}
	if w == nil || !w.Requested {
		return h, errors.New("handover was not requested for this step")
	}
	h.FirstStep = w.FirstStep
	return h, h.Validate(runID, w.FirstStep, limit)
}

// retrieve decodes args, the arguments of a retrieval, once and performs
// it: the only interpretation of a recorded retrieval's arguments. A loop
// without a reader, as one built with another configuration after the
// acceptance would be, fails the read.
func (l *Loop) retrieve(ctx context.Context, args string) (Outcome, error) {
	if l.cfg.Reader == nil {
		return Outcome{Status: OutcomeFailed, Detail: retrievalDisabled}, nil
	}
	var q archive.RetrievalQuery
	if err := decodeArguments(args, &q); err != nil {
		return failed(err), nil
	}
	r, err := l.cfg.Reader.Retrieve(ctx, q)
	switch {
	case ctx.Err() != nil:
		return Outcome{}, ctx.Err()
	case err != nil:
		return failed(err), nil
	case r.Error != "":
		return Outcome{Status: OutcomeFailed, Retrieval: &r}, nil
	}
	return Outcome{Status: OutcomeDone, Retrieval: &r}, nil
}

// decodeArguments decodes exactly one JSON object of text into value,
// refusing unknown fields.
func decodeArguments(text string, value any) error {
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
