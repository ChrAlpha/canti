package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

// effect routes exclusively by the committed recovery contract.
func (l *Loop) effect(ctx context.Context, run Run, snap Snapshot) (Snapshot, error) {
	a, err := ActionOf(ctx, l.journal, snap.State)
	if err != nil {
		return snap, err
	}
	if a.Local() {
		if snap.State.Intent() == core.IntentEffect {
			return l.perform(ctx, run, snap, a.Action)
		}
		obs := Observation{Execution: core.ExecutionCancelled, Preview: "Context action cancelled."}
		return l.runner.Commit(ctx, snap, core.Observed{EffectID: snap.State.EffectID, Execution: core.ExecutionCancelled}, Facts{Observation: &obs})
	}
	effect, err := l.descriptor(run, snap.State, a)
	if err != nil {
		return snap, err
	}
	return l.watch(ctx, run, snap, effect)
}

func (l *Loop) descriptor(run Run, s core.State, a Accepted) (Effect, error) {
	if a.Execution == nil || a.Execution.ConfigDigest != ConfigDigest(run.Config) {
		return Effect{}, fmt.Errorf("%w: accepted execution configuration differs", ErrIncompatible)
	}
	e := Effect{ID: s.EffectID, Command: s.Decision.Payload, Spec: a.Execution.Spec}
	if l.executor.Specification(e.Command) != e.Spec {
		return Effect{}, fmt.Errorf("%w: executor specification differs from acceptance", ErrIncompatible)
	}
	return e, nil
}

// perform carries out the local action a of snap and commits its outcome,
// with the window it changes, and the step's progress as one Performed.
// Nothing is committed once ctx is done, so a resume performs it again: a
// read from its sources as they are then, a local change identically.
// Committed context that fails an integrity check stops the run instead.
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
	o, err := l.outcome(ctx, run, snap, a, source)
	if err != nil {
		return l.refuse(ctx, snap, err)
	}
	if ctx.Err() != nil {
		return snap, ctx.Err()
	}
	return l.runner.Commit(ctx, snap, core.Performed{EffectID: s.EffectID}, Facts{Outcome: &o, Window: windowAfter(s.Step, source, o)})
}

// outcome performs a, accepted at the step of snap under the window source
// of its request. An error is a journal read that failed, committed context
// that fails a check, or a done context, never a failed action, which is an
// outcome.
func (l *Loop) outcome(ctx context.Context, run Run, snap Snapshot, a Action, source Window) (Outcome, error) {
	s := snap.State
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
	p := carryProposal{Text: args}
	if a.Name == ActionHandoverCarry {
		p = carryProposal{}
		if err := decodeArguments(args, &p); err != nil {
			return failed(err), nil
		}
	}
	return l.handover(ctx, run, snap, source, p, a.Name == ActionHandoverCarry)
}

// handover performs the handover p proposes in window source: one that
// carries the evidence its entries name when carry is set, which the name of
// the accepted action decides, and otherwise a plain one of its text. It
// admits the handover only
// when the window it opens, rendered as its first request renders it with
// the run's earlier session dialogue and the carried evidence, keeps work
// capacity above zero, and above carryMargin when it carries evidence.
func (l *Loop) handover(ctx context.Context, run Run, snap Snapshot, source Window, p carryProposal, carry bool) (Outcome, error) {
	s := snap.State
	h, err := proposedHandover(s.RunID, &source, p.Text, l.cfg.HandoverBytes)
	if err == nil && carry {
		err = p.check()
	}
	if err != nil {
		return failed(err), nil
	}
	f := l.frame(run, s)
	f.Window = &Window{FirstStep: s.Step + 1, SourceStep: s.Step, Handover: h.Text}
	if err := l.sessionContext(ctx, run, &f); err != nil {
		return Outcome{}, err
	}
	open := f.capacity(l.cfg.Limits)
	if open <= 0 {
		_, err := f.workCapacity(l.cfg.Limits)
		return failed(err), nil
	}
	if !carry {
		return Outcome{Status: OutcomeDone, Handover: &h}, nil
	}
	carried, err := l.selectCarry(ctx, snap, source, p.Carry)
	if errors.As(err, new(carryProblem)) {
		return failed(err), nil
	}
	if err != nil {
		return Outcome{}, err
	}
	f.Carried = carried
	for _, c := range carried {
		f.Window.Carry = append(f.Window.Carry, c.Ref)
	}
	margin := l.carryMargin(f)
	if left := f.capacity(l.cfg.Limits); left <= margin {
		return failed(fmt.Errorf("carried evidence of %d bytes leaves the next window %d bytes of work capacity, %d without it; a carry needs more than %d. Carry fewer ranges, or hand over with %s",
			open-left, left, open, margin, handoverName)), nil
	}
	h.Carry = f.Window.Carry
	return Outcome{Status: OutcomeDone, Handover: &h}, nil
}

// carryMargin is the work capacity, beyond the step that the reserve always
// fits, that a carry must leave the window f opens.
func (l *Loop) carryMargin(f Frame) int {
	if l.cfg.carryMarginStep {
		return f.reserve(l.cfg.Limits).Step
	}
	return 0
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
