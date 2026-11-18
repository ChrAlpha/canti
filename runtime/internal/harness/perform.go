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
// with the context head a successful handover opens, and the step's progress
// as one Performed. Nothing is committed once ctx is done, so a resume
// performs it again: a read from its sources as they are then, a local change
// identically. Committed context that fails an integrity check stops the run
// instead.
func (l *Loop) perform(ctx context.Context, run Run, snap Snapshot, a Action) (Snapshot, error) {
	s := snap.State
	o, err := l.outcome(ctx, snap, a)
	if err != nil {
		return l.refuse(ctx, snap, err)
	}
	if ctx.Err() != nil {
		return snap, ctx.Err()
	}
	facts := Facts{Outcome: &o}
	if a.Name == ActionHandover && o.Status == OutcomeDone {
		current, err := l.journal.Context(ctx, snap)
		if err != nil {
			return snap, err
		}
		event := core.Performed{EffectID: s.EffectID}
		next, err := s.Next(event)
		if err != nil {
			return snap, err
		}
		facts.ContextState = &ContextState{
			AnchorSeq: snap.Seq + 1, FirstStep: next.Step,
			Rejection: current.Rejection,
		}
	}
	return l.runner.Commit(ctx, snap, core.Performed{EffectID: s.EffectID}, facts)
}

// outcome performs a, accepted at the step of snap. An error is a journal read that failed, committed context
// that fails a check, or a done context, never a failed action, which is an
// outcome.
func (l *Loop) outcome(ctx context.Context, snap Snapshot, a Action) (Outcome, error) {
	if a.Invalid != "" {
		return Outcome{Status: OutcomeFailed, Detail: problemText(a.Invalid)}, nil
	}
	args, err := a.arguments(snap.State.Decision.Payload)
	if err != nil {
		return failed(err), nil
	}
	if a.Name == ActionHandover {
		return l.handover(args)
	}
	return l.retrieve(ctx, args)
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
	if err := l.repairSnapshot(ctx, q); err != nil {
		if ctx.Err() != nil {
			return Outcome{}, ctx.Err()
		}
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
