package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/core"
)

// Runner owns the explicit decision commit boundary. Model requests and
// observation collection are separate steps so they can be recovered by phase.
type Runner struct {
	store    Store
	executor Executor
}

// NewRunner wires the durable fact store and external effect backend.
func NewRunner(store Store, executor Executor) (Runner, error) {
	if store == nil {
		return Runner{}, errors.New("harness: store is required")
	}
	if executor == nil {
		return Runner{}, errors.New("harness: executor is required")
	}
	return Runner{store: store, executor: executor}, nil
}

type OutcomeKind string

const (
	OutcomeReturned   OutcomeKind = "returned"
	OutcomeDispatched OutcomeKind = "dispatched"
)

// Outcome reports what ApplyDecision committed and whether an effect was
// handed to the executor. Return is a control-flow outcome, not task success.
type Outcome struct {
	Kind     OutcomeKind
	Decision core.Decision
	Effect   *Effect
	Job      *Job
}

// ApplyDecision decodes one complete model response, persists the decision,
// then either returns control or persists and dispatches one stable effect.
// The caller must have persisted the response before calling ApplyDecision.
func (r Runner) ApplyDecision(ctx context.Context, runID, stepID string, response Response) (Outcome, error) {
	if err := ctx.Err(); err != nil {
		return Outcome{}, err
	}
	if !response.Complete {
		return Outcome{}, errors.New("harness: complete model response is required")
	}
	decision, err := core.Decode(response.Body)
	if err != nil {
		return Outcome{}, err
	}
	if runID == "" || stepID == "" {
		return Outcome{}, errors.New("harness: run and step identifiers are required")
	}
	if err := r.store.SaveDecision(ctx, DecisionRecord{RunID: runID, StepID: stepID, Decision: decision}); err != nil {
		return Outcome{}, fmt.Errorf("harness: save decision: %w", err)
	}

	if decision.Kind == core.Return {
		if err := r.store.SaveReturn(ctx, ReturnRecord{RunID: runID, StepID: stepID, Decision: decision}); err != nil {
			return Outcome{}, fmt.Errorf("harness: save return: %w", err)
		}
		return Outcome{Kind: OutcomeReturned, Decision: decision}, nil
	}

	effect := Effect{ID: stableEffectID(runID, stepID), Command: decision.Payload}
	if err := r.store.SaveEffect(ctx, EffectRecord{RunID: runID, StepID: stepID, Effect: effect}); err != nil {
		return Outcome{}, fmt.Errorf("harness: save effect: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Outcome{}, err
	}
	job, err := r.executor.StartOrResume(ctx, effect)
	if err != nil {
		return Outcome{}, fmt.Errorf("harness: start or resume effect: %w", err)
	}
	return Outcome{Kind: OutcomeDispatched, Decision: decision, Effect: &effect, Job: &job}, nil
}

func stableEffectID(runID, stepID string) string {
	// A length prefix keeps arbitrary identifiers unambiguous before hashing.
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s%s", len(runID), runID, stepID)))
	return "effect_" + hex.EncodeToString(sum[:])
}
