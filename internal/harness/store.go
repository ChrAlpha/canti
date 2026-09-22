package harness

import (
	"context"

	"github.com/ChrAlpha/canti/core"
)

// Store is the durable fact boundary. A SQLite implementation can provide
// short transactions for each method; memory must not be required for recovery.
// Records must be immutable: identical retries may succeed, but conflicting
// content for an existing identity must fail before any effect is dispatched.
// These writes alone do not implement transactions across records, recovery,
// workspace ownership, or advancing run progress; the concrete store must.
type Store interface {
	SaveRequest(context.Context, RequestRecord) error
	SaveResponse(context.Context, ResponseRecord) error
	SaveDecision(context.Context, DecisionRecord) error
	SaveEffect(context.Context, EffectRecord) error
	SaveObservation(context.Context, ObservationRecord) error
	SaveReturn(context.Context, ReturnRecord) error
}

type RequestRecord struct {
	RunID     string
	StepID    string
	Request   Request
	Attempt   int
	RequestID string
}

type ResponseRecord struct {
	RequestID string
	Response  Response
}

type DecisionRecord struct {
	RunID    string
	StepID   string
	Decision core.Decision
}

type EffectRecord struct {
	RunID  string
	StepID string
	Effect Effect
}

type ObservationRecord struct {
	EffectID    string
	Observation Observation
}

type ReturnRecord struct {
	RunID    string
	StepID   string
	Decision core.Decision
}
