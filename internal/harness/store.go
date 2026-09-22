package harness

import (
	"context"

	"github.com/ChrAlpha/canti/core"
)

// Store commits one transition and its attached facts atomically. Inside the
// same transaction Commit must verify that the durable state equals From and
// reject the transition otherwise, so a stale or competing harness cannot
// advance from a state that is no longer current. To may equal From: a legal
// event can leave the state unchanged and is still recorded, so enforce this
// as a compare-and-set on the current state, not as a uniqueness rule on
// From. Records are immutable: identical retries may succeed, but conflicting
// content for an existing identity must fail. A concrete store also owns
// crash safety and workspace ownership.
type Store interface {
	Commit(context.Context, Transition) error
}

// Transition is the durable unit of run progress: the committed state before
// and after one event, plus the facts the harness learned by acting.
type Transition struct {
	From  core.State
	Event core.Event
	To    core.State
	Facts Facts
}

// Facts are the records attached to a transition. Identity comes from the
// surrounding states, so records carry only content and provenance. Each
// event permits a fixed subset; Runner.Commit rejects anything else.
type Facts struct {
	Request     *Request
	Response    *Response
	Observation *Observation
	Failure     string
}
