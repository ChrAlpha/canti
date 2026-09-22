package harness

import (
	"context"

	"github.com/ChrAlpha/canti/core"
)

// Store commits one transition and its attached facts atomically. Records are
// immutable: identical retries may succeed, but conflicting content for an
// existing identity must fail. A concrete store also owns crash safety and
// workspace ownership; this interface does not provide them.
type Store interface {
	Commit(context.Context, Transition) error
}

// Transition is the durable unit of run progress: the committed state before
// and after one event, plus the facts the harness learned by acting.
type Transition struct {
	From  core.State
	Event core.Event
	To    core.State
	Facts
}

// Facts are the records attached to a transition. Identity comes from the
// surrounding states, so records carry only content and provenance.
type Facts struct {
	Request     *Request
	Response    *Response
	Observation *Observation
	Failure     string
}
