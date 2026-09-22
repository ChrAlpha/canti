package harness

import (
	"context"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/core"
)

// ErrStaleSnapshot reports a commit whose sequence is not the next one for
// the run and whose event ID does not match the record already committed at
// that sequence. Reloading the latest snapshot and re-deciding is the remedy.
var ErrStaleSnapshot = errors.New("harness: stale snapshot")

// ErrBrokenChain reports a commit whose From does not equal the state
// committed at the previous sequence. The caller's snapshot is corrupt, not
// merely behind; the commit must not be retried.
var ErrBrokenChain = errors.New("harness: transition does not extend the committed chain")

// Store commits one transition and its attached facts atomically. Inside the
// same transaction Commit must verify that Seq is exactly one past the last
// committed sequence for the run and reject the transition with
// ErrStaleSnapshot otherwise; the sequence, not the state value, is the
// compare-and-set, because a legal event may leave the state unchanged and is
// still recorded. It must also verify that From equals the state committed at
// the previous sequence and reject a transition that would break the chain
// with ErrBrokenChain — an integrity assertion the sequence alone cannot
// make. If a record already exists at Seq, a transition with the same event
// ID and content must succeed
// without recording again — this is how a caller resolves a commit whose
// acknowledgement was lost — while conflicting content must fail and never
// overwrite. A concrete store also owns crash safety and workspace ownership,
// and must provide recovery reads for the latest Snapshot, the frozen request
// of the current revision, and the saved response of the current attempt.
type Store interface {
	Commit(context.Context, Transition) error
}

// Snapshot is the committed position of a run: the state and the sequence of
// the last committed transition. NewState starts at sequence zero.
type Snapshot struct {
	Seq   int64
	State core.State
}

// Validate checks that the snapshot can act as a commit position.
func (s Snapshot) Validate() error {
	if s.Seq < 0 {
		return fmt.Errorf("harness: snapshot sequence %d is negative", s.Seq)
	}
	return s.State.Validate()
}

// Transition is the durable unit of run progress. Seq is the sequence this
// commit writes; EventID identifies the logical event occurrence, so an
// identical retry can be recognized after a lost acknowledgement.
type Transition struct {
	Seq     int64
	EventID string
	From    core.State
	Event   core.Event
	To      core.State
	Facts   Facts
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
