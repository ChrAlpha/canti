package harness

import (
	"context"
	"errors"
	"fmt"
	"time"

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
// overwrite. In the same transaction it must apply CheckEffect to every
// transition EffectTransition accepts, with the committed acceptance of its
// effect and the window of the request at its From's step and revision,
// failing with ErrFacts and recording nothing when it fails. A concrete store
// also owns crash safety and workspace ownership,
// and must provide recovery reads for the latest Snapshot, the frozen request
// of the current revision, and the saved response of the current attempt.
type Store interface {
	Commit(context.Context, Transition) error
}

// ErrNotFound reports a run or record the store does not hold.
var ErrNotFound = errors.New("harness: record not found")

// Journal is a Store with the recovery and history reads a loop needs. Every
// read is bounded independently of the run's history length.
//
// Request returns the frozen request of the named step and revision and
// Response the saved complete response of the named step and attempt, or
// ErrNotFound. Accepted returns the acceptance of the exec decision of the
// named step and attempt, the transition right after its saved response,
// with the semantics version it was committed under, or ErrNotFound when
// that response accepted none; through Acceptance it fails for an
// acceptance whose action contradicts its semantics version. Recent returns
// at most limit of the latest observed, performed, or resolved steps
// committed at a sequence below before, or the latest ones when before is
// not positive, oldest first; paging backward passes the Seq of the oldest
// record read. Totals are maintained in the same transaction as each
// commit.
type Journal interface {
	Store
	Run(ctx context.Context, runID string) (Run, error)
	Latest(ctx context.Context, runID string) (Snapshot, error)
	Request(ctx context.Context, runID string, step, rev int) (Request, error)
	LatestRequest(ctx context.Context, runID string) (Request, error)
	Response(ctx context.Context, runID string, step, attempt int) (Response, error)
	Accepted(ctx context.Context, runID string, step, attempt int) (Accepted, error)
	Recent(ctx context.Context, runID string, before int64, limit int) ([]StepRecord, error)
	// Completion returns the step that the transition at seq completed, by a
	// point read that needs no history scan: ErrNotFound when the run holds
	// no transition at seq, and ErrIncompatible when the transition there
	// completes no step or does not decode as one.
	Completion(ctx context.Context, runID string, seq int64) (StepRecord, error)
	Totals(ctx context.Context, runID string) (Totals, error)
	// SessionRuns returns at most limit runs of the session, newest first,
	// before session sequence before when positive.
	SessionRuns(ctx context.Context, sessionID string, before int64, limit int) ([]Run, error)
}

// Run is the immutable record a run is created with. Config is opaque host
// wiring, such as executor and model settings, that a resume must reuse.
type Run struct {
	ID          string
	SessionID   string `json:",omitempty"`
	SessionSeq  int64  `json:",omitempty"`
	InputID     string `json:",omitempty"`
	Goal        string
	Constraints string
	Budget      Budget
	Created     time.Time
	Config      []byte
}

// Budget holds the limits the harness measures itself. Zero means unlimited.
// JobTimeout bounds one job's run time from its backend start.
type Budget struct {
	WallTime    time.Duration
	Tokens      int64
	OutputBytes int64
	JobTimeout  time.Duration
}

// StepRecord is one completed step as the input builder sees it. Seq is the
// sequence of the observed, performed, or resolved transition that completed
// it. A performed step has an Outcome and the Window it committed, if any,
// and a zero Observation; any other step has an Observation.
type StepRecord struct {
	Seq         int64
	Step        int
	Command     string
	Purpose     string
	Observation Observation
	Outcome     *Outcome `json:",omitempty"`
	Window      *Window  `json:",omitempty"`
	Resolved    bool
}

// Totals are the run-wide sums the budgets are checked against: usage of
// every saved or incomplete response, output bytes of every observation, and
// the number of observed, performed, or resolved steps.
type Totals struct {
	Usage       Usage
	OutputBytes int64
	Steps       int
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
// identical retry can be recognized after a lost acknowledgement. Semantics
// is the version the transition is committed under: core.SemanticsVersion
// for every commit, and for a read one the version recorded with it.
type Transition struct {
	Seq       int64
	EventID   string
	From      core.State
	Event     core.Event
	To        core.State
	Facts     Facts
	Semantics int `json:"-"`
}

// Facts are the records attached to a transition. Identity comes from the
// surrounding states, so records carry only content and provenance. Each
// event permits a fixed subset; Runner.Commit rejects anything else. Action
// belongs to the ResponseLoaded that accepts an exec decision; Outcome and
// Window, the window the next step renders when it changes, to Performed.
type Facts struct {
	Request     *Request
	Response    *Response
	Observation *Observation
	Action      *Action            `json:",omitempty"`
	Execution   *ExecutionIdentity `json:",omitempty"`
	Receipt     *Job               `json:",omitempty"`
	Resolution  *Resolution        `json:",omitempty"`
	Outcome     *Outcome           `json:",omitempty"`
	Window      *Window            `json:",omitempty"`
	Failure     string
}
