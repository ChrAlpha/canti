package harness

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
)

// ErrStaleSnapshot reports a commit whose sequence is not the next one for
// the run and whose event ID does not match the record already committed at
// that sequence. Reloading the latest snapshot and re-deciding is the remedy.
var ErrStaleSnapshot = errors.New("harness: stale snapshot")

// ErrBrokenChain reports a commit whose From does not equal the state
// committed at the previous sequence. The caller's snapshot is corrupt, not
// merely behind; the commit must not be retried.
var ErrBrokenChain = errors.New("harness: transition does not extend the committed chain")

// ErrInputsPending reports a ResponseLoaded whose run holds principal
// instructions admitted after the request was frozen. Binding them and
// loading the response again is the remedy; the decision is then superseded.
var ErrInputsPending = errors.New("harness: principal instructions are not bound")

// ErrHeld reports a principal input refused because a stopped run holds
// inputs that only withdraw releases. Its message is the remedy.
var ErrHeld = errors.New("withdraw the held inputs first")

// ErrDeliveryRequired reports a principal input admitted while the latest run
// is open, which must name steer or follow_up.
var ErrDeliveryRequired = errors.New("the agent is working: a principal input must name a delivery")

// ErrInputConflict reports a client input ID already admitted with other
// content.
var ErrInputConflict = errors.New("the input ID already names an input of other content")

// ErrNotWithdrawable reports an input a principal cannot take back.
var ErrNotWithdrawable = errors.New("only a queued follow-up or an unseen input of a stopped run can be withdrawn")

// ErrInputSpec reports an input that cannot be admitted as submitted.
var ErrInputSpec = errors.New("input does not fit")

// ErrAdmissionRace reports an input prepared for a run that the latest run's
// state no longer selects, because the run returned or stopped after the
// preparation. Preparing the input again is the remedy.
var ErrAdmissionRace = errors.New("the latest run changed before admission")

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
// effect and the context of the request at its From's step and revision,
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
	// Context reads the head and context pointer in one snapshot, rejecting a stale expected position.
	Context(ctx context.Context, expected Snapshot) (ContextState, error)
	Request(ctx context.Context, runID string, step, rev int) (Request, error)
	LatestRequest(ctx context.Context, runID string) (Request, error)
	Response(ctx context.Context, runID string, step, attempt int) (Response, error)
	Accepted(ctx context.Context, runID string, step, attempt int) (Accepted, error)
	Recent(ctx context.Context, runID string, before int64, limit int) ([]StepRecord, error)
	Receipts(ctx context.Context, runID string, before int64, limit int) ([]StepReceipt, error)
	// Completion returns the step that the transition at seq completed, by a
	// point read that needs no history scan: ErrNotFound when the run holds
	// no transition at seq, and ErrIncompatible when the transition there
	// completes no step or does not decode as one.
	Completion(ctx context.Context, runID string, seq int64) (StepRecord, error)
	CompletionAtStep(ctx context.Context, runID string, step int) (StepRecord, error)
	Totals(ctx context.Context, runID string) (Totals, error)
	// Steers returns the principal steer rows of runID in admission order,
	// each with the transition that bound it when the run had bound it.
	Steers(ctx context.Context, runID string) ([]Input, error)
	// InputAt returns the input at position pos.
	InputAt(ctx context.Context, pos int64) (Input, error)
	// InputsAt returns the inputs at the given positions.
	InputsAt(ctx context.Context, positions []int64) ([]Input, error)
	// Observations returns the latest observation of each source and subject,
	// most recently admitted first, at most MaxObservationKeys.
	Observations(ctx context.Context) ([]ObservationInput, error)
	// PreviousRuns returns the closed runs of the run's line before it, most
	// recent first, at most MaxPreviousRuns, and how many earlier runs the
	// page leaves out.
	PreviousRuns(ctx context.Context, runID string) ([]PreviousRun, int, error)
}

// Run is the immutable record a run is created with. Config is opaque host
// wiring, such as executor and model settings, that a resume must reuse.
// InputPos is the agent-log position of the principal input that opened it,
// zero for a run a test creates without one. Fresh starts a new line: the run
// links to no previous run and inherits no constraint.
type Run struct {
	ID          string
	InputID     string `json:",omitempty"`
	InputPos    int64  `json:",omitempty"`
	Fresh       bool   `json:",omitempty"`
	Goal        string
	Constraints string
	Budget      Budget
	Created     time.Time
	Config      []byte
}

// InputAuthority names who admitted an input.
type InputAuthority string

const (
	AuthorityPrincipal   InputAuthority = "principal"
	AuthorityObservation InputAuthority = "observation"
)

// InputDelivery names how a principal input reaches the agent while it works.
type InputDelivery string

const (
	DeliverySteer    InputDelivery = "steer"
	DeliveryFollowUp InputDelivery = "follow_up"
)

// InputDisposition is what became of a principal input. An observation has
// none: it is evidence, never a mandate.
type InputDisposition string

const (
	DispositionOpened    InputDisposition = "opened"
	DispositionSteer     InputDisposition = "steer"
	DispositionQueued    InputDisposition = "queued"
	DispositionWithdrawn InputDisposition = "withdrawn"
)

// The bounds of an admitted input and of what a request renders.
const (
	ObservationTextBytes = 16 << 10
	InputKeyBytes        = 256
	MaxObservationKeys   = 16
	MaxPreviousRuns      = 16
	PreviousRunsBytes    = 8 << 10
	PreviousGoalBytes    = 200
	ObservationPreview   = 512
)

// Input is one admitted agent-log fact: a principal instruction or an
// observation. Pos is its agent-log position. BoundSeq and BoundStep are
// derived from the run's InputsBound transitions and are zero while the input
// is not a bound steer.
type Input struct {
	Pos                 int64
	InputID             string `json:",omitempty"`
	Authority           InputAuthority
	Delivery            InputDelivery `json:",omitempty"`
	Disposition         InputDisposition
	RunID               string `json:",omitempty"`
	Text                string
	Constraints         string `json:",omitempty"`
	ExplicitConstraints bool   `json:",omitempty"`
	Fresh               bool   `json:",omitempty"`
	Spec                []byte `json:",omitempty"`
	Source              string `json:",omitempty"`
	Subject             string `json:",omitempty"`
	Admitted            time.Time
	// WithdrawnPos is the agent-log position of the withdrawal that took
	// this input back, zero while it stands. It is a fact of the withdrawal,
	// not of admission.
	WithdrawnPos int64 `json:",omitempty"`
	// DeliveredPos is the agent-log position of the first RequestFrozen that
	// rendered this input. Zero means the agent was never shown it and no
	// archive record may exist.
	DeliveredPos int64 `json:",omitempty"`
	// AdmittedRunID is the run this input opened or targeted at admission;
	// empty for an observation and a queued follow-up. It never follows a
	// later opening.
	AdmittedRunID string `json:",omitempty"`
	BoundSeq      int64  `json:"-"`
	BoundStep     int    `json:"-"`
}

// ObservationInput is the latest observation of one source and subject.
type ObservationInput struct {
	Source, Subject string
	InputID         string
	Pos             int64
	Text            string
}

// PreviousRun is one closed run of the current line, as a request names it.
type PreviousRun struct {
	Run    Run
	State  core.State
	Closed bool
}

// InputSubmission is one input a host asks a store to admit: a principal
// instruction, which may open a run or target the open one, or an
// observation, which never touches a run. Run and Initial are the run to
// create when the input opens one, with the goal, constraints, freshness,
// input identity, spec, and creation time that were resolved before admission.
type InputSubmission struct {
	InputID             string
	Authority           InputAuthority
	Delivery            InputDelivery
	Text                string
	Constraints         string
	ExplicitConstraints bool
	Fresh               bool
	Spec                []byte
	Source, Subject     string
	Run                 Run
	Initial             core.State
	Now                 time.Time
}

// Admission is what became of a submitted input. Run is set when the input
// opened a run, now or earlier. Replayed reports the answer of the input ID's
// first admission.
type Admission struct {
	Disposition InputDisposition
	Input       Input
	Run         *Run
	Replayed    bool
}

// InputCounts are the input counts a view reports: unseen steers of the run,
// queued follow-ups, and held inputs.
type InputCounts struct {
	Pending, Queued, Held int
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
// it. A performed step has an Outcome and a zero Observation; external steps have
// an Observation. Context mutations live on their authoritative Transition.
type StepRecord struct {
	Seq         int64
	Step        int
	Command     string
	Purpose     string
	Observation Observation
	Outcome     *Outcome `json:",omitempty"`
	Resolved    bool
	Superseded  *Superseded `json:",omitempty"`
}

// Superseded records that a decoded decision was not performed because a
// principal instruction arrived after the request that produced it was
// frozen. Instruction is the 1-based ordinal of that instruction in its run;
// Kind is the decision the model had chosen.
type Superseded struct {
	Instruction int
	Kind        core.Kind
	Purpose     string
}

// Totals are the run-wide sums the budgets are checked against: usage of
// every saved or incomplete response, output bytes of every observation, and
// the number of observed, performed, or resolved steps.
type Totals struct {
	Usage       Usage
	Coverage    UsageCoverage
	OutputBytes int64
	Steps       int
}

// UsageCoverage describes settled attempts, including unknown reports and lost
// sends. Reported totals are retrospective and cannot bound unreported charges.
type UsageCoverage struct {
	Attempts      int64 `json:"attempts"`
	InputReports  int64 `json:"input_reports"`
	OutputReports int64 `json:"output_reports"`
	Lost          int64 `json:"lost"`
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
// ContextState, the anchor/focus the next request renders when it changes, to Performed.
type Delivered struct {
	Positions []int64 `json:",omitempty"`
}

type Facts struct {
	Request      *Request
	Delivered    *Delivered `json:",omitempty"`
	Response     *Response
	Observation  *Observation
	Action       *Action            `json:",omitempty"`
	Execution    *ExecutionIdentity `json:",omitempty"`
	Receipt      *Job               `json:",omitempty"`
	Resolution   *Resolution        `json:",omitempty"`
	Outcome      *Outcome           `json:",omitempty"`
	ContextState *ContextState      `json:",omitempty"`
	Rejection    *ContextRejection  `json:",omitempty"`
	Supersede    *Superseded        `json:",omitempty"`
	Failure      string
}

// StepReceipt is the bounded historical projection needed by a request.
// Whole commands and result/evidence bodies stay in the archive and journal.
type StepReceipt struct {
	Seq          int64
	Step         int
	Purpose      string
	Status       string
	ExitCode     *int
	Resolved     bool
	SupersededBy int       `json:",omitempty"`
	DecisionKind core.Kind `json:",omitempty"`
}
