package host

import (
	"context"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

// View is the state of a run as the host reports it: the run, its stored
// config, the committed snapshot with its totals, whether a drive or stop owns
// its work, whether the host could drive it by itself, and the input counts
// of the agent log.
type View struct {
	Run           harness.Run
	Config        RunConfig
	Snapshot      harness.Snapshot
	Pos           int64
	Ordinal       int
	Totals        harness.Totals
	Driving       bool
	PendingSteers int
	Queued        int
	Held          int
	// Drivable reports that the host drives the run or could continue it
	// without a client's credential: it finishes a pending stop, which needs
	// none, or holds a credential bound to the run's API and provider. Why
	// says what is missing otherwise.
	Drivable bool
	Why      string
}

// Cursor is the agent-log position of the view's run: following the agent
// from it misses no later fact and repeats none.
func (v View) Cursor() Cursor { return Cursor(v.Pos) }

// View returns the view of run id, or of the latest run when id is empty.
// The snapshot and totals are read until two reads of the snapshot agree, so
// they belong together.
func (h *Host) View(ctx context.Context, id string) (View, error) {
	run, c, err := h.Find(ctx, id)
	if err != nil {
		return View{}, err
	}
	v := View{Run: run, Config: c, Driving: h.Driving(run.ID)}
	for range 5 {
		if v.Snapshot, err = h.store.Latest(ctx, run.ID); err != nil {
			return v, err
		}
		if v.Totals, err = h.store.Totals(ctx, run.ID); err != nil {
			return v, err
		}
		again, err := h.store.Head(ctx, run.ID)
		if err != nil || again.Seq == v.Snapshot.Seq {
			counts, err := h.store.InputCounts(ctx, run.ID)
			if err != nil {
				return v, err
			}
			v.PendingSteers, v.Queued, v.Held = counts.Pending, counts.Queued, counts.Held
			if v.Pos, err = h.store.RunPos(ctx, run.ID); err != nil {
				return v, err
			}
			if v.Ordinal, err = h.store.RunOrdinal(ctx, run.ID); err != nil {
				return v, err
			}
			v.Drivable, v.Why = h.drivability(v)
			return v, err
		}
	}
	return v, fmt.Errorf("run %s changed during every attempt to read its state", run.ID)
}

// drivability reports whether the host could continue the run of v by
// itself, and why not.
func (h *Host) drivability(v View) (bool, string) {
	s, c := v.Snapshot.State, v.Config
	switch {
	case v.Driving:
		return true, ""
	case s.Phase == core.PhaseNeedsResolution:
		return false, "the outcome of its effect is unknown: resolve it"
	case s.Intent() == core.IntentHalt:
		return false, "the run has halted"
	case c.ContextVersion != ContextVersion:
		return false, "the run " + Unresumable(c)
	case s.Phase == core.PhaseCancelling:
		return true, ""
	}
	p := c.Provider
	if h.opts.Credentials[Pair{c.API, p}].For(c.API) != (Keys{}) {
		return true, ""
	}
	return false, fmt.Sprintf("a client must give a credential for %s", Pair{c.API, p})
}

// ResumeOpen continues the open run of the state directory, if any, that the
// host can continue by itself, or opens the oldest queued follow-up after a
// returned latest run. It finishes a pending stop, within ctx, and starts
// driving a run of its context version whose API and provider one of its
// credentials is bound to. It commits nothing to any other run, which waits
// for a client: a run of another context version, one that needs resolution,
// or one that needs a credential the host does not hold. Nothing starts after
// a stop.
func (h *Host) ResumeOpen(ctx context.Context) error {
	ctx, leave, err := h.Enter(ctx)
	if err != nil {
		return err
	}
	defer leave()
	last, err := h.store.LastRun(ctx)
	if errors.Is(err, harness.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	v, err := h.View(ctx, last.ID)
	switch {
	case err != nil:
		return err
	case v.Driving:
		return nil
	case v.Snapshot.State.Phase == core.PhaseReturned:
		return h.startFollowUp(ctx, v.Snapshot)
	case v.Snapshot.State.Closed():
		return nil
	case v.Snapshot.State.Phase == core.PhaseCancelling:
		_, err = h.Stop(ctx, last.ID)
	default:
		if !v.Drivable {
			return nil
		}
		_, err = h.Drive(ctx, last.ID, Binding{}, DriveOptions{})
	}
	return err
}

// ItemKind names what one progress item reports.
type ItemKind string

const (
	ItemDecided         ItemKind = "decided"        // an exec decision was accepted
	ItemReturned        ItemKind = "returned"       // a return decision closed the run
	ItemRetrying        ItemKind = "retrying"       // a response was incomplete or held no valid decision
	ItemCompleted       ItemKind = "completed"      // a step completed, by the executor, the harness, or a resolution
	ItemStopRequested   ItemKind = "stop_requested" // a persistent stop was recorded
	ItemRecovered       ItemKind = "effect_recovered"
	ItemNeedsResolution ItemKind = "needs_resolution" // an effect's outcome is unknown
	ItemContextReset    ItemKind = "context_reset"
	ItemStopped         ItemKind = "stopped"      // the run stopped
	ItemInput           ItemKind = "input"        // an admitted principal instruction or observation
	ItemWithdrawn       ItemKind = "withdrawn"    // a withdrawal of held principal inputs
	ItemInputsBound     ItemKind = "inputs_bound" // principal instructions were bound to a run
	ItemRunStarted      ItemKind = "run_started"  // a run's first committed transition
)

// InputItem is one admitted input as progress reports it, with bounded text.
type InputItem struct {
	InputID     string
	Authority   harness.InputAuthority
	Delivery    harness.InputDelivery
	Disposition harness.InputDisposition
	Source      string
	Subject     string
	Text        string
	Truncated   bool
}

// Item is one entry of a run's progress, a host projection of the agent log:
// what a reader shows of a step or an admitted input, never a journal record.
// Pos is its agent-log position. A local step reports its Outcome; a step
// that ran, or was resolved, reports its execution.
type Item struct {
	Pos   int64
	RunID string
	Seq   int64
	Step  int
	Kind  ItemKind

	Command, Purpose string // ItemDecided: the decision's payload and purpose
	Action           string // ItemDecided: the recorded action
	Result           string // ItemReturned: the returned payload

	Local        bool                // ItemCompleted: the harness performed it or cancelled it in process
	Resolved     bool                // ItemCompleted: an operator recorded the outcome
	Execution    core.ExecutionState // ItemCompleted of an observation
	ExitCode     *int
	Preview      string
	Detail       string
	Outcome      *Outcome // ItemCompleted of a local action the harness performed
	AnchorSeq    int64
	SupersededBy int // ItemCompleted: the instruction that superseded the decision
	Inputs       int // ItemInputsBound: the bound instruction count

	Input     *InputItem // ItemInput
	Withdrawn []string   // ItemWithdrawn: the inputs it took back, by ID or position, bounded

	Reason  core.StopReason // ItemStopRequested, ItemStopped
	Failure string          // ItemRetrying, ItemNeedsResolution, ItemStopRequested

	// Truncated reports that a text of the item was longer than its cap and
	// is cut at a rune boundary; View has the full result of a return.
	Truncated bool
}

// Outcome is the result of a local action as progress reports it: its
// status and detail, what a read found without the evidence, and the byte
// count derived from a successful handover's committed payload. A done local
// outcome with no detail and no retrieval is the handover shape.
type Outcome struct {
	Status         harness.OutcomeStatus
	Detail         string
	Retrieval      bool
	RetrievalError string
	Sources        int
	Entries        int
	HandoverBytes  int
}

// Cursor is an opaque agent-log position. It names no run: the log spans
// every run of one state directory.
type Cursor int64

// ErrCursor reports a cursor that is not a position of the agent log.
var ErrCursor = errors.New("host: the cursor names no position of the agent log")

// cursor checks that c is a position of the agent log, and returns the
// latest one.
func (h *Host) cursor(ctx context.Context, c Cursor) (Cursor, error) {
	if c < 0 {
		return 0, fmt.Errorf("%w: %d is negative", ErrCursor, c)
	}
	last, err := h.logEnd(ctx)
	if err != nil {
		return 0, err
	}
	if int64(c) > last {
		return Cursor(last), fmt.Errorf("%w: %d is past the end %d of the agent log", ErrCursor, c, last)
	}
	return Cursor(last), nil
}

func (h *Host) logEnd(ctx context.Context) (int64, error) {
	last, err := h.store.LastPos(ctx)
	return last, err
}

// Page is one page of the agent log. Next is the cursor after the last row
// the page read, also when those rows showed no item; More reports that rows
// remain, and Quiescent that the latest run was closed when the page was read.
type Page struct {
	Items     []Item
	Next      Cursor
	More      bool
	Quiescent bool
}

// The bounds of one page: the rows it reads, and the bytes of the items it
// returns. Every text of an item is capped, so every item fits a page.
const (
	PageRows  = 64
	PageBytes = 256 << 10
)

// Progress returns the page of the agent log after cursor: at most limit
// rows, PageRows when limit is not positive, and PageBytes of items. A
// cursor of no position of the log is ErrCursor. It reads the journal only,
// so a page read again is the same page.
func (h *Host) Progress(ctx context.Context, after Cursor, limit int) (Page, error) {
	if limit <= 0 || limit > PageRows {
		limit = PageRows
	}
	if _, err := h.cursor(ctx, after); err != nil {
		return Page{}, err
	}
	rows, err := h.store.Log(ctx, int64(after), limit)
	if err != nil {
		return Page{}, err
	}
	working := h.ownsWork()
	p := Page{Next: after, More: len(rows) == limit, Quiescent: h.quiescent(ctx) && !working}
	bytes := 0
	for _, r := range rows {
		if r.Transition != nil {
			if r.Transition.Seq == 1 && r.Transition.Kind == "request_frozen" {
				item := Item{Pos: r.Pos, RunID: r.RunID, Seq: 1, Step: r.Transition.Step, Kind: ItemRunStarted}
				p.Items = append(p.Items, item)
				bytes += item.bytes()
				p.Next = Cursor(r.Pos)
				continue
			}
			item, ok := project(*r.Transition)
			if !ok {
				p.Next = Cursor(r.Pos)
				continue
			}
			item.Pos = r.Pos
			item.RunID = r.RunID
			size := item.bytes()
			if bytes+size > PageBytes {
				p.More = true
				return p, nil
			}
			bytes += size
			p.Items = append(p.Items, item)
		} else if r.Input != nil {
			item := Item{Pos: r.Pos, RunID: r.Input.RunID, Kind: ItemInput, Input: &InputItem{
				InputID: r.Input.InputID, Authority: r.Input.Authority, Delivery: r.Input.Delivery,
				Disposition: r.Input.Disposition, Source: r.Input.Source, Subject: r.Input.Subject,
				Text: r.Input.Text, Truncated: r.InputCut,
			}}
			size := item.bytes()
			if bytes+size > PageBytes {
				p.More = true
				return p, nil
			}
			bytes += size
			p.Items = append(p.Items, item)
		} else if r.Withdrawn != nil {
			item := Item{Pos: r.Pos, Kind: ItemWithdrawn, Withdrawn: r.Withdrawn, Truncated: r.WithdrawnCut}
			size := item.bytes()
			if bytes+size > PageBytes {
				p.More = true
				return p, nil
			}
			bytes += size
			p.Items = append(p.Items, item)
		}
		p.Next = Cursor(r.Pos)
	}
	return p, nil
}

// bytes is what the texts of it take.
func (it Item) bytes() int {
	n := len(it.Command) + len(it.Purpose) + len(it.Action) + len(it.Result) + len(it.Execution) + len(it.Preview) + len(it.Detail) + len(it.Reason) + len(it.Failure)
	for _, name := range it.Withdrawn {
		n += len(name)
	}
	if o := it.Outcome; o != nil {
		n += len(o.Status) + len(o.Detail) + len(o.RetrievalError)
	}
	if in := it.Input; in != nil {
		n += len(in.InputID) + len(in.Text) + len(in.Source) + len(in.Subject)
	}
	return n
}

// project maps bounded journal facts into progress. Store validation ensures
// that a receipt-free Observed is cancellation of an accepted local action.
func project(r sqlite.ProgressRow) (Item, bool) {
	it := Item{Seq: r.Seq, Step: r.Step}
	// shown are the texts of r the item shows, which alone make it Truncated.
	shown := sqlite.ProgressName
	switch r.Kind {
	case "response_loaded":
		switch {
		case r.SupersedeBy > 0:
			it.Kind, it.SupersededBy, it.Detail = ItemCompleted, r.SupersedeBy, "superseded by instruction #"+fmt.Sprint(r.SupersedeBy)
			it.Execution = core.ExecutionSuperseded
		case r.Phase == core.PhaseEffectSaved:
			it.Kind, it.Command, it.Purpose, it.Action = ItemDecided, r.Decision.Payload, r.Decision.Purpose, r.Action
			shown |= sqlite.ProgressPayload | sqlite.ProgressPurpose

		case r.Phase == core.PhaseReturned:
			it.Kind, it.Result = ItemReturned, r.Decision.Payload
			shown |= sqlite.ProgressPayload
		default:
			it.Kind, it.Failure = ItemRetrying, "the response held no valid decision"
		}
	case "response_incomplete":
		it.Kind, it.Failure = ItemRetrying, r.Failure
		shown |= sqlite.ProgressFailure
	case "request_rejected":
		it.Kind, it.Failure, it.AnchorSeq = ItemRetrying, r.Failure, r.AnchorSeq
		if r.AnchorSeq > 0 {
			it.Kind = ItemContextReset
		}
		shown |= sqlite.ProgressFailure
	case "inputs_bound":
		it.Kind, it.Inputs = ItemInputsBound, r.Inputs
	case "performed":
		it.Kind, it.Local, it.AnchorSeq, it.Command = ItemCompleted, true, r.AnchorSeq, r.Decision.Payload
		it.Outcome = &Outcome{Status: harness.OutcomeStatus(r.Outcome), Detail: r.OutcomeDetail, Retrieval: r.Retrieval,
			RetrievalError: r.RetrievalError, Sources: r.Sources, Entries: r.Entries, HandoverBytes: r.HandoverBytes}
		shown |= sqlite.ProgressPayload | sqlite.ProgressOutcomeDetail | sqlite.ProgressRetrievalError
	case "observed", "resolved":
		it.Kind, it.Resolved, it.Command = ItemCompleted, r.ResolutionSource == harness.ResolutionOperator, r.Decision.Payload
		it.Local = r.Kind == "observed" && !r.External && r.Execution == core.ExecutionCancelled
		it.Execution, it.ExitCode, it.Preview, it.Detail = r.Execution, r.ExitCode, r.Preview, r.Detail
		shown |= sqlite.ProgressPayload | sqlite.ProgressPreview | sqlite.ProgressDetail
	case "effect_recovered":
		it.Kind = ItemRecovered
	case "outcome_unknown":
		it.Kind, it.Failure = ItemNeedsResolution, r.Failure
		shown |= sqlite.ProgressFailure
	case "stop_requested":
		it.Kind, it.Reason, it.Failure = ItemStopRequested, r.Reason, r.Failure
		shown |= sqlite.ProgressFailure
		if r.Phase == core.PhaseStopped {
			it.Kind = ItemStopped
		}
	default:
		return it, false
	}
	it.Truncated = r.Cut&shown != 0
	return it, true
}

// WaitResult is what a Wait observed: the latest agent-log position, whether
// the agent was quiescent, and whether a drive or operation owned work.
type WaitResult struct {
	Cursor    Cursor
	Quiescent bool
	Driving   bool
}

// Wait returns once the agent log has a row after cursor, the agent is
// quiescent and no drive or operation owns work, or the host shuts down.
// A cursor of no position of the log is ErrCursor. It registers for the
// host's next change before it rereads the journal, so a commit that lands
// between the two still wakes it: no wakeup is lost, and the journal, not the
// wakeup, decides.
func (h *Host) Wait(ctx context.Context, after Cursor) (WaitResult, error) {
	if _, err := h.cursor(ctx, after); err != nil {
		return WaitResult{}, err
	}
	for {
		h.mu.Lock()
		changed, closing := h.changed, h.closing
		h.mu.Unlock()
		working := h.ownsWork()
		h.checkpoint(PointWaiting)
		last, err := h.logEnd(ctx)
		if err != nil {
			return WaitResult{}, err
		}
		quiescent := h.quiescent(ctx)
		r := WaitResult{Cursor: Cursor(last), Quiescent: quiescent, Driving: working}
		if Cursor(last) > after || !working || closing {
			return r, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return r, ctx.Err()
		}
	}
}

// quiescent reports whether the latest run of the state directory is closed,
// or there is none.
func (h *Host) quiescent(ctx context.Context) bool {
	last, err := h.store.LastRun(ctx)
	if errors.Is(err, harness.ErrNotFound) {
		return true
	}
	if err != nil {
		return false
	}
	snap, err := h.store.Latest(ctx, last.ID)
	return err == nil && snap.State.Closed()
}

// ownsWork reports whether a drive, a stop, a resolution, or an admission
// owns work that may still commit.
func (h *Host) ownsWork() bool {
	h.mu.Lock()
	working := len(h.drivers) > 0 || len(h.runs) > 0
	h.mu.Unlock()
	return working || len(h.admission) > 0
}
