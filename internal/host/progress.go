package host

import (
	"context"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

// View is the state of a run as the host reports it: the run, its stored
// config, the committed snapshot with its totals, whether the host drives
// it, and whether the host could drive it by itself, with why not.
type View struct {
	Run      harness.Run
	Config   RunConfig
	Snapshot harness.Snapshot
	Totals   harness.Totals
	Driving  bool
	// Drivable reports that the host drives the run or could continue it
	// without a client's credential: it finishes a pending stop, which needs
	// none, or holds a credential bound to the run's API and provider. Why
	// says what is missing otherwise.
	Drivable bool
	Why      string
}

// Cursor is the position of the view's snapshot: a reader that follows the
// run from it misses no transition and repeats none.
func (v View) Cursor() Cursor { return Cursor{v.Run.ID, v.Snapshot.Seq} }

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
		again, err := h.store.Latest(ctx, run.ID)
		if err != nil || again.Seq == v.Snapshot.Seq {
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
// host can continue by itself, as its View's Drivable reports: it finishes a
// pending stop, within ctx, and starts driving a run of its context version
// whose API and provider one of its credentials is bound to. It commits
// nothing to any other run, which waits for a client: a run of another
// context version, one that needs resolution, or one that needs a credential
// the host does not hold.
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
	case !v.Drivable || v.Driving:
		return nil
	case v.Snapshot.State.Phase == core.PhaseCancelling:
		_, err = h.Stop(ctx, last.ID)
	default:
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
	ItemStopped         ItemKind = "stopped"          // the run stopped
)

// Item is one entry of a run's progress, a host projection of committed
// transitions: what a reader shows of a step, never a journal record. A
// local step reports its Outcome; a step that ran, or was resolved, reports
// its execution.
type Item struct {
	Seq  int64
	Step int
	Kind ItemKind

	Command, Purpose string // ItemDecided: the decision's payload and purpose
	Action           string // ItemDecided: the recorded action
	Result           string // ItemReturned: the returned payload

	Local     bool                // ItemCompleted: the harness performed it or cancelled it in process
	Resolved  bool                // ItemCompleted: an operator recorded the outcome
	Execution core.ExecutionState // ItemCompleted of an observation
	ExitCode  *int
	Preview   string
	Detail    string
	Outcome   *Outcome // ItemCompleted of a local action the harness performed
	Window    int      // ItemCompleted: the first step of the window it opened, if any

	Reason  core.StopReason // ItemStopRequested, ItemStopped
	Failure string          // ItemRetrying, ItemNeedsResolution, ItemStopRequested

	// Truncated reports that a text of the item was longer than its cap and
	// is cut at a rune boundary; View has the full result of a return.
	Truncated bool
}

// Outcome is the result of a local action as progress reports it: its
// status and detail, and what a read found, without the evidence.
type Outcome struct {
	Status         harness.OutcomeStatus
	Detail         string
	Retrieval      bool
	RetrievalError string
	Sources        int
	Entries        int
	Handover       bool
}

// Cursor names a position in the journal of one run: after its transition
// at Seq, 0 before the first. It is not portable between runs: Progress and
// Wait take no other run ID.
type Cursor struct {
	RunID string
	Seq   int64
}

// ErrCursor reports a cursor that names no run of the state directory or a
// position past the end of its run.
var ErrCursor = errors.New("host: the cursor names no position of a run")

// cursor checks that c names a run of the state directory and a position of
// it, and returns the run's latest snapshot.
func (h *Host) cursor(ctx context.Context, c Cursor) (harness.Snapshot, error) {
	if c.RunID == "" || c.Seq < 0 {
		return harness.Snapshot{}, fmt.Errorf("%w: %+v", ErrCursor, c)
	}
	latest, err := h.store.Latest(ctx, c.RunID)
	switch {
	case errors.Is(err, harness.ErrNotFound):
		return latest, fmt.Errorf("%w: no run %q", ErrCursor, c.RunID)
	case err != nil:
		return latest, err
	case c.Seq > latest.Seq:
		return latest, fmt.Errorf("%w: %d is past the %d transitions of run %s", ErrCursor, c.Seq, latest.Seq, c.RunID)
	}
	return latest, nil
}

// Page is one page of a run's progress. Next is the cursor after the last
// row the page read, also when those rows showed no item; More reports that
// rows remain, and Halted that the run had halted when the page was read.
type Page struct {
	Items  []Item
	Next   Cursor
	More   bool
	Halted bool
}

// The bounds of one page: the rows it reads, and the bytes of the items it
// returns. Every text of an item is capped (sqlite.ProgressRowBytes), so
// every item fits a page.
const (
	PageRows  = 64
	PageBytes = 256 << 10
)

// Progress returns the page of the run of cursor after that follows it: at
// most limit rows, PageRows when limit is not positive, and PageBytes of
// items. A cursor of no run, or past the end of its run, is ErrCursor. It
// reads the journal only, so a page read again is the same page.
func (h *Host) Progress(ctx context.Context, after Cursor, limit int) (Page, error) {
	if limit <= 0 || limit > PageRows {
		limit = PageRows
	}
	latest, err := h.cursor(ctx, after)
	if err != nil {
		return Page{}, err
	}
	rows, err := h.store.Progress(ctx, after.RunID, after.Seq, limit)
	if err != nil {
		return Page{}, err
	}
	p := Page{Next: after, More: len(rows) == limit, Halted: latest.State.Intent() == core.IntentHalt}
	bytes := 0
	for _, r := range rows {
		item, ok := project(r)
		if ok {
			size := item.bytes()
			if bytes+size > PageBytes {
				p.More = true
				break
			}
			bytes += size
			p.Items = append(p.Items, item)
		}
		p.Next.Seq = r.Seq
	}
	return p, nil
}

// bytes is what the texts of it take.
func (it Item) bytes() int {
	n := len(it.Command) + len(it.Purpose) + len(it.Action) + len(it.Result) + len(it.Execution) + len(it.Preview) + len(it.Detail) + len(it.Reason) + len(it.Failure)
	if o := it.Outcome; o != nil {
		n += len(o.Status) + len(o.Detail) + len(o.RetrievalError)
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
	case "performed":
		it.Kind, it.Local, it.Window, it.Command = ItemCompleted, true, r.WindowFirstStep, r.Decision.Payload
		it.Outcome = &Outcome{Status: harness.OutcomeStatus(r.Outcome), Detail: r.OutcomeDetail, Retrieval: r.Retrieval,
			RetrievalError: r.RetrievalError, Sources: r.Sources, Entries: r.Entries, Handover: r.Handover}
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

// WaitResult is what a Wait observed: the cursor of the latest transition
// of the run, whether the run has halted, and whether the host drives it.
type WaitResult struct {
	Cursor  Cursor
	Halted  bool
	Driving bool
}

// Wait returns once the run of cursor after has a transition after it, has
// halted, or is not driven by the host, which then commits nothing to it by
// itself, or when the host shuts down or ctx is done; a cursor of no run, or
// past the end of its run, is ErrCursor. It registers for the host's next
// change before it rereads the journal, so a commit that lands between the
// two still wakes it: no wakeup is lost, and the journal, not the wakeup,
// decides.
func (h *Host) Wait(ctx context.Context, after Cursor) (WaitResult, error) {
	if _, err := h.cursor(ctx, after); err != nil {
		return WaitResult{}, err
	}
	for {
		h.mu.Lock()
		changed, driving, closing := h.changed, h.drivers[after.RunID] != nil, h.closing
		h.mu.Unlock()
		h.checkpoint(PointWaiting)
		snap, err := h.store.Latest(ctx, after.RunID)
		if err != nil {
			return WaitResult{}, err
		}
		r := WaitResult{Cursor: Cursor{after.RunID, snap.Seq}, Halted: snap.State.Intent() == core.IntentHalt, Driving: driving}
		if r.Cursor.Seq > after.Seq || r.Halted || !driving || closing {
			return r, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return r, ctx.Err()
		}
	}
}
