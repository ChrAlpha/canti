package chat

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

const progressBytes = 64 << 10
const progressEntries = 128

// Event contains one typed runtime fact or an omission notice. Acceptance and
// completion have dedicated slots and cannot be evicted by progress volume.
type Event struct {
	Ack        *hostrpc.Ack
	Item       *hostrpc.Item
	Result     *Result
	Omitted    int
	Diagnostic string
}

type Turn struct {
	Snapshot       Snapshot
	reconcile      bool
	owned          bool
	cancel         context.CancelFunc
	stop           chan struct{}
	done           chan struct{}
	notify         chan struct{}
	diagnostics    []string
	mu             sync.Mutex
	lost           bool
	stopping       bool
	interrupts     int
	ack            *hostrpc.Ack
	runID          string
	items          []hostrpc.Item
	sizes          []int
	bytes, omitted int
	outcome        *Result
	consumed       bool
}

func newTurn(s Snapshot, owned bool, cancel context.CancelFunc) *Turn {
	return &Turn{Snapshot: s, owned: owned, cancel: cancel, stop: make(chan struct{}, 1), done: make(chan struct{}), notify: make(chan struct{}, 1)}
}
func (t *Turn) wake() {
	select {
	case t.notify <- struct{}{}:
	default:
	}
}
func (t *Turn) accept(a hostrpc.Ack) {
	t.mu.Lock()
	t.ack = &a
	if a.Run != nil && a.Run.RunID != "" {
		t.runID = a.Run.RunID
	}
	t.mu.Unlock()
	t.wake()
}
func (t *Turn) diagnostic(note string) {
	t.mu.Lock()
	if len(t.diagnostics) < 8 {
		t.diagnostics = append(t.diagnostics, note[:min(len(note), 4096)])
	}
	t.mu.Unlock()
	t.wake()
}
func (t *Turn) progress(item hostrpc.Item) {
	data, _ := json.Marshal(item)
	size := len(data)
	t.mu.Lock()
	defer t.mu.Unlock()
	if item.RunID != "" {
		// The log, not the submission, names the run the controls act on:
		// a follow-up start moves the turn to the next run.
		t.runID = item.RunID
	}
	if size > progressBytes {
		t.omitted++
		t.wake()
		return
	}
	for len(t.items) > 0 && (len(t.items) >= progressEntries || t.bytes+size > progressBytes) {
		t.bytes -= t.sizes[0]
		t.items[0] = hostrpc.Item{}
		t.items = t.items[1:]
		t.sizes = t.sizes[1:]
		t.omitted++
	}
	t.items = append(t.items, item)
	t.sizes = append(t.sizes, size)
	t.bytes += size
	t.wake()
}
func (t *Turn) finish(out Result) {
	t.mu.Lock()
	t.outcome = &out
	t.mu.Unlock()
	close(t.done)
	t.wake()
}
func (t *Turn) Done() <-chan struct{} { return t.done }
func (t *Turn) Outcome() Result       { <-t.done; t.mu.Lock(); defer t.mu.Unlock(); return *t.outcome }

// RunID is the run the turn's submission opened, once it is accepted.
func (t *Turn) RunID() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.runID
}

// Interrupt records stop intent even while acceptance is ambiguous. A second
// interrupt abandons the wait; the owner then reaps its runtime child.
func (t *Turn) Interrupt() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.interrupts++
	if t.interrupts == 1 {
		t.requestStopLocked()
	} else {
		t.cancel()
	}
}

func (t *Turn) requestStopLocked() {
	if !t.stopping {
		t.stopping = true
		select {
		case t.stop <- struct{}{}:
		default:
		}
	}
}
func (t *Turn) requestStop()        { t.mu.Lock(); defer t.mu.Unlock(); t.requestStopLocked() }
func (t *Turn) stopRequested() bool { t.mu.Lock(); defer t.mu.Unlock(); return t.stopping }
func (t *Turn) takeStop() bool {
	select {
	case <-t.stop:
		return true
	default:
		return false
	}
}

// InputLost detaches an auto client, but cancels a run whose runtime it owns.
func (t *Turn) InputLost() {
	t.mu.Lock()
	if t.lost {
		t.mu.Unlock()
		return
	}
	t.lost = true
	if t.owned {
		t.requestStopLocked()
	}
	t.mu.Unlock()
	if !t.owned {
		t.cancel()
	}
}

func (t *Turn) Next(ctx context.Context) (Event, error) {
	for {
		t.mu.Lock()
		e, ok := t.pop()
		done := t.consumed
		t.mu.Unlock()
		if ok {
			return e, nil
		}
		if done {
			return Event{}, io.EOF
		}
		select {
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case <-t.notify:
		}
	}
}
func (t *Turn) pop() (Event, bool) {
	if t.ack != nil {
		a := t.ack
		t.ack = nil
		return Event{Ack: a}, true
	}
	if len(t.diagnostics) > 0 {
		s := t.diagnostics[0]
		t.diagnostics = t.diagnostics[1:]
		return Event{Diagnostic: s}, true
	}
	if t.omitted > 0 {
		n := t.omitted
		t.omitted = 0
		return Event{Omitted: n}, true
	}
	if len(t.items) > 0 {
		it := t.items[0]
		t.items[0] = hostrpc.Item{}
		t.items = t.items[1:]
		t.bytes -= t.sizes[0]
		t.sizes = t.sizes[1:]
		return Event{Item: &it}, true
	}
	if t.outcome != nil && !t.consumed {
		t.consumed = true
		return Event{Result: t.outcome}, true
	}
	return Event{}, false
}
