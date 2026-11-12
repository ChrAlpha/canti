package host

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

type qaWaitState struct {
	Result WaitResult
	Err    error
}

// qaPendingWait starts a Wait after cursor and fails unless it is still
// blocked once the action has had time to register.
func qaPendingWait(t *testing.T, h *Host, after Cursor) <-chan qaWaitState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	out := make(chan qaWaitState, 1)
	go func() {
		result, err := h.Wait(ctx, after)
		out <- qaWaitState{Result: result, Err: err}
	}()
	select {
	case got := <-out:
		t.Fatalf("Wait after %d returned before the action: %+v, %v", after, got.Result, got.Err)
	case <-time.After(100 * time.Millisecond):
	}
	return out
}

func qaAwaitWait(t *testing.T, out <-chan qaWaitState) qaWaitState {
	t.Helper()
	select {
	case got := <-out:
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("a pending Wait did not wake for the agent-log write")
		return qaWaitState{}
	}
}

func qaDrain(t *testing.T, h *Host) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = h.Shutdown(ctx)
}

func TestWaitWakesForAnAdmittedInput(t *testing.T) {
	for _, tc := range []struct {
		name  string
		admit func(*testing.T, *Host, string)
	}{
		{"observation", func(t *testing.T, h *Host, _ string) {
			if _, err := h.Observe(context.Background(), Observation{InputID: "obs", Source: "ci", Subject: "wait", Text: "green"}); err != nil {
				t.Fatal(err)
			}
		}},
		{"steer", func(t *testing.T, h *Host, url string) {
			if _, err := h.Submit(context.Background(), Submission{InputID: "steer", Goal: "steer", Delivery: harness.DeliverySteer, Spec: testSpec(h), Binding: binding(url)}); err != nil {
				t.Fatal(err)
			}
		}},
		{"follow_up", func(t *testing.T, h *Host, url string) {
			if _, err := h.Submit(context.Background(), Submission{InputID: "next", Goal: "next", Delivery: harness.DeliveryFollowUp, Spec: testSpec(h), Binding: binding(url)}); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := testHost(t, Options{})
			url, requested := holding(t)
			t.Cleanup(func() { qaDrain(t, h) })
			first := submit(t, h, Submission{InputID: "first", Binding: binding(url)})
			if _, err := h.Drive(context.Background(), first.Run.ID, binding(url), DriveOptions{}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-requested:
			case <-time.After(3 * time.Second):
				t.Fatal("the held model request never arrived")
			}
			last, err := h.logEnd(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			out := qaPendingWait(t, h, Cursor(last))
			tc.admit(t, h, url)
			got := qaAwaitWait(t, out)
			if got.Err != nil {
				t.Fatal(got.Err)
			}
			if got.Result.Cursor <= Cursor(last) {
				t.Fatalf("Wait returned cursor %d after the new input at %d", got.Result.Cursor, last)
			}
		})
	}
}

func TestWaitWakesForAWithdrawal(t *testing.T) {
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h := testHost(t, Options{Checkpoint: func(point string) {
		if point == PointWithdrawing {
			once.Do(func() { close(reached); <-release })
		}
	}})
	url, requested := holding(t)
	t.Cleanup(func() { qaDrain(t, h) })
	first := submit(t, h, Submission{InputID: "first", Binding: binding(url)})
	if _, err := h.Drive(context.Background(), first.Run.ID, binding(url), DriveOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requested:
	case <-time.After(3 * time.Second):
		t.Fatal("the held model request never arrived")
	}
	submit(t, h, Submission{InputID: "held", Goal: "later", Delivery: harness.DeliveryFollowUp, Binding: binding(url)})
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if _, err := h.Stop(stopCtx, first.Run.ID); err != nil {
		t.Fatal(err)
	}
	last, err := h.logEnd(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	withdrawn := make(chan struct {
		inputs []harness.Input
		err    error
	}, 1)
	go func() {
		inputs, err := h.Withdraw(context.Background(), "")
		withdrawn <- struct {
			inputs []harness.Input
			err    error
		}{inputs, err}
	}()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("Withdraw did not reach its admission point")
	}
	out := qaPendingWait(t, h, Cursor(last))
	close(release)
	result := <-withdrawn
	if result.err != nil {
		t.Fatal(result.err)
	}
	if len(result.inputs) != 1 || result.inputs[0].Disposition != harness.DispositionWithdrawn {
		t.Fatalf("withdraw = %+v", result.inputs)
	}
	got := qaAwaitWait(t, out)
	if got.Err != nil {
		t.Fatal(got.Err)
	}
	if got.Result.Cursor <= Cursor(last) {
		t.Fatalf("Wait after withdraw = %+v, want a cursor after %d", got.Result, last)
	}
	if !got.Result.Quiescent || got.Result.Driving {
		t.Fatalf("Wait after withdraw = %+v", got.Result)
	}
	page, err := h.Progress(context.Background(), Cursor(last), 0)
	if err != nil {
		t.Fatal(err)
	}
	var withdrawnItems []Item
	for _, item := range page.Items {
		if item.Kind == "withdrawn" {
			withdrawnItems = append(withdrawnItems, item)
		}
	}
	if len(withdrawnItems) != 1 || withdrawnItems[0].Pos != int64(got.Result.Cursor) {
		t.Fatalf("withdrawal progress = %+v, want one item at %d", withdrawnItems, got.Result.Cursor)
	}
	if len(withdrawnItems[0].Withdrawn) != 1 || withdrawnItems[0].Withdrawn[0] != "held" {
		t.Fatalf("withdrawal names = %+v, want the held input ID", withdrawnItems[0].Withdrawn)
	}
	if page.Next != got.Result.Cursor {
		t.Fatalf("withdrawal page next = %d, want the withdrawal position %d", page.Next, got.Result.Cursor)
	}
	again, err := h.Progress(context.Background(), got.Result.Cursor, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Items) != 0 {
		t.Fatalf("withdrawal item repeated after its position: %+v", again.Items)
	}
}

// qaReturnGate pauses a drive after its return transition commits, before
// the follow-up that starts from it.
type qaReturnGate struct {
	harness.Journal
	runID   string
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *qaReturnGate) Commit(ctx context.Context, t harness.Transition) error {
	err := g.Journal.Commit(ctx, t)
	if err == nil && t.From.RunID == g.runID && t.To.Phase == core.PhaseReturned {
		g.once.Do(func() {
			close(g.reached)
			select {
			case <-g.release:
			case <-ctx.Done():
			}
		})
	}
	return err
}

func TestWaitWakesForAFollowUpOpening(t *testing.T) {
	gate := &qaReturnGate{reached: make(chan struct{}), release: make(chan struct{})}
	h := testHost(t, Options{WrapJournal: func(j harness.Journal) harness.Journal {
		gate.Journal = j
		return gate
	}})
	t.Cleanup(func() { qaDrain(t, h) })
	url := answering(t, core.Decision{Kind: core.Return, Payload: "first"}, core.Decision{Kind: core.Return, Payload: "next"})
	first := submit(t, h, Submission{InputID: "first", Binding: binding(url)})
	gate.runID = first.Run.ID
	submit(t, h, Submission{InputID: "next", Goal: "next", Delivery: harness.DeliveryFollowUp, Binding: binding(url)})
	if _, err := h.Drive(context.Background(), first.Run.ID, binding(url), DriveOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gate.reached:
	case <-time.After(3 * time.Second):
		t.Fatal("the first return transition never committed")
	}
	last, err := h.logEnd(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := qaPendingWait(t, h, Cursor(last))
	close(gate.release)
	got := qaAwaitWait(t, out)
	if got.Err != nil {
		t.Fatal(got.Err)
	}
	if got.Result.Cursor <= Cursor(last) {
		t.Fatalf("Wait returned cursor %d after the follow-up opened at %d", got.Result.Cursor, last)
	}
}
