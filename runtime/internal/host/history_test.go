package host

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestHistoryPagesTheLineFromItsEndToItsStart(t *testing.T) {
	h := testHost(t, Options{})
	url := answering(t, core.Decision{Kind: core.Return, Payload: "first answer"}, core.Decision{Kind: core.Return, Payload: "second answer"})
	first := submit(t, h, Submission{Goal: "first goal", Binding: binding(url)}).Run
	driveToReturn(t, h, first.ID, binding(url))
	second := submit(t, h, Submission{Goal: "second goal", Fresh: true, Binding: binding(url)}).Run
	driveToReturn(t, h, second.ID, binding(url))

	ctx := context.Background()
	page, err := h.History(ctx, HistoryCursor{}, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if page.Latest == nil || page.Latest.Run.ID != second.ID || page.Latest.State.Phase != core.PhaseReturned || page.Latest.Result != "second answer" {
		t.Fatalf("latest run = %+v, want the returned second run", page.Latest)
	}
	if len(page.Runs) != 1 || page.Runs[0].Run.ID != second.ID || page.Cursor == 0 || page.Older.Pos == 0 || len(page.Items) != 1 {
		t.Fatalf("newest page = %+v, want one run and one row with an older page", page)
	}
	var all []Item
	line := page.Line
	seen := map[int64]bool{}
	for more := true; more; {
		for _, it := range page.Items {
			if seen[it.Pos] {
				t.Fatalf("history repeated position %d", it.Pos)
			}
			seen[it.Pos] = true
			if it.RunID != second.ID {
				t.Fatalf("history crossed the fresh boundary into %s", it.RunID)
			}
		}
		all = append(append([]Item{}, page.Items...), all...)
		more = page.Older.Pos != 0
		if !more {
			break
		}
		if page, err = h.History(ctx, page.Older, 1, line); err != nil {
			t.Fatal(err)
		}
		if page.Latest != nil || len(page.Runs) == 0 {
			t.Fatalf("older page summaries = %+v", page)
		}
	}
	if len(all) < 2 {
		t.Fatalf("history holds only %d rows", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i-1].Pos >= all[i].Pos {
			t.Fatalf("history rows are not in agent-log order: %d then %d", all[i-1].Pos, all[i].Pos)
		}
	}
	last, err := h.logEnd(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.History(ctx, HistoryCursor{Pos: Cursor(last + 1)}, 1, line); !errors.Is(err, ErrCursor) {
		t.Fatalf("a history cursor past the end = %v, want ErrCursor", err)
	}
	if _, err := h.History(ctx, HistoryCursor{Pos: Cursor(-1)}, 1, ""); !errors.Is(err, ErrCursor) {
		t.Fatalf("a negative history cursor = %v, want ErrCursor", err)
	}
}

func TestHistorySeenMarksTheSteerInTheFrozenRequestAndHeldAtStop(t *testing.T) {
	url, sent := holding(t)
	h := testHost(t, Options{})
	ack := submit(t, h, Submission{Goal: "goal", Binding: binding(url)})
	submit(t, h, Submission{InputID: "seen-steer", Goal: "seen steer", Delivery: harness.DeliverySteer, Binding: binding(url)})
	d, err := h.Drive(context.Background(), ack.Run.ID, binding(url), DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	<-sent
	submit(t, h, Submission{InputID: "held-steer", Goal: "held steer", Delivery: harness.DeliverySteer, Binding: binding(url)})
	if _, err := h.Stop(context.Background(), ack.Run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Result(); !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped drive ended with %v, want its cancellation", err)
	}
	page, err := h.History(context.Background(), HistoryCursor{}, 64, "")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, item := range page.Items {
		if item.Input != nil && (item.Input.InputID == "seen-steer" || item.Input.InputID == "held-steer") {
			seen[item.Input.InputID] = item.Input.Seen
		}
	}
	if !seen["seen-steer"] || seen["held-steer"] {
		t.Fatalf("seen states = %v; want the frozen request's steer seen and the stop-race steer held", seen)
	}
}

func TestHistorySummarizesAStoppedRun(t *testing.T) {
	h := testHost(t, Options{})
	url := answering(t, core.Decision{Kind: core.Exec, Payload: "canti:handover step one"})
	spec := testSpec(h)
	spec.Limits.MaxSteps = 1
	ack := submit(t, h, Submission{Goal: "one step", Spec: spec, Binding: binding(url)})
	d, err := h.Drive(context.Background(), ack.Run.ID, binding(url), DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if snap, err := d.Result(); err != nil || snap.State.Phase != core.PhaseStopped || snap.State.Reason != core.StopStepsExhausted {
		t.Fatalf("drive = %+v, %v", snap.State, err)
	}
	page, err := h.History(context.Background(), HistoryCursor{}, 64, "")
	if err != nil {
		t.Fatal(err)
	}
	if page.Latest == nil || page.Latest.State.Phase != core.PhaseStopped || page.Latest.State.Reason != core.StopStepsExhausted || page.Latest.Result != "" {
		t.Fatalf("stopped summary = %+v, want the steps-exhausted reason", page.Latest)
	}
}

func TestHistorySummarizesEarlierClosedRuns(t *testing.T) {
	h := testHost(t, Options{})
	stoppedURL := answering(t, core.Decision{Kind: core.Exec, Payload: "canti:handover step one"})
	spec := testSpec(h)
	spec.Limits.MaxSteps = 1
	stopped := submit(t, h, Submission{Goal: "one step", Spec: spec, Binding: binding(stoppedURL)})
	d, err := h.Drive(context.Background(), stopped.Run.ID, binding(stoppedURL), DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Result(); err != nil {
		t.Fatal(err)
	}
	url := answering(t, core.Decision{Kind: core.Return, Payload: "done"})
	returned := submit(t, h, Submission{Goal: "after the stop", Binding: binding(url)})
	driveToReturn(t, h, returned.Run.ID, binding(url))
	page, err := h.History(context.Background(), HistoryCursor{}, 64, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 2 || page.Runs[0].Run.ID != returned.Run.ID || page.Runs[1].Run.ID != stopped.Run.ID {
		t.Fatalf("history runs = %+v, want the returned run then the stopped run", page.Runs)
	}
	if page.Runs[1].State.Phase != core.PhaseStopped || page.Runs[1].State.Reason != core.StopStepsExhausted {
		t.Fatalf("earlier run summary = %+v, want its steps-exhausted outcome", page.Runs[1])
	}
}

func TestHistoryClipsASummaryAnswer(t *testing.T) {
	h := testHost(t, Options{})
	answer := strings.Repeat("x", historyTextBytes+10)
	url := answering(t, core.Decision{Kind: core.Return, Payload: answer})
	ack := submit(t, h, Submission{Goal: "long answer", Binding: binding(url)})
	driveToReturn(t, h, ack.Run.ID, binding(url))
	page, err := h.History(context.Background(), HistoryCursor{}, 64, "")
	if err != nil {
		t.Fatal(err)
	}
	if page.Latest == nil || len(page.Latest.Result) != historyTextBytes || !page.Latest.Truncated {
		t.Fatalf("clipped answer = %d bytes, truncated %v; want %d and a notice", len(page.Latest.Result), page.Latest.Truncated, historyTextBytes)
	}
}

func TestLatestHistoryRechecksWithPointReadsOnly(t *testing.T) {
	h := testHost(t, Options{})
	url := answering(t, core.Decision{Kind: core.Return, Payload: "done"})
	ack := submit(t, h, Submission{Goal: "latest", Binding: binding(url)})
	driveToReturn(t, h, ack.Run.ID, binding(url))
	page, err := h.LatestHistory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if page.Latest == nil || page.Latest.Run.ID != ack.Run.ID || len(page.Runs) != 1 || len(page.Items) != 0 || page.Cursor != 0 || page.Older.Pos != 0 {
		t.Fatalf("latest history = %+v, want only the latest run summary", page)
	}
	if page.Latest.Result != "done" || page.Latest.State.Phase != core.PhaseReturned {
		t.Fatalf("latest summary = %+v", page.Latest)
	}
}

func TestHistoryOfAnEmptyStateDirectoryHasNoLatestRun(t *testing.T) {
	h := testHost(t, Options{})
	page, err := h.History(context.Background(), HistoryCursor{}, 0, "")
	if err != nil || page.Latest != nil || len(page.Runs) != 0 || len(page.Items) != 0 || page.Cursor != 0 || page.Older.Pos != 0 {
		t.Fatalf("empty history = %+v, %v", page, err)
	}
}

func driveToReturn(t *testing.T, h *Host, id string, b Binding) {
	t.Helper()
	d, err := h.Drive(context.Background(), id, b, DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if snap, err := d.Result(); err != nil || snap.State.Phase != core.PhaseReturned {
		t.Fatalf("drive = %+v, %v", snap.State, err)
	}
}
