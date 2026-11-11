package harness_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func inputPath(root string, pos int64) string {
	return filepath.Join(root, fmt.Sprintf("%012d", pos))
}

func readPublishedInput(t *testing.T, root string, in harness.Input) {
	t.Helper()
	for _, file := range []string{"meta", "text"} {
		if _, err := os.ReadFile(filepath.Join(inputPath(root, in.Pos), file)); err != nil {
			t.Fatalf("delivered input %d missing archive %s: %v", in.Pos, file, err)
		}
	}
}

func TestUndeliveredInputsAreNotPublished(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	f.cfg.Inputs = t.TempDir()
	ctx := context.Background()
	first, err := f.store.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityObservation,
		Source: "ci", Subject: "build", Text: "red", Now: created})
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.store.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityObservation,
		Source: "ci", Subject: "build", Text: "green", Now: created})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.loop().Step(ctx, runID); err != nil {
		t.Fatal(err)
	}
	readPublishedInput(t, f.cfg.Inputs, second.Input)
	delivered, err := f.store.InputAt(ctx, second.Input.Pos)
	if err != nil || delivered.DeliveredPos == 0 {
		t.Fatalf("rendered observation delivery = %+v, %v", delivered, err)
	}
	undelivered, err := f.store.InputAt(ctx, first.Input.Pos)
	if err != nil || undelivered.DeliveredPos != 0 {
		t.Fatalf("superseded observation delivery = %+v, %v", undelivered, err)
	}
	if _, err := os.Stat(inputPath(f.cfg.Inputs, first.Input.Pos)); !os.IsNotExist(err) {
		t.Fatalf("superseded observation %d was published", first.Input.Pos)
	}
	queued, err := f.store.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityPrincipal,
		Delivery: harness.DeliveryFollowUp, Text: "later", Now: created})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Withdraw(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(inputPath(f.cfg.Inputs, queued.Input.Pos)); !os.IsNotExist(err) {
		t.Fatalf("withdrawn undelivered input %d was published", queued.Input.Pos)
	}
}

func TestQueuedFollowUpIsPublishedWhenItsRunFirstFreezes(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, returnReply("done"))
	f.cfg.Inputs = t.TempDir()
	ctx := context.Background()
	queued, err := f.store.Admit(ctx, harness.InputSubmission{InputID: "follow", Authority: harness.AuthorityPrincipal,
		Delivery: harness.DeliveryFollowUp, Text: "later", Now: created})
	if err != nil {
		t.Fatal(err)
	}
	l := f.loop()
	snap, err := l.Run(f.bounded(), runID)
	if err != nil || snap.State.Phase != core.PhaseReturned {
		t.Fatalf("first run = %+v, %v", snap.State, err)
	}
	initial, err := core.NewState("next", limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.OpenFollowUp(ctx, queued.Input, harness.Run{ID: "next", Goal: "later", Created: created}, initial); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Step(ctx, "next"); err != nil {
		t.Fatal(err)
	}
	readPublishedInput(t, f.cfg.Inputs, queued.Input)
	delivered, err := f.store.InputAt(ctx, queued.Input.Pos)
	if err != nil || delivered.DeliveredPos == 0 {
		t.Fatalf("queued input delivery = %+v, %v", delivered, err)
	}
}

func TestQueuedWithdrawalLeavesNoRecord(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	f.cfg.Inputs = t.TempDir()
	ctx := context.Background()
	queued, err := f.store.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityPrincipal,
		Delivery: harness.DeliveryFollowUp, Text: "later", Now: created})
	if err != nil {
		t.Fatal(err)
	}
	stop(t, f, runID)
	if _, err := f.store.Withdraw(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(inputPath(f.cfg.Inputs, queued.Input.Pos)); !os.IsNotExist(err) {
		t.Fatalf("withdrawn input %d was published", queued.Input.Pos)
	}
}

func TestBoundSteerWithoutFreezeLeavesNoRecord(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	f.cfg.Inputs = t.TempDir()
	ctx := context.Background()
	steer, err := f.store.Admit(ctx, harness.InputSubmission{InputID: "steer", Authority: harness.AuthorityPrincipal,
		Delivery: harness.DeliverySteer, Text: strings.Repeat("x", 16<<10), Now: created})
	if err != nil {
		t.Fatal(err)
	}
	f.cfg.Limits = harness.DefaultPromptLimits()
	f.cfg.Limits.Total = 8192
	snap, err := f.loop().Step(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.State.Phase != core.PhaseStopped || snap.State.Seen != 0 || snap.State.Inputs != 1 {
		t.Fatalf("bound steer did not stop before freeze: %+v", snap.State)
	}
	if _, err := f.store.Withdraw(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(inputPath(f.cfg.Inputs, steer.Input.Pos)); !os.IsNotExist(err) {
		t.Fatalf("steer %d was published before a frozen request", steer.Input.Pos)
	}
	if got, err := f.store.InputAt(ctx, steer.Input.Pos); err != nil || got.DeliveredPos != 0 {
		t.Fatalf("steer delivery = %+v, %v", got, err)
	}
}

func TestMissingDeliveredRecordRepairsOnNativeRead(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	f.cfg.Inputs = t.TempDir()
	f.cfg.Reader = &archive.Reader{Inputs: f.cfg.Inputs}
	ctx := context.Background()
	in, err := f.store.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityObservation,
		Source: "ci", Subject: "repair", Text: "durable", Now: created})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.loop().Step(ctx, runID); err != nil {
		t.Fatal(err)
	}
	textPath := filepath.Join(inputPath(f.cfg.Inputs, in.Input.Pos), "text")
	if err := os.RemoveAll(inputPath(f.cfg.Inputs, in.Input.Pos)); err != nil {
		t.Fatal(err)
	}
	got, err := f.cfg.Reader.Retrieve(ctx, archive.RetrievalQuery{Operation: "read", Path: "/canti/inputs/" + fmt.Sprintf("%012d", in.Input.Pos) + "/text", Reason: "repair"})
	if err != nil || got.Error != "" || len(got.Sources) != 1 || string(got.Evidence[0]) != "durable" {
		t.Fatalf("native repair = %+v, %v", got, err)
	}
	if body, err := os.ReadFile(textPath); err != nil || string(body) != "durable" {
		t.Fatalf("repaired file = %q, %v", body, err)
	}
}

func TestUndeliveredNativeReadReportsAbsent(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	f.cfg.Inputs = t.TempDir()
	f.cfg.Reader = &archive.Reader{Inputs: f.cfg.Inputs}
	ctx := context.Background()
	in, err := f.store.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityObservation,
		Source: "ci", Subject: "undelivered", Text: "never shown", Now: created})
	if err != nil {
		t.Fatal(err)
	}
	f.loop()
	got, err := f.cfg.Reader.Retrieve(ctx, archive.RetrievalQuery{Operation: "read", Path: "/canti/inputs/" + fmt.Sprintf("%012d", in.Input.Pos) + "/text", Reason: "absent"})
	if err != nil || got.Error == "" || !strings.Contains(got.Error, "not delivered") {
		t.Fatalf("undelivered native read = %+v, %v", got, err)
	}
}

func TestPartialInputRecordIsRepairedOnNextFreeze(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	f.cfg.Inputs = t.TempDir()
	ctx := context.Background()
	in, err := f.store.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityObservation,
		Source: "ci", Subject: "partial", Text: "repaired", Now: created})
	if err != nil {
		t.Fatal(err)
	}
	dir := inputPath(f.cfg.Inputs, in.Input.Pos)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(f.cfg.Inputs, ".tmp-"+fmt.Sprintf("%012d", in.Input.Pos)), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := f.loop().Step(ctx, runID); err != nil {
		t.Fatal(err)
	}
	readPublishedInput(t, f.cfg.Inputs, in.Input)
	if _, err := os.Stat(filepath.Join(f.cfg.Inputs, ".tmp-"+fmt.Sprintf("%012d", in.Input.Pos))); !os.IsNotExist(err) {
		t.Fatal("stale temporary was not removed")
	}
}

type inputPositionProbe struct {
	harness.Journal
	seen [][]int64
}

func (p *inputPositionProbe) InputsAt(ctx context.Context, positions []int64) ([]harness.Input, error) {
	p.seen = append(p.seen, append([]int64{}, positions...))
	return p.Journal.InputsAt(ctx, positions)
}

func TestPreparationReadsOnlyRenderedInputs(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	f.cfg.Inputs = t.TempDir()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if _, err := f.store.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityObservation,
			Source: "ci", Subject: "same", Text: fmt.Sprintf("observation %d", i), Now: created}); err != nil {
			t.Fatal(err)
		}
	}
	probe := &inputPositionProbe{Journal: f.store}
	if _, err := f.loopOn(probe).Step(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if len(probe.seen) != 1 || len(probe.seen[0]) != 1 {
		t.Fatalf("preparation read positions %v; want only the rendered observation", probe.seen)
	}
}

func TestNativeRepairRejectsFIFOWithoutBlocking(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	f.cfg.Inputs = t.TempDir()
	f.cfg.Reader = &archive.Reader{Inputs: f.cfg.Inputs}
	ctx := context.Background()
	in, err := f.store.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityObservation,
		Source: "ci", Subject: "fifo", Text: "durable", Now: created})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.loop().Step(ctx, runID); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(inputPath(f.cfg.Inputs, in.Input.Pos), "text")
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(target, 0o600); err != nil {
		t.Fatal(err)
	}
	call, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		got, err := f.cfg.Reader.Retrieve(call, archive.RetrievalQuery{Operation: "read",
			Path: fmt.Sprintf("/canti/inputs/%012d/meta", in.Input.Pos), Reason: "read safe metadata"})
		if err == nil && got.Error != "" {
			err = fmt.Errorf("%s", got.Error)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected nonregular-file rejection")
		}
	case <-time.After(300 * time.Millisecond):
		writer, err := os.OpenFile(target, os.O_RDWR|syscall.O_NONBLOCK, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.WriteString("durable"); err != nil {
			t.Fatal(err)
		}
		writer.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("reader did not release")
		}
		t.Fatal("native repair blocked on FIFO beyond context deadline before archive path/type validation")
	}
}
