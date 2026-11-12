package harness_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func handoverReply(text string) reply { return execReply(harness.HandoverPrefix + text) }

// performedAt returns the performed transition of step.
func performedAt(t *testing.T, f *fixture, step int) harness.Transition {
	t.Helper()
	for _, tr := range f.transitions() {
		if tr.Event.Kind() == "performed" && tr.From.Step == step {
			return tr
		}
	}
	t.Fatalf("step %d was not performed", step)
	return harness.Transition{}
}

func requestAt(t *testing.T, f *fixture, step, rev int) harness.Request {
	t.Helper()
	req, err := f.store.Request(context.Background(), runID, step, rev)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestHandoverCommitsItsOutcomeAndHeadInOneTransition(t *testing.T) {
	const text = "verified: three Go files, next run the tests"
	f := newFixture(t, core.Limits{MaxSteps: 12, MaxAttempts: 4}, harness.Budget{},
		execReply("true"), handoverReply(text), returnReply("done"))
	f.run(f.loop())
	tr := performedAt(t, f, 2)
	if tr.Facts.Outcome == nil || tr.Facts.Outcome.Handover == nil || tr.Facts.Outcome.Handover.Text != text ||
		tr.Facts.Outcome.Status != harness.OutcomeDone || tr.Facts.ContextState == nil {
		t.Fatalf("handover transition = %+v", tr)
	}
	want := harness.ContextState{
		AnchorSeq: tr.Seq, FirstStep: 3,
		Handover: &harness.HandoverRef{RunID: runID, Seq: tr.Seq, Step: 2},
	}
	if !tr.Facts.ContextState.Equal(want) {
		t.Fatalf("head = %+v, want %+v", *tr.Facts.ContextState, want)
	}
	next := requestAt(t, f, 3, 1)
	if next.ContextState == nil || !next.ContextState.Equal(want) {
		t.Fatalf("next request head = %+v", next.ContextState)
	}
	if next.Context == nil || next.Context.Bytes.Handover == 0 {
		t.Fatalf("the handover section was not rendered: %+v", next.Context)
	}
	body := string(next.Body)
	for _, part := range []string{"## Handover", text, "/steps/00000002/command", "From an earlier run of this line."} {
		if strings.Contains(part, "From an earlier") && strings.Contains(body, part) {
			t.Fatalf("a same-run handover claimed another run: %s", body)
		}
		if !strings.Contains(part, "From an earlier") && !strings.Contains(body, part) {
			t.Fatalf("the next request lacks %q", part)
		}
	}
}

func TestHandoverArgumentRulesLeaveContextUnchanged(t *testing.T) {
	for name, payload := range map[string]string{
		"blank":     harness.HandoverPrefix + " \t ",
		"empty":     "canti:handover",
		"oversized": harness.HandoverPrefix + strings.Repeat("x", harness.HandoverBytes+1),
		"retired":   "canti:handover-carry carry this",
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
				execReply("true"), execReply(payload), returnReply("done"))
			f.run(f.loop())
			tr := performedAt(t, f, 2)
			if tr.Facts.Outcome == nil || tr.Facts.Outcome.Status != harness.OutcomeFailed || tr.Facts.Outcome.Handover != nil || tr.Facts.ContextState != nil {
				t.Fatalf("invalid handover changed context: %+v", tr)
			}
			if next := requestAt(t, f, 3, 1); next.ContextState == nil || next.ContextState.Handover != nil {
				t.Fatalf("invalid handover opened a head: %+v", next.ContextState)
			}
		})
	}
}

func TestDoubleHandoverReplacesTheCarriedHandover(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 12, MaxAttempts: 4}, harness.Budget{},
		handoverReply("first"), handoverReply("second"), returnReply("done"))
	f.run(f.loop())
	second := performedAt(t, f, 2)
	first := performedAt(t, f, 1)
	if second.Facts.ContextState == nil || second.Facts.ContextState.Handover == nil || second.Facts.ContextState.Handover.Step != 2 {
		t.Fatalf("second handover head = %+v", second.Facts.ContextState)
	}
	next := requestAt(t, f, 3, 1)
	if next.ContextState == nil || !next.ContextState.Handover.Equal(second.Facts.ContextState.Handover) {
		t.Fatalf("the newer handover did not replace the carried one: %+v", next.ContextState)
	}
	if strings.Contains(string(next.Body), "first") && !strings.Contains(string(next.Body), "second") {
		t.Fatal("the older handover text was carried")
	}
	if first.Facts.ContextState == nil || first.Facts.ContextState.AnchorSeq != first.Seq {
		t.Fatalf("first handover head = %+v", first.Facts.ContextState)
	}
}

func TestHandoverAtMaxStepsStopsWithoutAdvancing(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 2, MaxAttempts: 3}, harness.Budget{},
		execReply("true"), handoverReply("last allowed step"))
	snap := f.run(f.loop())
	if snap.State.Phase != core.PhaseStopped || snap.State.Reason != core.StopStepsExhausted || snap.State.Step != 2 {
		t.Fatalf("final state = %+v", snap.State)
	}
	tr := performedAt(t, f, 2)
	if tr.Facts.ContextState == nil || tr.Facts.ContextState.AnchorSeq != tr.Seq ||
		tr.Facts.ContextState.FirstStep != tr.To.Step || tr.Facts.ContextState.Handover == nil ||
		tr.Facts.ContextState.Handover.Step != tr.From.Step {
		t.Fatalf("last-step handover head = %+v", tr.Facts.ContextState)
	}
}

func TestHandoverReplayAfterLostAcknowledgementCommitsOneTransition(t *testing.T) {
	for _, landed := range []bool{false, true} {
		t.Run(fmt.Sprint(landed), func(t *testing.T) {
			f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 4}, harness.Budget{},
				handoverReply("resume after the lost ack"), returnReply("done"))
			f.stepUntil(f.bounded(), core.PhaseEffectSaved)
			failed := false
			var first, again harness.Transition
			if _, err := f.loopOn(capturing{failingCommit{f.store, "performed", landed, &failed}, "performed", &first}).Step(f.bounded(), runID); err == nil {
				t.Fatal("acknowledgement failure missing")
			}
			snap, err := f.loopOn(capturing{f.open(), "performed", &again}).Step(f.bounded(), runID)
			if err != nil {
				t.Fatal(err)
			}
			if !landed {
				if !reflect.DeepEqual(first, again) {
					a, _ := json.Marshal(first)
					b, _ := json.Marshal(again)
					t.Fatalf("local replay changed: %s vs %s", a, b)
				}
				if snap.State.Phase != core.PhaseIdle || snap.State.Step != 2 {
					t.Fatalf("replay state = %+v", snap.State)
				}
			}
			n := 0
			for _, tr := range f.transitions() {
				if tr.Event.Kind() == "performed" && tr.From.Step == 1 {
					n++
				}
			}
			if n != 1 {
				t.Fatalf("handover committed %d times", n)
			}
			current, err := f.store.Context(f.bounded(), mustLatest(t, f))
			if err != nil || current.Handover == nil || current.AnchorSeq != 5 {
				t.Fatalf("committed head = %+v, %v", current, err)
			}
		})
	}
}

func mustLatest(t *testing.T, f *fixture) harness.Snapshot {
	t.Helper()
	snap, err := f.store.Latest(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestHandoverCancellationBeforeCommitLeavesNoHead(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 4}, harness.Budget{},
		handoverReply("must not be committed"), returnReply("done"))
	if snap := f.steps(f.loop(), 3); snap.State.Phase != core.PhaseEffectSaved {
		t.Fatalf("state before cancel = %+v", snap.State)
	}
	snap, err := f.loop().Stop(f.bounded(), runID, core.StopCancelled)
	if err != nil || snap.State.Phase != core.PhaseStopped {
		t.Fatalf("stop = %+v, %v", snap.State, err)
	}
	for _, tr := range f.transitions() {
		if tr.Event.Kind() == "performed" {
			t.Fatalf("a cancelled local action was performed: %+v", tr)
		}
		if tr.Facts.ContextState != nil {
			t.Fatalf("a cancelled local action opened a head: %+v", tr.Facts.ContextState)
		}
	}
}

// capturing records the first transition of a kind the delegate commits.
type capturing struct {
	harness.Journal
	kind string
	got  *harness.Transition
}

func (j capturing) Commit(ctx context.Context, tr harness.Transition) error {
	if tr.Event.Kind() == j.kind && j.got.Event == nil {
		*j.got = tr
	}
	return j.Journal.Commit(ctx, tr)
}

var _ = errors.Is
