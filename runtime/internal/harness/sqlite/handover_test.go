package sqlite

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// commitHandover commits one successful handover of the driver's run with a
// complete payload and returns its head.
func commitHandover(t *testing.T, d *driver, payload string) harness.ContextState {
	t.Helper()
	d.decide(execBody(payload), harness.Usage{})
	before := d.snap
	seq := before.Seq + 1
	head := harness.ContextState{AnchorSeq: seq, FirstStep: before.State.Step + 1}
	d.do(core.Performed{EffectID: before.State.EffectID}, harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeDone}, ContextState: &head})
	return head
}

// performHandover commits one successful handover with the given note.
func performHandover(t *testing.T, d *driver, text string) harness.ContextState {
	t.Helper()
	return commitHandover(t, d, "canti:handover "+text)
}

func TestEveryRunStartsFromInitialContext(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s := open(t, path)
	d := newDriver(t, s, createRun(t, s, "run-1", core.Limits{MaxSteps: 10, MaxAttempts: 3}))
	head := performHandover(t, d, "phase one")
	if head.AnchorSeq == 0 {
		t.Fatalf("handover did not open a head: %+v", head)
	}
	stopRun(t, s, testRun("run-1"))
	createRun(t, s, "run-2", core.Limits{MaxSteps: 10, MaxAttempts: 3})
	snap, err := s.Latest(ctx, "run-2")
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Context(ctx, snap)
	if err != nil || !got.Equal(harness.InitialContext()) {
		t.Fatalf("a later run inherited a head: %+v, %v", got, err)
	}
	// The head survives reopen as the initial head; no line walk is involved.
	reopened := open(t, path)
	if got, err := reopened.Context(ctx, snap); err != nil || !got.Equal(harness.InitialContext()) {
		t.Fatalf("reopened initial head = %+v, %v", got, err)
	}
}

func TestHandoverHeadRejectionMustMatchTheRun(t *testing.T) {
	for _, damage := range []string{"dropped", "older", "modified"} {
		t.Run(damage, func(t *testing.T) {
			ctx := context.Background()
			s := open(t, tempPath(t))
			d := newDriver(t, s, createRun(t, s, "run-rejection-head", core.Limits{MaxSteps: 10, MaxAttempts: 3}))
			signal := harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}
			reject := func(step int) int64 {
				d.freeze()
				d.send()
				current, err := s.Context(ctx, d.snap)
				if err != nil {
					t.Fatal(err)
				}
				seq := d.snap.Seq + 1
				rejection := &harness.ContextRejection{Previous: current, Signal: signal, EpochSteps: 1}
				head, class := rejection.Head(seq, step, current)
				if class != harness.RejectionOpensHead {
					t.Fatalf("fixture rejection class = %v", class)
				}
				d.do(core.RequestRejected{Step: step, Attempt: d.snap.State.Attempt}, harness.Facts{Response: &harness.Response{Body: []byte(`{}`)},
					Failure: "confirmed overflow", Rejection: rejection, ContextState: &head})
				return seq
			}
			performHandover := func(text string) {
				d.freeze()
				d.send()
				body := execBody("canti:handover " + text)
				d.save(body, harness.Usage{})
				d.load(body)
				current, err := s.Context(ctx, d.snap)
				if err != nil {
					t.Fatal(err)
				}
				before := d.snap
				seq := before.Seq + 1
				head := harness.ContextState{AnchorSeq: seq, FirstStep: before.State.Step + 1, Rejection: current.Rejection}
				d.do(core.Performed{EffectID: before.State.EffectID}, harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeDone}, ContextState: &head})
			}
			first := reject(1)
			performHandover("first phase")
			reject(2)
			performHandover("second phase")
			headSeq := d.snap.Seq
			if got, err := s.Context(ctx, d.snap); err != nil || got.AnchorSeq != headSeq || got.Rejection == nil || got.Rejection.Seq == first {
				t.Fatalf("untampered handover head = %+v, %v", got, err)
			}
			var mutation string
			switch damage {
			case "dropped":
				mutation = `json_remove(facts,'$.ContextState.Rejection')`
			case "older":
				mutation = fmt.Sprintf(`json_set(facts,'$.ContextState.Rejection',json('{"Seq":%d,"Step":1,"MaxAcceptedInput":0,"MaxAcceptedReported":false}'))`, first)
			case "modified":
				mutation = `json_set(facts,'$.ContextState.Rejection.MaxAcceptedInput',999)`
			}
			if _, err := s.db.Exec(`UPDATE transitions SET facts=`+mutation+` WHERE run_id=? AND seq=?`, d.snap.State.RunID, headSeq); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(`UPDATE runs SET context_seq=? WHERE id=?`, headSeq, d.snap.State.RunID); err != nil {
				t.Fatal(err)
			}
			if got, err := s.Context(ctx, d.snap); err == nil {
				t.Fatalf("accepted a %s rejection reference: %+v", damage, got)
			}
		})
	}
}

func TestCreateRunBoundsTheRunID(t *testing.T) {
	s := open(t, tempPath(t))
	initial, err := core.NewState(strings.Repeat("r", harness.MaxRunIDBytes+1), core.Limits{MaxSteps: 2, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	run := testRun(strings.Repeat("r", harness.MaxRunIDBytes+1))
	if err := s.CreateRun(context.Background(), run, initial); err == nil {
		t.Fatal("a run ID over the bound was accepted")
	}
}

func TestHandoverBytesComesFromTheCommittedPayload(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		failed        bool
		want          int
	}{
		{"with a note", "canti:handover phase one", false, len("phase one")},
		{"without a note", "canti:handover", false, 0},
		{"a separator only", "canti:handover ", false, 0},
		{"a maximum note", "canti:handover " + strings.Repeat("x", harness.HandoverBytes), false, harness.HandoverBytes},
		{"clipping splits the action word", strings.Repeat(" ", 2040) + "canti:handover abc", false, 3},
		{"clipping immediately before the separator", strings.Repeat(" ", 2034) + "canti:handover abc", false, 3},
		{"clipping immediately after the separator", strings.Repeat(" ", 2033) + "canti:handover abc", false, 3},
		{"a failed handover", "canti:handover " + strings.Repeat("x", harness.HandoverBytes+1), true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := open(t, tempPath(t))
			d := newDriver(t, s, createRun(t, s, "run-handover-bytes", core.Limits{MaxSteps: 6, MaxAttempts: 3}))
			if tc.failed {
				d.decide(execBody(tc.payload), harness.Usage{})
				before := d.snap
				d.do(core.Performed{EffectID: before.State.EffectID}, harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeFailed,
					Detail: "harness: handover note exceeds 2048 bytes"}})
			} else {
				commitHandover(t, d, tc.payload)
			}
			rows, err := s.Progress(context.Background(), "run-handover-bytes", 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			var got *ProgressRow
			for i := len(rows) - 1; i >= 0; i-- {
				if rows[i].Kind == "performed" {
					row := rows[i]
					got = &row
					break
				}
			}
			if got == nil {
				t.Fatal("no performed progress row")
			}
			if got.HandoverBytes != tc.want {
				t.Fatalf("handover_bytes = %d, want %d (outcome %q)", got.HandoverBytes, tc.want, got.Outcome)
			}
			if tc.failed && got.Outcome != string(harness.OutcomeFailed) {
				t.Fatalf("failed handover reported %q", got.Outcome)
			}
		})
	}
}
