package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func closeLineRun(t *testing.T, s *Store, id string) {
	t.Helper()
	snap, err := s.Latest(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := harness.NewRunner(s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Commit(context.Background(), snap, core.StopRequested{Reason: core.StopCancelled}, harness.Facts{}); err != nil {
		t.Fatal(err)
	}
}

func createLineRun(t *testing.T, s *Store, id string, fresh bool) harness.Snapshot {
	t.Helper()
	initial, err := core.NewState(id, core.Limits{MaxSteps: 10, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	run := testRun(id)
	run.Fresh = fresh
	if err := s.CreateRun(context.Background(), run, initial); err != nil {
		t.Fatal(err)
	}
	return harness.Snapshot{State: initial}
}

// handoverRun performs one handover in run id and closes it, so that its
// handover is a candidate for the line's later runs.
func handoverRun(t *testing.T, s *Store, id string, fresh bool, text string) {
	t.Helper()
	d := newDriver(t, s, createLineRun(t, s, id, fresh))
	d.decide(execBody("canti:handover "+text), harness.Usage{})
	before := d.snap
	seq := before.Seq + 1
	head := harness.ContextState{AnchorSeq: seq, FirstStep: before.State.Step + 1,
		Handover: &harness.HandoverRef{RunID: id, Seq: seq, Step: before.State.Step}}
	d.do(core.Performed{EffectID: before.State.EffectID}, harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeDone, Handover: &harness.Handover{Text: text}}, ContextState: &head})
	d.do(core.StopRequested{Reason: core.StopCancelled}, harness.Facts{})
}

func TestInitialHeadInheritsTheNewestHandoverOfTheLine(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s := open(t, path)
	handoverRun(t, s, "run-1", true, "first phase")
	handoverRun(t, s, "run-2", false, "second phase")
	createLineRun(t, s, "run-3", false)
	snap, err := s.Latest(ctx, "run-3")
	if err != nil {
		t.Fatal(err)
	}
	head, err := s.Context(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}
	if head.AnchorSeq != 0 || head.FirstStep != 1 || head.Handover == nil || head.Handover.RunID != "run-2" || head.Minimal || head.Rejection != nil {
		t.Fatalf("inherited head = %+v", head)
	}
	record, err := s.Completion(ctx, head.Handover.RunID, head.Handover.Seq)
	if err != nil || record.Outcome == nil || record.Outcome.Handover == nil || record.Outcome.Handover.Text != "second phase" {
		t.Fatalf("inherited handover = %+v, %v", record, err)
	}
	// The inherited reference freezes with the first request and survives
	// reopen.
	d := newDriver(t, s, snap)
	d.freeze()
	reopened := open(t, path)
	got, err := reopened.Context(ctx, d.snap)
	if err != nil || !got.Handover.Equal(head.Handover) {
		t.Fatalf("frozen inherited head = %+v, %v", got, err)
	}
}

func TestInitialHeadStopsAtAFreshStart(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	handoverRun(t, s, "run-1", true, "before the boundary")
	handoverRun(t, s, "run-2", true, "the new line")
	createLineRun(t, s, "run-3", false)
	snap, err := s.Latest(ctx, "run-3")
	if err != nil {
		t.Fatal(err)
	}
	head, err := s.Context(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}
	if head.Handover == nil || head.Handover.RunID != "run-2" {
		t.Fatalf("a fresh start was not inherited: %+v", head)
	}
	// The run that started fresh is eligible; its own predecessor is not.
	closeLineRun(t, s, "run-3")
	createLineRun(t, s, "run-4", true)
	snap, err = s.Latest(ctx, "run-4")
	if err != nil {
		t.Fatal(err)
	}
	if head, err := s.Context(ctx, snap); err != nil || head.Handover != nil || !head.Equal(harness.InitialContext()) {
		t.Fatalf("a fresh run inherited a handover: %+v, %v", head, err)
	}
}

func TestInitialHeadFindsAHandoverPastThePreviousRunPage(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	handoverRun(t, s, "run-1", true, "beyond the page")
	for i := 2; i <= 20; i++ {
		createLineRun(t, s, fmt.Sprintf("run-%d", i), false)
		closeLineRun(t, s, fmt.Sprintf("run-%d", i))
	}
	snap, err := s.Latest(ctx, "run-20")
	if err != nil {
		t.Fatal(err)
	}
	head, err := s.Context(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}
	if head.Handover == nil || head.Handover.RunID != "run-1" {
		t.Fatalf("handover past the 16-run page was lost: %+v", head)
	}
}

func TestInheritedHeadRejectionKeepsTheReference(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s := open(t, path)
	handoverRun(t, s, "run-1", true, "the inherited note")
	snap := createLineRun(t, s, "run-2", false)
	current, err := s.Context(ctx, snap)
	if err != nil || current.Handover == nil {
		t.Fatalf("initial head = %+v, %v", current, err)
	}
	d := newDriver(t, s, snap)
	d.freeze()
	d.send()
	rejection := &harness.ContextRejection{Previous: current, Signal: harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"},
		EpochSteps: 1}
	head, class := rejection.Head(d.snap.Seq+1, d.snap.State.Step, current)
	if class != harness.RejectionOpensHead {
		t.Fatalf("optional content did not open a floor: %v", class)
	}
	d.do(core.RequestRejected{Step: 1, Attempt: 1}, harness.Facts{Response: &harness.Response{Body: []byte(`{}`)}, Failure: "confirmed overflow",
		Rejection: rejection, ContextState: &head})
	reopened := open(t, path)
	got, err := reopened.Context(ctx, d.snap)
	if err != nil || !got.Handover.Equal(current.Handover) || got.Minimal {
		t.Fatalf("rejected inherited head = %+v, %v", got, err)
	}
}

func TestForeignHandoverOriginIsRefused(t *testing.T) {
	s := open(t, tempPath(t))
	handoverRun(t, s, "run-1", true, "phase")
	snap := createLineRun(t, s, "run-2", false)
	d := newDriver(t, s, snap)
	d.decide(execBody("canti:handover another phase"), harness.Usage{})
	before := d.snap
	seq := before.Seq + 1
	head := harness.ContextState{AnchorSeq: seq, FirstStep: before.State.Step + 1,
		Handover: &harness.HandoverRef{RunID: "run_foreign", Seq: 1, Step: 1}}
	facts := harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeDone, Handover: &harness.Handover{Text: "another phase"}}, ContextState: &head}
	if err := d.try(core.Performed{EffectID: before.State.EffectID}, facts); err == nil {
		t.Fatal("a handover from an unknown run was accepted")
	}
}

func TestAnotherStateDirectoryCarriesNoHandover(t *testing.T) {
	first := open(t, tempPath(t))
	handoverRun(t, first, "run-1", true, "state one note")
	second := open(t, tempPath(t))
	createLineRun(t, second, "run-2", false)
	snap, err := second.Latest(context.Background(), "run-2")
	if err != nil {
		t.Fatal(err)
	}
	if head, err := second.Context(context.Background(), snap); err != nil || head.Handover != nil || !head.Equal(harness.InitialContext()) {
		t.Fatalf("a state directory saw another's handover: %+v, %v", head, err)
	}
}

func TestInheritedHandoverRequiresTheSourceCompletion(t *testing.T) {
	for _, damage := range []string{"missing-head", "failed-outcome", "missing-outcome", "wrong-action", "to-state-run"} {
		t.Run(damage, func(t *testing.T) {
			path := tempPath(t)
			s := open(t, path)
			handoverRun(t, s, "run-1", true, "source note")
			var seq, step, attempt int64
			if err := s.db.QueryRow(`SELECT seq, json_extract(from_state,'$.Step'), json_extract(from_state,'$.Attempt') FROM transitions WHERE run_id='run-1' AND kind='performed'`).Scan(&seq, &step, &attempt); err != nil {
				t.Fatal(err)
			}
			var mutation string
			switch damage {
			case "missing-head":
				mutation = `json_remove(facts,'$.ContextState')`
			case "failed-outcome":
				mutation = `json_set(facts,'$.Outcome.Status','failed')`
			case "missing-outcome":
				mutation = `json_remove(facts,'$.Outcome')`
			case "wrong-action":
				if _, err := s.db.Exec(`UPDATE transitions SET facts=json_set(facts,'$.Action.Name','retrieve') WHERE run_id='run-1' AND kind='response_loaded' AND step=? AND attempt=?`, step, attempt); err != nil {
					t.Fatal(err)
				}
				mutation = `facts`
			case "to-state-run":
				if _, err := s.db.Exec(`UPDATE transitions SET to_state=json_set(to_state,'$.RunID','run_other') WHERE run_id='run-1' AND seq=?`, seq); err != nil {
					t.Fatal(err)
				}
				mutation = `facts`
			}
			if _, err := s.db.Exec(`UPDATE transitions SET facts=`+mutation+` WHERE run_id='run-1' AND seq=?`, seq); err != nil {
				t.Fatal(err)
			}
			createLineRun(t, s, "run-2", false)
			snap, err := s.Latest(context.Background(), "run-2")
			if err != nil {
				t.Fatal(err)
			}
			head, err := s.Context(context.Background(), snap)
			if damage == "missing-outcome" {
				// The source no longer looks like a handover, so nothing is
				// inherited; it can never be selected as a corrupted one.
				if err != nil || head.Handover != nil {
					t.Fatalf("missing outcome inherited %+v, %v", head, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("inherited a source with %s: %+v", damage, head)
			}
		})
	}
}

func TestInitialHeadIgnoresUnrelatedPerformedHistory(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	createLineRun(t, s, "run-unrelated", true)
	closeLineRun(t, s, "run-unrelated")
	var closed string
	if err := s.db.QueryRow(`SELECT to_state FROM transitions WHERE run_id='run-unrelated' ORDER BY seq DESC LIMIT 1`).Scan(&closed); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO transitions
		(run_id, seq, pos, event_id, kind, semantics, event, from_state, to_state, facts, step, attempt, rev)
		VALUES ('run-unrelated', ?, ?, ?, 'performed', 8, '{"EffectID":"effect-unrelated"}',
			json_set(?, '$.Step', ?, '$.Limits.MaxSteps', 0), json_set(?, '$.Limits.MaxSteps', 0), '{}', 1, 1, 1)`)
	if err != nil {
		t.Fatal(err)
	}
	var start int64
	if err := tx.QueryRow(`SELECT COALESCE(max(seq),0)+1 FROM transitions WHERE run_id='run-unrelated'`).Scan(&start); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 10000; i++ {
		if _, err := stmt.Exec(start+i, 100000+i, fmt.Sprintf("unrelated-%d", i), closed, i+1, closed); err != nil {
			t.Fatal(err)
		}
	}
	if err := errors.Join(stmt.Close(), tx.Commit()); err != nil {
		t.Fatal(err)
	}
	handoverRun(t, s, "run-1", true, "line note")
	createLineRun(t, s, "run-2", false)
	snap, err := s.Latest(ctx, "run-2")
	if err != nil {
		t.Fatal(err)
	}
	head, err := s.Context(ctx, snap)
	if err != nil || head.Handover == nil || head.Handover.RunID != "run-1" {
		t.Fatalf("initial head with unrelated history = %+v, %v", head, err)
	}
	plan := planOf(t, s, latestHandoverSQL, "run-1")
	if strings.Contains(plan, "SCAN transitions") || !strings.Contains(plan, "SEARCH transitions USING INDEX transitions_performed (run_id=?)") {
		t.Fatalf("the handover lookup no longer searches its run's index: %s", plan)
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
				head := harness.ContextState{AnchorSeq: seq, FirstStep: before.State.Step + 1,
					Handover: &harness.HandoverRef{RunID: before.State.RunID, Seq: seq, Step: before.State.Step}, Rejection: current.Rejection}
				d.do(core.Performed{EffectID: before.State.EffectID}, harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeDone,
					Handover: &harness.Handover{Text: text}}, ContextState: &head})
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
