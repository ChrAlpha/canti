package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

var admittedAt = time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

func submission(t *testing.T, id, text string, freshRun bool) harness.InputSubmission {
	t.Helper()
	run := testRun(id)
	state := fresh(t, id, limits)
	return harness.InputSubmission{InputID: "input-" + id, Authority: harness.AuthorityPrincipal, Text: text,
		Constraints: "read only", ExplicitConstraints: true, Fresh: freshRun, Run: run, Initial: state, Now: admittedAt}
}

func admitOpen(t *testing.T, s *Store, id, text string, freshRun bool) harness.Admission {
	t.Helper()
	got, err := s.Admit(context.Background(), submission(t, id, text, freshRun))
	if err != nil {
		t.Fatalf("Admit(%s) error = %v", id, err)
	}
	return got
}

func admitKind(t *testing.T, s *Store, kind harness.InputDelivery, text string) harness.Admission {
	t.Helper()
	got, err := s.Admit(context.Background(), harness.InputSubmission{Authority: harness.AuthorityPrincipal, Delivery: kind,
		Text: text, Constraints: "read only", ExplicitConstraints: true, Now: admittedAt})
	if err != nil {
		t.Fatalf("Admit(%s) error = %v", kind, err)
	}
	return got
}

func TestAdmitOpensOneRunWithItsInputAtTheFirstLogPosition(t *testing.T) {
	s := open(t, tempPath(t))
	got := admitOpen(t, s, "run-1", "Do the task.", true)
	if got.Disposition != harness.DispositionOpened || got.Run == nil || got.Replayed {
		t.Fatalf("Admit = %+v", got)
	}
	if got.Input.Pos != 1 || got.Run.InputPos != 1 || !got.Run.Fresh || !got.Input.Fresh {
		t.Fatalf("opened input = %+v, run = %+v", got.Input, got.Run)
	}
	if got.Run.Goal != "Do the task." || got.Run.InputID != "input-run-1" || !got.Run.Created.Equal(admittedAt) {
		t.Fatalf("run = %+v", got.Run)
	}
	stored, err := s.Run(context.Background(), got.Run.ID)
	if err != nil || stored.InputPos != 1 {
		t.Fatalf("stored run = %+v, %v", stored, err)
	}
	last, err := s.LastPos(context.Background())
	if err != nil || last != 1 {
		t.Fatalf("last position = %d, %v", last, err)
	}
}

func TestAdmitReplaysAnInputAndConflictsWithOtherContent(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := admitOpen(t, s, "run-1", "Do the task.", true)
	replay := submission(t, "other-id", "Do the task.", true)
	replay.InputID = first.Input.InputID
	again, err := s.Admit(ctx, replay)
	if err != nil || !again.Replayed || again.Run == nil || again.Run.ID != first.Run.ID || again.Input.Pos != first.Input.Pos {
		t.Fatalf("replay = %+v, %v", again, err)
	}
	changed := submission(t, "changed-id", "Another task.", true)
	changed.InputID = first.Input.InputID
	if _, err := s.Admit(ctx, changed); !errors.Is(err, harness.ErrInputConflict) {
		t.Fatalf("conflict = %v", err)
	}
	forced := submission(t, "forced-id", "Do the task.", true)
	forced.InputID = first.Input.InputID
	forced.Constraints = "another rule"
	if _, err := s.Admit(ctx, forced); !errors.Is(err, harness.ErrInputConflict) {
		t.Fatalf("constraint conflict = %v", err)
	}
}

func TestAdmitObservationNeverTouchesRuns(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "Do the task.", true)
	before := latestOf(t, s, run.Run.ID)
	obs, err := s.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityObservation, Text: "the build is green",
		Source: "ci", Subject: "build", InputID: "obs-1", Now: admittedAt})
	if err != nil {
		t.Fatal(err)
	}
	if obs.Disposition != "" || obs.Run != nil || obs.Input.RunID != "" || obs.Input.Pos != 2 {
		t.Fatalf("observation = %+v", obs)
	}
	if after := latestOf(t, s, run.Run.ID); after != before {
		t.Fatalf("observation changed the run: %+v", after)
	}
	got, err := s.Observations(ctx)
	if err != nil || len(got) != 1 || got[0].Source != "ci" || got[0].Subject != "build" || got[0].Text != "the build is green" {
		t.Fatalf("observations = %+v, %v", got, err)
	}
	if _, err := s.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityObservation, Text: "newer",
		Source: "ci", Subject: "build", Now: admittedAt}); err != nil {
		t.Fatal(err)
	}
	got, err = s.Observations(ctx)
	if err != nil || len(got) != 1 || got[0].Text != "newer" || got[0].Pos != 3 {
		t.Fatalf("latest observation = %+v, %v", got, err)
	}
}

func TestAdmitWhileWorkingNeedsADelivery(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "Do the task.", true)
	sub := submission(t, "steer-id", "Try the tests first.", false)
	sub.Run, sub.Initial = harness.Run{}, core.State{}
	if _, err := s.Admit(ctx, sub); !errors.Is(err, harness.ErrDeliveryRequired) {
		t.Fatalf("principal without delivery = %v", err)
	}
	steer := admitKind(t, s, harness.DeliverySteer, "Try the tests first.")
	if steer.Disposition != harness.DispositionSteer || steer.Input.RunID != run.Run.ID || steer.Input.Pos != 2 {
		t.Fatalf("steer = %+v", steer)
	}
	follow := admitKind(t, s, harness.DeliveryFollowUp, "Then report.")
	if follow.Disposition != harness.DispositionQueued || follow.Input.RunID != "" || follow.Input.Pos != 3 {
		t.Fatalf("follow-up = %+v", follow)
	}
	queued, err := s.Queued(ctx)
	if err != nil || queued.Pos != 3 || queued.Text != "Then report." {
		t.Fatalf("queued = %+v, %v", queued, err)
	}
	steers, err := s.Steers(ctx, run.Run.ID)
	if err != nil || len(steers) != 1 || steers[0].Text != "Try the tests first." || steers[0].BoundSeq != 0 {
		t.Fatalf("steers = %+v, %v", steers, err)
	}
}

func TestAdmitBoundsTheSteersOfOneRun(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	admitOpen(t, s, "run-1", "Do the task.", true)
	limit := harness.DefaultPromptLimits().Instructions
	admitKind(t, s, harness.DeliverySteer, strings.Repeat("x", limit))
	if _, err := s.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityPrincipal, Delivery: harness.DeliverySteer,
		Text: "y", Constraints: "read only", ExplicitConstraints: true, Now: admittedAt}); !errors.Is(err, harness.ErrInputSpec) {
		t.Fatalf("over-budget steer = %v", err)
	}
	freshSteer := submission(t, "fresh-id", "A new line.", true)
	freshSteer.Delivery = harness.DeliverySteer
	if _, err := s.Admit(ctx, freshSteer); !errors.Is(err, harness.ErrInputSpec) {
		t.Fatalf("fresh steer = %v", err)
	}
}

func TestHeldInputsBlockANewRunUntilWithdrawn(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "Do the task.", true)
	steer := admitKind(t, s, harness.DeliverySteer, "Try the tests first.")
	follow := admitKind(t, s, harness.DeliveryFollowUp, "Then report.")
	d := newDriver(t, s, harness.Snapshot{State: latestOf(t, s, run.Run.ID).State})
	d.do(core.StopRequested{Reason: core.StopCancelled}, harness.Facts{})
	counts, err := s.InputCounts(ctx, run.Run.ID)
	if err != nil || counts.Pending != 1 || counts.Queued != 1 || counts.Held != 2 {
		t.Fatalf("counts = %+v, %v", counts, err)
	}
	next := submission(t, "run-2", "A new task.", false)
	if _, err := s.Admit(ctx, next); !errors.Is(err, harness.ErrHeld) {
		t.Fatalf("new run beside held inputs = %v", err)
	}
	withdrawn, err := s.Withdraw(ctx, "")
	if err != nil || len(withdrawn) != 2 || withdrawn[0].Pos != steer.Input.Pos || withdrawn[1].Pos != follow.Input.Pos {
		t.Fatalf("withdraw all = %+v, %v", withdrawn, err)
	}
	if withdrawn[0].Text != "Try the tests first." || withdrawn[1].Text != "Then report." {
		t.Fatalf("withdrawn texts = %+v", withdrawn)
	}
	if again, err := s.Withdraw(ctx, ""); err != nil || len(again) != 0 {
		t.Fatalf("second withdraw = %+v, %v", again, err)
	}
	if _, err := s.Admit(ctx, next); err != nil {
		t.Fatalf("new run after withdrawal = %v", err)
	}
	if _, err := s.Withdraw(ctx, "input-run-1"); !errors.Is(err, harness.ErrNotWithdrawable) {
		t.Fatalf("withdraw an opened input = %v", err)
	}
	if got, err := s.Withdraw(ctx, "input-steer?"); err == nil {
		t.Fatalf("withdraw an unknown input = %+v", got)
	}
}

func TestAdmitRefusesBesideAnAbandonedUnknownEffect(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "Do the task.", true)
	d := newDriver(t, s, harness.Snapshot{State: latestOf(t, s, run.Run.ID).State})
	d.decide(execBody("true"), harness.Usage{})
	d.do(core.OutcomeUnknown{EffectID: d.snap.State.EffectID}, harness.Facts{Failure: "lost job"})
	d.do(core.Resolved{EffectID: d.snap.State.EffectID, Execution: core.ExecutionUnknown}, harness.Facts{Observation: observation(core.ExecutionUnknown, 0)})
	if snap := latestOf(t, s, run.Run.ID); snap.State.Phase != core.PhaseStopped || snap.State.Reason != core.StopUnresolved {
		t.Fatalf("abandoned state = %+v", snap.State)
	}
	next := submission(t, "run-2", "A new task.", false)
	if _, err := s.Admit(ctx, next); err == nil || !strings.Contains(err.Error(), "unknown effect") {
		t.Fatalf("admission beside an abandoned unknown effect = %v", err)
	}
}

func TestWithdrawByNameIsIdempotentAndReturnsTheText(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "Do the task.", true)
	follow, err := s.Admit(ctx, harness.InputSubmission{InputID: "follow-me", Authority: harness.AuthorityPrincipal,
		Delivery: harness.DeliveryFollowUp, Text: "Then report.", Constraints: "read only", ExplicitConstraints: true, Now: admittedAt})
	if err != nil {
		t.Fatal(err)
	}
	_ = run
	got, err := s.Withdraw(ctx, "follow-me")
	if err != nil || len(got) != 1 || got[0].Disposition != harness.DispositionWithdrawn || got[0].Pos != follow.Input.Pos {
		t.Fatalf("withdraw = %+v, %v", got, err)
	}
	again, err := s.Withdraw(ctx, "follow-me")
	if err != nil || len(again) != 1 || again[0].Text != "Then report." {
		t.Fatalf("idempotent withdraw = %+v, %v", again, err)
	}
}

func TestReturnGuardRefusesAnUnboundSteerAndSupersedesTheDecision(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "Do the task.", true)
	admitKind(t, s, harness.DeliverySteer, "Try the tests first.")
	d := newDriver(t, s, harness.Snapshot{State: latestOf(t, s, run.Run.ID).State})
	d.freeze()
	d.send()
	d.save(execBody("printf ok"), harness.Usage{})
	event := core.ResponseLoaded{Step: d.snap.State.Step, Attempt: d.snap.State.Attempt, Body: execBody("printf ok")}
	if err := d.try(event, acceptance(d.snap.State, event)); !errors.Is(err, harness.ErrInputsPending) {
		t.Fatalf("ResponseLoaded with an unbound steer = %v", err)
	}
	d.do(core.InputsBound{Count: 1}, harness.Facts{})
	if d.snap.State.Inputs != 1 || d.snap.State.Seen != 0 {
		t.Fatalf("binding = %+v", d.snap.State)
	}
	facts := harness.Facts{Supersede: &harness.Superseded{Instruction: 1, Kind: core.Exec, Purpose: "check"}}
	d.do(event, facts)
	if d.snap.State.Phase != core.PhaseIdle || d.snap.State.Step != 2 || d.snap.State.Seen != 0 {
		t.Fatalf("superseded = %+v", d.snap.State)
	}
	record, err := s.CompletionAtStep(ctx, run.Run.ID, 1)
	if err != nil || record.Superseded == nil || record.Superseded.Instruction != 1 || record.Superseded.Kind != core.Exec || record.Superseded.Purpose != "check" {
		t.Fatalf("superseded record = %+v, %v", record, err)
	}
	receipts, err := s.Receipts(ctx, run.Run.ID, 0, 1)
	if err != nil || len(receipts) != 1 || receipts[0].Status != "superseded" || receipts[0].SupersededBy != 1 || receipts[0].DecisionKind != core.Exec {
		t.Fatalf("superseded receipt = %+v, %v", receipts, err)
	}
	transitions, err := s.Transitions(ctx, run.Run.ID, 0, 10)
	if err != nil || len(transitions) != 5 {
		t.Fatalf("transitions = %d, %v", len(transitions), err)
	}
}

func TestReturnGuardSeesASteerAdmittedAfterTheBind(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "Do the task.", true)
	admitKind(t, s, harness.DeliverySteer, "First instruction.")
	d := newDriver(t, s, harness.Snapshot{State: latestOf(t, s, run.Run.ID).State})
	d.freeze()
	d.send()
	d.save(execBody("printf ok"), harness.Usage{})
	d.do(core.InputsBound{Count: 1}, harness.Facts{})
	// A steer that arrives after the bind is still unbound: the return guard
	// refuses the decision, and the second bind supersedes it.
	admitKind(t, s, harness.DeliverySteer, "Second instruction.")
	event := core.ResponseLoaded{Step: d.snap.State.Step, Attempt: d.snap.State.Attempt, Body: execBody("printf ok")}
	if err := d.try(event, acceptance(d.snap.State, event)); !errors.Is(err, harness.ErrInputsPending) {
		t.Fatalf("ResponseLoaded after a late steer = %v", err)
	}
	d.do(core.InputsBound{Count: 2}, harness.Facts{})
	d.do(event, harness.Facts{Supersede: &harness.Superseded{Instruction: 1, Kind: core.Exec}})
	if d.snap.State.Phase != core.PhaseIdle || d.snap.State.Step != 2 || d.snap.State.Inputs != 2 {
		t.Fatalf("superseded state = %+v", d.snap.State)
	}
	steers, err := s.Steers(ctx, run.Run.ID)
	if err != nil || len(steers) != 2 || steers[1].BoundSeq != d.commits[len(d.commits)-2].Seq {
		t.Fatalf("late steer binding = %+v, %v", steers, err)
	}
}

func TestAgentLogPositionsAreStrictlyIncreasingAcrossInputsAndTransitions(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "Do the task.", true)
	admitKind(t, s, harness.DeliverySteer, "Try the tests first.")
	d := newDriver(t, s, harness.Snapshot{State: latestOf(t, s, run.Run.ID).State})
	d.do(core.InputsBound{Count: 1}, harness.Facts{})
	d.decide([]byte(`{"kind":"return","payload":"done"}`), harness.Usage{})
	entries, err := s.Log(ctx, 0, 100)
	if err != nil || len(entries) < 6 {
		t.Fatalf("log = %+v, %v", entries, err)
	}
	var last int64
	for _, e := range entries {
		if e.Pos <= last {
			t.Fatalf("log position %d does not follow %d", e.Pos, last)
		}
		last = e.Pos
	}
	end, err := s.LastPos(ctx)
	if err != nil || end != last {
		t.Fatalf("last position = %d, log ends at %d, %v", end, last, err)
	}
}

func TestSteersCarryTheBindingTransition(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "Do the task.", true)
	admitKind(t, s, harness.DeliverySteer, "Try the tests first.")
	d := newDriver(t, s, harness.Snapshot{State: latestOf(t, s, run.Run.ID).State})
	d.do(core.InputsBound{Count: 1}, harness.Facts{})
	steers, err := s.Steers(ctx, run.Run.ID)
	if err != nil || len(steers) != 1 || steers[0].BoundSeq != d.snap.Seq || steers[0].BoundStep != 1 {
		t.Fatalf("steers = %+v, %v", steers, err)
	}
}

func TestOpenFollowUpAfterAReturn(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "Do the task.", true)
	follow := admitKind(t, s, harness.DeliveryFollowUp, "Then report.")
	d := newDriver(t, s, harness.Snapshot{State: latestOf(t, s, run.Run.ID).State})
	d.decide([]byte(`{"kind":"return","payload":"done"}`), harness.Usage{})
	next := submission(t, "run-2", "Then report.", false)
	next.Run.Goal, next.Run.Constraints = "ignored", "resolved by the host"
	got, err := s.OpenFollowUp(ctx, follow.Input, next.Run, next.Initial)
	if err != nil || got.Disposition != harness.DispositionOpened || got.Run == nil {
		t.Fatalf("open follow-up = %+v, %v", got, err)
	}
	if got.Run.InputPos != follow.Input.Pos || got.Run.Goal != "Then report." || got.Run.Constraints != "resolved by the host" || got.Run.Fresh {
		t.Fatalf("follow-up run = %+v", got.Run)
	}
	stored, err := s.Input(ctx, "input-run-1")
	if err != nil {
		t.Fatal(err)
	}
	_ = stored
	queuedInput, err := s.Input(ctx, "input-run-1")
	if err != nil || queuedInput.Disposition == harness.DispositionQueued {
		t.Fatalf("opened input = %+v, %v", queuedInput, err)
	}
	if again, err := s.OpenFollowUp(ctx, follow.Input, next.Run, next.Initial); err == nil {
		t.Fatalf("second open = %+v", again)
	}
	previous, more, err := s.PreviousRuns(ctx, got.Run.ID)
	if err != nil || more != 0 || len(previous) != 1 || previous[0].Run.ID != run.Run.ID || !previous[0].Closed {
		t.Fatalf("previous runs = %+v, %d, %v", previous, more, err)
	}
}

func TestPreviousRunsStartAtTheLastFreshRun(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := testRun("run-1")
	first.Fresh = true
	if err := s.CreateRun(ctx, first, fresh(t, "run-1", limits)); err != nil {
		t.Fatal(err)
	}
	stopRun(t, s, first)
	second := testRun("run-2")
	if err := s.CreateRun(ctx, second, fresh(t, "run-2", limits)); err != nil {
		t.Fatal(err)
	}
	stopRun(t, s, second)
	third := testRun("run-3")
	third.Fresh = true
	if err := s.CreateRun(ctx, third, fresh(t, "run-3", limits)); err != nil {
		t.Fatal(err)
	}
	prior, more, err := s.PreviousRuns(ctx, "run-3")
	if err != nil || more != 0 || len(prior) != 0 {
		t.Fatalf("previous after a fresh run = %+v, %d, %v", prior, more, err)
	}
	stopRun(t, s, third)
	fourth := testRun("run-4")
	if err := s.CreateRun(ctx, fourth, fresh(t, "run-4", limits)); err != nil {
		t.Fatal(err)
	}
	prior, more, err = s.PreviousRuns(ctx, "run-4")
	if err != nil || more != 0 || len(prior) != 1 || prior[0].Run.ID != "run-3" {
		t.Fatalf("previous of run-4 = %+v, %d, %v", prior, more, err)
	}
}

func TestInputsBoundAcceptsAnAdmittedPrefix(t *testing.T) {
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "Do the task.", true)
	admitKind(t, s, harness.DeliverySteer, "First instruction.")
	admitKind(t, s, harness.DeliverySteer, "Second instruction.")
	d := newDriver(t, s, harness.Snapshot{State: latestOf(t, s, run.Run.ID).State})
	// A bind that read one steer before the second arrived commits the
	// admitted prefix; the store must not demand the exact current count.
	d.do(core.InputsBound{Count: 1}, harness.Facts{})
	if d.snap.State.Inputs != 1 {
		t.Fatalf("prefix binding = %+v", d.snap.State)
	}
	if err := d.try(core.InputsBound{Count: 1}, harness.Facts{}); err == nil {
		t.Fatal("a non-increasing binding was accepted")
	}
	d.do(core.InputsBound{Count: 2}, harness.Facts{})
	if d.snap.State.Inputs != 2 {
		t.Fatalf("later binding = %+v", d.snap.State)
	}
	if err := d.try(core.InputsBound{Count: 3}, harness.Facts{}); !errors.Is(err, harness.ErrFacts) {
		t.Fatalf("binding beyond the admitted steers = %v", err)
	}
}

func TestAbsentInputIDsAreNullAndDistinct(t *testing.T) {
	s := open(t, tempPath(t))
	ctx := context.Background()
	admitOpen(t, s, "run-1", "Do the task.", true)
	for _, text := range []string{"first", "second"} {
		if _, err := s.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityPrincipal,
			Delivery: harness.DeliveryFollowUp, Text: text, Now: admittedAt}); err != nil {
			t.Fatal(err)
		}
	}
	var absent int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM inputs WHERE input_id IS NULL`).Scan(&absent); err != nil {
		t.Fatal(err)
	}
	if absent != 2 {
		t.Fatalf("absent input IDs stored non-NULL: %d", absent)
	}
	if _, err := s.Input(ctx, ""); err == nil {
		t.Fatal("empty input ID unexpectedly named an input")
	}
}

func TestPreviousRunsNameDeliveredAmendments(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "Do the task.", true)
	for _, text := range []string{"One.", "Two.", "Three.", "Four."} {
		admitKind(t, s, harness.DeliverySteer, text)
	}
	d := newDriver(t, s, latestOf(t, s, run.Run.ID))
	d.do(core.InputsBound{Count: 4}, harness.Facts{})
	if _, err := s.db.Exec(`UPDATE inputs SET delivered_pos = pos + 10 WHERE pos > 1`); err != nil {
		t.Fatal(err)
	}
	stopRun(t, s, *run.Run)
	createRun(t, s, "run-2", limits)
	previous, more, err := s.PreviousRuns(ctx, "run-2")
	if err != nil || more != 0 || len(previous) != 1 {
		t.Fatalf("previous runs = %+v, %d, %v", previous, more, err)
	}
	if got := previous[0]; got.AmendmentCount != 4 || len(got.Amendments) != 3 ||
		got.Amendments[0] != 3 || got.Amendments[1] != 4 || got.Amendments[2] != 5 {
		t.Fatalf("previous amendments = %+v", got)
	}
}
