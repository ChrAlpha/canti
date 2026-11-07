package sqlite

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

// replay folds core.State.Next over got from initial and fails unless every
// transition extends the previous one and reproduces its stored To.
func replay(t testing.TB, initial core.State, got []harness.Transition) core.State {
	t.Helper()
	state := initial
	for i, tr := range got {
		if tr.Seq != int64(i)+1 || tr.From != state {
			t.Fatalf("transition %d = seq %d from %#v; want seq %d from %#v", i, tr.Seq, tr.From, i+1, state)
		}
		next, err := state.Next(tr.Event)
		if err != nil {
			t.Fatalf("replay seq %d: %v", tr.Seq, err)
		}
		if next != tr.To {
			t.Fatalf("replay seq %d = %#v, stored To %#v", tr.Seq, next, tr.To)
		}
		state = next
	}
	return state
}

func TestOfflineRunRecoversAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	initial := d.snap.State

	d.step("printf ok", harness.Usage{InputTokens: 10, OutputTokens: 5}, 42)

	d.decide(execBody("make build"), harness.Usage{InputTokens: 12, OutputTokens: 6})
	d.call(harness.Runner.Bind)
	d.call(harness.Runner.Start)
	d.do(core.OutcomeUnknown{EffectID: d.snap.State.EffectID}, harness.Facts{Failure: "backend lost the job"})
	d.do(core.Resolved{EffectID: d.snap.State.EffectID, Execution: core.ExecutionExited}, harness.Facts{Observation: observation(core.ExecutionExited, 7)})

	d.freeze()
	d.send()
	d.do(core.ResponseIncomplete{Step: 3, Attempt: 1}, harness.Facts{
		Response: &harness.Response{Body: []byte(`{"kind":`), Usage: harness.Usage{InputTokens: 3, OutputTokens: 1}},
		Failure:  "connection reset",
	})
	d.send()
	done := []byte(`{"kind":"return","payload":"done"}`)
	d.save(done, harness.Usage{InputTokens: 20, OutputTokens: 8})
	d.load(done)
	if d.snap.State.Phase != core.PhaseReturned || d.snap.State.Step != 3 {
		t.Fatalf("final state = %#v, want returned at step 3", d.snap.State)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open(t, path)
	if got := latestOf(t, s, "run-1"); got != d.snap {
		t.Fatalf("Latest() = %#v, want %#v", got, d.snap)
	}
	stored := history(t, s, "run-1")
	if !reflect.DeepEqual(stored, d.commits) {
		t.Fatalf("Transitions() = %#v, want the committed %#v", stored, d.commits)
	}
	if final := replay(t, initial, stored); final != d.snap.State {
		t.Fatalf("replayed state = %#v, want %#v", final, d.snap.State)
	}
	var paged []harness.Transition
	for after := int64(0); ; {
		page, err := s.Transitions(ctx, "run-1", after, 4)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		paged = append(paged, page...)
		after = page[len(page)-1].Seq
	}
	if !reflect.DeepEqual(paged, stored) {
		t.Fatal("paged Transitions differ from the full history")
	}

	var requests, responses int
	for _, tr := range d.commits {
		switch tr.Event.(type) {
		case core.RequestFrozen:
			requests++
			got, err := s.Request(ctx, "run-1", tr.To.Step, tr.To.RequestRev)
			if err != nil || !reflect.DeepEqual(got, *tr.Facts.Request) {
				t.Fatalf("Request(%d, %d) = %#v, %v; want %#v", tr.To.Step, tr.To.RequestRev, got, err, *tr.Facts.Request)
			}
		case core.ResponseSaved:
			responses++
			got, err := s.Response(ctx, "run-1", tr.To.Step, tr.To.Attempt)
			if err != nil || !reflect.DeepEqual(got, *tr.Facts.Response) {
				t.Fatalf("Response(%d, %d) = %#v, %v; want %#v", tr.To.Step, tr.To.Attempt, got, err, *tr.Facts.Response)
			}
		}
	}
	if requests != 3 || responses != 3 {
		t.Fatalf("checked %d requests and %d responses, want 3 each", requests, responses)
	}
	if _, err := s.Request(ctx, "run-1", 3, 2); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("Request(unfrozen revision) error = %v, want ErrNotFound", err)
	}
	if _, err := s.Response(ctx, "run-1", 3, 1); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("Response(incomplete attempt) error = %v, want ErrNotFound", err)
	}

	want := []harness.StepRecord{
		{Seq: 7, Step: 1, Command: "printf ok", Observation: *observation(core.ExecutionExited, 42)},
		{Seq: 15, Step: 2, Command: "make build", Observation: *observation(core.ExecutionExited, 7), Resolved: true},
	}
	for limit, records := range map[int][]harness.StepRecord{1: want[1:], 2: want, 10: want} {
		got, err := s.Recent(ctx, "run-1", 0, limit)
		if err != nil || !reflect.DeepEqual(got, records) {
			t.Fatalf("Recent(0, %d) = %#v, %v; want %#v", limit, got, err, records)
		}
	}
	totals := harness.Totals{Usage: harness.Usage{InputTokens: 45, OutputTokens: 20}, OutputBytes: 49, Steps: 2}
	if got := totalsOf(t, s, "run-1"); got != totals {
		t.Fatalf("Totals() = %#v, want %#v", got, totals)
	}
}

// completed returns the steps that commits observed or resolved, oldest
// first, derived independently of the store.
func completed(commits []harness.Transition) []harness.StepRecord {
	var out []harness.StepRecord
	for _, tr := range commits {
		switch tr.Event.(type) {
		case core.Observed, core.Resolved:
			_, resolved := tr.Event.(core.Resolved)
			out = append(out, harness.StepRecord{Seq: tr.Seq, Step: tr.From.Step, Command: tr.From.Decision.Payload, Observation: *tr.Facts.Observation, Resolved: resolved})
		case core.Performed:
			out = append(out, harness.StepRecord{Seq: tr.Seq, Step: tr.From.Step, Command: tr.From.Decision.Payload, Outcome: tr.Facts.Outcome, Window: tr.Facts.Window})
		}
	}
	return out
}

// TestCompletionReadsOneStepBySequence requires that Completion returns the
// step a transition completed, by its sequence alone, also from a read-only
// store without the index of performed steps, and refuses a sequence that
// holds no transition, one that completes no step, and a step of a shape
// its version never records, the last two also as harness.ErrIncompatible.
func TestCompletionReadsOneStepBySequence(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s := open(t, path)
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.step("make", harness.Usage{}, 1)
	d.decide(execBody("canti:lint"), harness.Usage{})
	d.perform()
	seqs := map[string]int64{}
	for _, tr := range d.commits {
		seqs[tr.Event.Kind()] = tr.Seq
	}
	if r, err := s.Completion(ctx, "run-1", seqs["observed"]); err != nil || r.Step != 1 || r.Command != "make" || r.Observation.Execution != core.ExecutionExited || r.Outcome != nil {
		t.Fatalf("Completion of the observed step = %+v, %v", r, err)
	}
	if r, err := s.Completion(ctx, "run-1", seqs["performed"]); err != nil || r.Step != 2 || r.Outcome == nil || r.Outcome.Status != harness.OutcomeFailed || r.Seq != seqs["performed"] {
		t.Fatalf("Completion of the performed step = %+v, %v", r, err)
	}
	for _, tc := range []struct {
		run  string
		seq  int64
		want error
	}{
		{"run-1", seqs["request_frozen"], ErrIncompatible},
		{"run-1", seqs["performed"] + 1, harness.ErrNotFound},
		{"run-2", seqs["performed"], harness.ErrNotFound},
	} {
		if _, err := s.Completion(ctx, tc.run, tc.seq); !errors.Is(err, tc.want) || tc.want == ErrIncompatible && !errors.Is(err, harness.ErrIncompatible) {
			t.Errorf("Completion(%s, %d) = %v, want %v", tc.run, tc.seq, err, tc.want)
		}
	}
	if _, err := s.db.Exec(`DROP INDEX transitions_performed`); err != nil {
		t.Fatal(err)
	}
	if r, err := readOnly(t, path).Completion(ctx, "run-1", seqs["performed"]); err != nil || r.Outcome == nil {
		t.Fatalf("read-only Completion without the index = %+v, %v", r, err)
	}
	if _, err := s.db.Exec(`UPDATE transitions SET facts = json_set(facts, '$.Outcome.Status', 'bogus') WHERE kind = 'performed'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Completion(ctx, "run-1", seqs["performed"]); !errors.Is(err, ErrIncompatible) || !errors.Is(err, harness.ErrIncompatible) {
		t.Fatalf("Completion of a corrupt step = %v, want ErrIncompatible of both packages", err)
	}
}

func TestRecentPagesBackwardBySequence(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", core.Limits{MaxSteps: 5, MaxAttempts: 2}))
	d.step("make", harness.Usage{}, 1)
	d.decide(execBody("make test"), harness.Usage{})
	d.call(harness.Runner.Bind)
	d.call(harness.Runner.Start)
	d.do(core.OutcomeUnknown{EffectID: d.snap.State.EffectID}, harness.Facts{Failure: "backend lost the job"})
	d.do(core.Resolved{EffectID: d.snap.State.EffectID, Execution: core.ExecutionExited}, harness.Facts{Observation: observation(core.ExecutionExited, 2)})
	d.decide([]byte("not a decision"), harness.Usage{})
	d.step("make vet", harness.Usage{}, 3)
	d.decide(execBody("canti:lint"), harness.Usage{})
	d.perform()
	d.step("make install", harness.Usage{}, 5)
	if s := d.snap.State; s.Phase != core.PhaseStopped || s.Reason != core.StopStepsExhausted {
		t.Fatalf("final state = %#v, want steps_exhausted", s)
	}
	all := completed(d.commits)
	if len(all) != 5 || !all[1].Resolved || all[4].Step != 5 {
		t.Fatalf("completed steps = %#v", all)
	}

	below := func(before int64, limit int) []harness.StepRecord {
		rest := all
		if before > 0 {
			rest = slices.DeleteFunc(slices.Clone(all), func(r harness.StepRecord) bool { return r.Seq >= before })
		}
		return rest[len(rest)-max(0, min(limit, len(rest))):]
	}
	for limit := -1; limit <= len(all)+1; limit++ {
		for before := int64(-1); before <= d.snap.Seq+1; before++ {
			got, err := s.Recent(ctx, "run-1", before, limit)
			if want := below(before, limit); err != nil || len(got) != len(want) || len(got) > 0 && !reflect.DeepEqual(got, want) {
				t.Fatalf("Recent(%d, %d) = %#v, %v; want %#v", before, limit, got, err, want)
			}
		}
		if limit < 1 {
			continue
		}
		var paged []harness.StepRecord
		pages := 0
		for before := int64(0); ; pages++ {
			page, err := s.Recent(ctx, "run-1", before, limit)
			if err != nil {
				t.Fatal(err)
			}
			if len(page) == 0 {
				break
			}
			paged, before = append(page, paged...), page[0].Seq
		}
		if !reflect.DeepEqual(paged, all) || pages != (len(all)+limit-1)/limit {
			t.Fatalf("%d pages of %d = %#v, want %#v", pages, limit, paged, all)
		}
	}
}

func TestReadsRefuseIncompatibleRecords(t *testing.T) {
	ctx := context.Background()
	corruptions := []struct {
		name, update string
		reads        []string
	}{
		{"unknown kind", `UPDATE transitions SET kind = 'teleported' WHERE run_id = ? AND seq = ?`, []string{"Latest", "Transitions", "Commit"}},
		{"other semantics", `UPDATE transitions SET semantics = semantics + 1 WHERE run_id = ? AND seq = ?`, []string{"Latest", "Transitions", "Recent", "Commit"}},
		{"unknown field", `UPDATE transitions SET to_state = json_insert(to_state, '$.Extra', 1) WHERE run_id = ? AND seq = ?`, []string{"Latest", "Transitions", "Commit"}},
		{"unknown facts field", `UPDATE transitions SET facts = json_insert(facts, '$.Extra', 1) WHERE run_id = ? AND seq = ?`, []string{"Transitions", "Recent"}},
	}
	for _, c := range corruptions {
		t.Run(c.name, func(t *testing.T) {
			s := open(t, tempPath(t))
			d := newDriver(t, s, createRun(t, s, "run-1", limits))
			d.step("ls", harness.Usage{}, 1)
			if _, err := s.db.Exec(c.update, "run-1", d.snap.Seq); err != nil {
				t.Fatal(err)
			}
			reads := map[string]func() error{
				"Latest":      func() error { _, err := s.Latest(ctx, "run-1"); return err },
				"Transitions": func() error { _, err := s.Transitions(ctx, "run-1", 0, 100); return err },
				"Recent":      func() error { _, err := s.Recent(ctx, "run-1", 0, 5); return err },
				"Commit":      func() error { return d.try(core.RequestFrozen{}, harness.Facts{Request: requestFor(d.snap.State)}) },
			}
			for _, name := range c.reads {
				if err := reads[name](); !errors.Is(err, ErrIncompatible) {
					t.Errorf("%s error = %v, want ErrIncompatible", name, err)
				}
			}
		})
	}

	t.Run("facts of other semantics", func(t *testing.T) {
		s := open(t, tempPath(t))
		d := newDriver(t, s, createRun(t, s, "run-1", limits))
		d.decide(execBody("ls"), harness.Usage{})
		if _, err := s.db.Exec(`UPDATE transitions SET semantics = 0`); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Request(ctx, "run-1", 1, 1); !errors.Is(err, ErrIncompatible) {
			t.Errorf("Request error = %v, want ErrIncompatible", err)
		}
		if _, err := s.Response(ctx, "run-1", 1, 1); !errors.Is(err, ErrIncompatible) {
			t.Errorf("Response error = %v, want ErrIncompatible", err)
		}
	})

	t.Run("initial state of other semantics", func(t *testing.T) {
		s := open(t, tempPath(t))
		createRun(t, s, "run-1", limits)
		if _, err := s.db.Exec(`UPDATE runs SET semantics = semantics + 1`); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Latest(ctx, "run-1"); !errors.Is(err, ErrIncompatible) {
			t.Errorf("Latest error = %v, want ErrIncompatible", err)
		}
	})
}

func TestCommitRefusesTransitionsItCannotReproduce(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.decide(execBody("cat binary"), harness.Usage{})
	bound := core.JobBound{EffectID: d.snap.State.EffectID, JobID: "job-1"}
	pointer := derive(t, d.snap, bound, harness.Facts{})
	pointer.Event = &bound
	for name, tr := range map[string]harness.Transition{
		"pointer event":        pointer,
		"invalid UTF-8 in IDs": derive(t, d.snap, core.JobBound{EffectID: d.snap.State.EffectID, JobID: "job-\xff"}, harness.Facts{}),
		"no event":             {Seq: d.snap.Seq + 1, From: d.snap.State, To: d.snap.State},
	} {
		if err := s.Commit(ctx, tr); err == nil {
			t.Errorf("Commit(%s) succeeded, want refusal", name)
		}
	}
	unchanged(t, s, "run-1", d.commits, harness.Totals{})

	run := testRun("run-2")
	run.Goal = "\xff"
	if err := s.CreateRun(ctx, run, fresh(t, "run-2", limits)); err == nil {
		t.Fatal("CreateRun(invalid UTF-8 goal) succeeded, want refusal")
	}
}

func TestCommitStoresFactTextAsValidUTF8(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.decide(execBody("cat binary"), harness.Usage{})
	d.call(harness.Runner.Bind)
	d.call(harness.Runner.Start)
	raw := observation(core.ExecutionExited, 1)
	raw.Preview = "ELF\x7f\xff truncated \xe2\x82"
	d.do(core.Observed{EffectID: d.snap.State.EffectID, Execution: core.ExecutionExited}, harness.Facts{Observation: raw})
	last := d.commits[len(d.commits)-1]
	if err := s.Commit(ctx, last); err != nil {
		t.Fatalf("identical retry error = %v", err)
	}

	stored := history(t, s, "run-1")
	if len(stored) != len(d.commits) {
		t.Fatalf("stored %d transitions, want %d", len(stored), len(d.commits))
	}
	if got, want := stored[len(stored)-1].Facts.Observation.Preview, "ELF\x7f\ufffd truncated \ufffd\ufffd"; got != want {
		t.Fatalf("stored preview = %q, want %q", got, want)
	}
	replay(t, fresh(t, "run-1", limits), stored)
}

// TestAcceptedIsRecordedLegacyOrRefused requires that Accepted returns the
// action of the acceptance of an exec decision, reads an acceptance of a
// version before harness.ActionSemantics without an action as legacy, and
// fails closed for one of a later version without a valid action and for
// one of an earlier version that holds an action.
func TestAcceptedIsRecordedLegacyOrRefused(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, mutation string
		want           harness.Provenance
	}{
		{"recorded", ``, harness.ProvenanceRecorded},
		{"legacy", `UPDATE transitions SET semantics = 3, facts = json_remove(facts, '$.Action') WHERE kind = 'response_loaded'`, harness.ProvenanceLegacy},
		{"current without action", `UPDATE transitions SET facts = json_remove(facts, '$.Action') WHERE kind = 'response_loaded'`, ""},
		{"current with another recovery", `UPDATE transitions SET facts = json_set(facts, '$.Action.Recovery', 'local_change') WHERE kind = 'response_loaded'`, ""},
		{"current with an unknown field", `UPDATE transitions SET facts = json_set(facts, '$.Action.Extra', 1) WHERE kind = 'response_loaded'`, ""},
		{"legacy with action", `UPDATE transitions SET semantics = 3 WHERE kind = 'response_loaded'`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := open(t, tempPath(t))
			d := newDriver(t, s, createRun(t, s, "run-1", limits))
			d.decide(execBody(`canti:retrieve {"operation":"read"}`), harness.Usage{})
			if tc.mutation != "" {
				if _, err := s.db.Exec(tc.mutation); err != nil {
					t.Fatal(err)
				}
			}
			a, err := s.Accepted(ctx, "run-1", 1, 1)
			if tc.want == "" {
				if !errors.Is(err, ErrIncompatible) {
					t.Fatalf("Accepted = %+v, %v; want ErrIncompatible", a, err)
				}
				if _, err := harness.ActionOf(ctx, s, d.snap.State); !errors.Is(err, ErrIncompatible) {
					t.Fatalf("ActionOf = %v; want ErrIncompatible", err)
				}
				return
			}
			want := harness.Action{Name: harness.ActionRetrieve, Recovery: harness.RecoveryRead, Args: len(harness.RetrievePrefix)}
			if tc.want == harness.ProvenanceLegacy {
				want = harness.Action{Name: harness.ActionLegacy}
			}
			if err != nil || a.Provenance != tc.want || a.Action != want {
				t.Fatalf("Accepted = %+v, %v; want %s %+v", a, err, tc.want, want)
			}
		})
	}
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.decide([]byte("not a decision"), harness.Usage{})
	if _, err := s.Accepted(ctx, "run-1", 1, 1); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("Accepted of a rejected decision = %v, want ErrNotFound", err)
	}
	if _, err := s.Accepted(ctx, "run-1", 1, 2); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("Accepted of an unsent attempt = %v, want ErrNotFound", err)
	}
}

// TestCommitChecksEffectsAgainstTheirAcceptanceInItsTransaction requires
// that the store refuses, writing nothing, a job bound to a local action, a
// performed command, and a performed handover whose window its request does
// not explain.
func TestCommitChecksEffectsAgainstTheirAcceptanceInItsTransaction(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		event         func(core.State) core.Event
		facts         harness.Facts
	}{
		{"job of a local action", "canti:lint", func(s core.State) core.Event { return core.JobBound{EffectID: s.EffectID, JobID: "job-1"} }, harness.Facts{}},
		{"performed command", "make", func(s core.State) core.Event { return core.Performed{EffectID: s.EffectID} },
			harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeFailed, Detail: "x"}}},
		{"unexplained window", "canti:lint", func(s core.State) core.Event { return core.Performed{EffectID: s.EffectID} },
			harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeFailed, Detail: "unknown harness action; the actions are canti:retrieve and canti:handover"},
				Window: &harness.Window{FirstStep: 1, Requested: true, Failures: 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := open(t, tempPath(t))
			d := newDriver(t, s, createRun(t, s, "run-1", limits))
			d.decide(execBody(tc.payload), harness.Usage{})
			if err := d.try(tc.event(d.snap.State), tc.facts); !errors.Is(err, harness.ErrFacts) {
				t.Fatalf("commit = %v, want ErrFacts", err)
			}
			unchanged(t, s, "run-1", d.commits, harness.Totals{Usage: harness.Usage{}})
		})
	}
}

// TestActionOfRefusesAnActionItsDecisionDoesNotMatch requires that a
// recorded action of a valid shape that is no reading of its decision, such
// as a command recorded for a reserved payload or arguments at another
// offset, fails ActionOf closed, so that the loop never binds it.
func TestActionOfRefusesAnActionItsDecisionDoesNotMatch(t *testing.T) {
	ctx := context.Background()
	for name, action := range map[string]string{
		"command for a reserved payload": `{"Name":"command","Recovery":"external_effect"}`,
		"arguments elsewhere":            `{"Name":"retrieve","Recovery":"repeatable_read","Args":3}`,
		"another action's word":          `{"Name":"handover","Recovery":"local_change","Args":15}`,
	} {
		t.Run(name, func(t *testing.T) {
			s := open(t, tempPath(t))
			d := newDriver(t, s, createRun(t, s, "run-1", limits))
			d.decide(execBody(`canti:retrieve {"operation":"read"}`), harness.Usage{})
			if _, err := s.db.Exec(`UPDATE transitions SET facts = json_set(facts, '$.Action', json(?)) WHERE kind = 'response_loaded'`, action); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Accepted(ctx, "run-1", 1, 1); err != nil {
				t.Fatalf("Accepted of a valid shape = %v", err)
			}
			if _, err := harness.ActionOf(ctx, s, d.snap.State); !errors.Is(err, harness.ErrIncompatible) {
				t.Fatalf("ActionOf = %v, want harness.ErrIncompatible", err)
			}
		})
	}
}

// TestRecentRefusesStepsOfAShapeTheirVersionNeverRecords requires that a
// completed step of the current version whose facts take a shape no commit
// writes fails Recent closed, and that the same observation of a legacy
// version reads as recorded.
func TestRecentRefusesStepsOfAShapeTheirVersionNeverRecords(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		build    func(d *driver)
		mutation string
		legacyOK bool
	}{
		"performed with an invalid outcome": {func(d *driver) { d.decide(execBody("canti:x"), harness.Usage{}); d.perform() },
			`UPDATE transitions SET facts = json_set(facts, '$.Outcome.Status', 'bogus') WHERE kind = 'performed'`, false},
		"performed with an observation": {func(d *driver) { d.decide(execBody("canti:x"), harness.Usage{}); d.perform() },
			`UPDATE transitions SET facts = json_set(facts, '$.Observation', json('{"Execution":"exited","ExitCode":0,"Preview":"","Output":{"Path":"","Bytes":0,"Complete":false,"Available":false,"Detail":""}}')) WHERE kind = 'performed'`, false},
		"performed with a window outside the next step": {func(d *driver) { d.decide(execBody("canti:x"), harness.Usage{}); d.perform() },
			`UPDATE transitions SET facts = json_set(facts, '$.Window', json('{"FirstStep":5,"Handover":"x","SourceStep":4,"Requested":false,"Failures":0}')) WHERE kind = 'performed'`, false},
		"observation with a retrieval": {func(d *driver) { d.step("make", harness.Usage{}, 1) },
			`UPDATE transitions SET facts = json_set(facts, '$.Observation.Retrieval', json('{"query":{"operation":"read","path":"/canti/runs","reason":"x"},"next_offset":0,"more":false}')) WHERE kind = 'observed'`, true},
		"observation with an outcome": {func(d *driver) { d.step("make", harness.Usage{}, 1) },
			`UPDATE transitions SET facts = json_set(facts, '$.Outcome', json('{"Status":"failed","Detail":"x"}')) WHERE kind = 'observed'`, false},
	} {
		t.Run(name, func(t *testing.T) {
			path := tempPath(t)
			s := open(t, path)
			d := newDriver(t, s, createRun(t, s, "run-1", limits))
			tc.build(d)
			if _, err := s.db.Exec(tc.mutation); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Recent(ctx, "run-1", 0, 10); !errors.Is(err, ErrIncompatible) {
				t.Fatalf("Recent of a corrupt step = %v, want ErrIncompatible", err)
			}
			if !tc.legacyOK {
				return
			}
			if _, err := s.db.Exec(`UPDATE runs SET semantics = 3; UPDATE transitions SET semantics = 3, facts = json_remove(facts, '$.Action')`); err != nil {
				t.Fatal(err)
			}
			if steps, err := s.Recent(ctx, "run-1", 0, 10); err != nil || len(steps) != 1 || steps[0].Observation.Retrieval == nil {
				t.Fatalf("Recent of a legacy step that recorded a retrieval = %+v, %v", steps, err)
			}
		})
	}
}
