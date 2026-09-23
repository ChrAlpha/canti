package sqlite

import (
	"context"
	"errors"
	"reflect"
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
		{Step: 1, Command: "printf ok", Observation: *observation(core.ExecutionExited, 42)},
		{Step: 2, Command: "make build", Observation: *observation(core.ExecutionExited, 7), Resolved: true},
	}
	for limit, records := range map[int][]harness.StepRecord{1: want[1:], 2: want, 10: want} {
		got, err := s.Recent(ctx, "run-1", limit)
		if err != nil || !reflect.DeepEqual(got, records) {
			t.Fatalf("Recent(%d) = %#v, %v; want %#v", limit, got, err, records)
		}
	}
	totals := harness.Totals{Usage: harness.Usage{InputTokens: 45, OutputTokens: 20}, OutputBytes: 49, Steps: 2}
	if got := totalsOf(t, s, "run-1"); got != totals {
		t.Fatalf("Totals() = %#v, want %#v", got, totals)
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
				"Recent":      func() error { _, err := s.Recent(ctx, "run-1", 5); return err },
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
