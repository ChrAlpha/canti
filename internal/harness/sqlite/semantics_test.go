package sqlite

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func semanticsReads(s *Store) map[string]func() error {
	ctx := context.Background()
	return map[string]func() error{
		"Latest":         func() error { _, err := s.Latest(ctx, "run-1"); return err },
		"Transitions":    func() error { _, err := s.Transitions(ctx, "run-1", 0, 100); return err },
		"Recent":         func() error { _, err := s.Recent(ctx, "run-1", 0, 10); return err },
		"Request":        func() error { _, err := s.Request(ctx, "run-1", 1, 1); return err },
		"Response":       func() error { _, err := s.Response(ctx, "run-1", 1, 1); return err },
		"LatestRequest":  func() error { _, err := s.LatestRequest(ctx, "run-1"); return err },
		"ContextRecords": func() error { _, err := s.ContextRecords(ctx, "run-1", 1, 1, 0, 100); return err },
		"ContextRequest": func() error { _, err := s.ContextRequest(ctx, "run-1", 1, 1); return err },
	}
}

func recordBytes(t *testing.T, s *Store, through int64) string {
	t.Helper()
	var data string
	err := s.db.QueryRow(`SELECT json_array(
		(SELECT json_array(semantics, run, initial) FROM runs WHERE id = 'run-1'),
		(SELECT json_group_array(json_array(seq, event_id, kind, semantics, event, from_state, to_state, facts, step, attempt, rev))
		FROM (SELECT * FROM transitions WHERE run_id = 'run-1' AND seq <= ? ORDER BY seq)))`, through).Scan(&data)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSemanticsVersionsResumeAndRetryWithoutRewriting(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ version, maxSteps int }{{1, 3}, {2, 3}, {2, 0}, {3, 0}} {
		t.Run(fmt.Sprintf("v%d/steps%d", tc.version, tc.maxSteps), func(t *testing.T) {
			path := tempPath(t)
			s := open(t, path)
			initial := createRun(t, s, "run-1", core.Limits{MaxSteps: tc.maxSteps, MaxAttempts: 2})
			var version int
			if err := s.db.QueryRow(`SELECT semantics FROM runs`).Scan(&version); err != nil || version != core.SemanticsVersion {
				t.Fatalf("new run semantics = %d, %v", version, err)
			}
			if _, err := s.db.Exec(`UPDATE runs SET semantics = ?`, tc.version); err != nil {
				t.Fatal(err)
			}
			if got := latestOf(t, s, "run-1"); got != initial {
				t.Fatalf("empty run = %#v, want %#v", got, initial)
			}
			if err := s.CreateRun(ctx, testRun("run-1"), initial.State); err != nil {
				t.Fatalf("identical empty CreateRun: %v", err)
			}
			if got, err := s.Transitions(ctx, "run-1", 0, 100); err != nil || len(got) != 0 {
				t.Fatalf("empty Transitions = %#v, %v", got, err)
			}
			if got, err := s.Recent(ctx, "run-1", 0, 10); err != nil || len(got) != 0 {
				t.Fatalf("empty Recent = %#v, %v", got, err)
			}
			d := newDriver(t, s, initial)
			d.step("printf first", harness.Usage{InputTokens: 5, OutputTokens: 3}, 10)
			if _, err := s.db.Exec(`UPDATE runs SET semantics = ?; UPDATE transitions SET semantics = ?`, tc.version, tc.version); err != nil {
				t.Fatal(err)
			}
			oldSeq, oldBytes := d.snap.Seq, recordBytes(t, s, d.snap.Seq)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = open(t, path)
			for name, read := range semanticsReads(readOnly(t, path)) {
				if err := read(); err != nil {
					t.Errorf("%s: %v", name, err)
				}
			}
			if err := s.CreateRun(ctx, testRun("run-1"), initial.State); err != nil {
				t.Fatalf("identical CreateRun: %v", err)
			}
			otherRun := testRun("run-1")
			otherRun.Goal += "changed"
			if err := s.CreateRun(ctx, otherRun, initial.State); err == nil {
				t.Fatal("CreateRun accepted different content")
			}
			resumed := newDriver(t, s, latestOf(t, s, "run-1"))
			resumed.decide([]byte(`{"kind":"exec","purpose":"write, then read it back","payload":"printf second"}`), harness.Usage{InputTokens: 7, OutputTokens: 2})
			if got := latestOf(t, s, "run-1").State.Decision; got != (core.Decision{Kind: core.Exec, Purpose: "write, then read it back", Payload: "printf second"}) {
				t.Fatalf("resumed decision = %#v", got)
			}
			resumed.call(harness.Runner.Bind)
			resumed.call(harness.Runner.Start)
			resumed.observe(core.ExecutionExited, 20)
			steps, err := s.Recent(ctx, "run-1", 0, 10)
			if err != nil || len(steps) != 2 || steps[0].Purpose != "" || steps[0].Command != "printf first" ||
				steps[1].Purpose != "write, then read it back" || steps[1].Command != "printf second" {
				t.Fatalf("Recent = %+v, %v; want the legacy step without a purpose and the new one with it", steps, err)
			}
			wantTotals := harness.Totals{Usage: harness.Usage{InputTokens: 12, OutputTokens: 5}, OutputBytes: 30, Steps: 2}
			for _, tr := range append(d.commits, resumed.commits...) {
				if err := s.Commit(ctx, tr); err != nil {
					t.Fatalf("retry seq %d: %v", tr.Seq, err)
				}
			}
			conflict := d.commits[2]
			conflict.EventID += "changed"
			if err := s.Commit(ctx, conflict); !errors.Is(err, harness.ErrStaleSnapshot) {
				t.Fatalf("changed event ID: %v", err)
			}
			conflict = d.commits[2]
			response := *conflict.Facts.Response
			response.Usage.OutputTokens++
			conflict.Facts.Response = &response
			if err := s.Commit(ctx, conflict); !errors.Is(err, harness.ErrStaleSnapshot) {
				t.Fatalf("changed facts: %v", err)
			}
			if got := recordBytes(t, s, oldSeq); got != oldBytes {
				t.Fatal("legacy records were rewritten")
			}
			unchanged(t, s, "run-1", append(d.commits, resumed.commits...), wantTotals)
			var nonCurrent int
			if err := s.db.QueryRow(`SELECT count(*) FROM transitions WHERE seq > ? AND semantics != ?`, oldSeq, core.SemanticsVersion).Scan(&nonCurrent); err != nil || nonCurrent != 0 {
				t.Fatalf("new transitions with non-current semantics = %d, %v", nonCurrent, err)
			}
			if got := replay(t, initial.State, history(t, s, "run-1")); got != resumed.snap.State {
				t.Fatalf("replayed state = %#v, want %#v", got, resumed.snap.State)
			}
		})
	}
}

func TestSemanticsNewWritesUseCurrentVersion(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := testRun("run-1")
	run.SessionID, run.InputID = "session", "input"
	initial := fresh(t, run.ID, core.Limits{MaxSteps: 0, MaxAttempts: 1})
	if _, err := s.Submit(ctx, run, initial); err != nil {
		t.Fatal(err)
	}
	d := newDriver(t, s, harness.Snapshot{State: initial})
	d.freeze()
	var nonCurrent int
	if err := s.db.QueryRow(`SELECT count(*) FROM (SELECT semantics FROM runs UNION ALL SELECT semantics FROM transitions) WHERE semantics != 3`).Scan(&nonCurrent); err != nil || nonCurrent != 0 {
		t.Fatalf("new records with non-v3 semantics = %d, %v", nonCurrent, err)
	}
}

func TestSemanticsRejectVersionOneNonpositiveSteps(t *testing.T) {
	for _, maxSteps := range []int{0, -1} {
		for _, field := range []string{"from_state", "to_state"} {
			t.Run(fmt.Sprintf("%s/%d", field, maxSteps), func(t *testing.T) {
				path := tempPath(t)
				s := open(t, path)
				d := newDriver(t, s, createRun(t, s, "run-1", limits))
				d.step("printf ok", harness.Usage{}, 1)
				if _, err := s.db.Exec(`UPDATE transitions SET semantics = 1, `+field+` = json_set(`+field+`, '$.Limits.MaxSteps', ?)`, maxSteps); err != nil {
					t.Fatal(err)
				}
				for name, read := range semanticsReads(readOnly(t, path)) {
					if name == "Latest" && field == "from_state" || name == "Recent" && field == "to_state" {
						continue
					}
					if err := read(); !errors.Is(err, ErrIncompatible) {
						t.Errorf("%s error = %v, want ErrIncompatible", name, err)
					}
				}
			})
		}
	}
}

func TestSemanticsRejectInvalidInitialAndUnknownVersions(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ version, maxSteps int }{{1, 0}, {1, -1}, {0, 3}, {4, 3}} {
		t.Run(fmt.Sprintf("v%d/steps%d", tc.version, tc.maxSteps), func(t *testing.T) {
			path := tempPath(t)
			s := open(t, path)
			initial := createRun(t, s, "run-1", core.Limits{MaxSteps: 3, MaxAttempts: 1})
			if _, err := s.db.Exec(`UPDATE runs SET semantics = ?, initial = json_set(initial, '$.Limits.MaxSteps', ?)`, tc.version, tc.maxSteps); err != nil {
				t.Fatal(err)
			}
			reads := semanticsReads(s)
			reads["CreateRun"] = func() error { return s.CreateRun(ctx, testRun("run-1"), initial.State) }
			for _, name := range []string{"Latest", "Transitions", "Recent", "CreateRun", "ContextRecords"} {
				if err := reads[name](); !errors.Is(err, ErrIncompatible) {
					t.Errorf("%s error = %v, want ErrIncompatible", name, err)
				}
			}
		})
	}
	for _, version := range []int{0, 4} {
		t.Run(fmt.Sprintf("transitions/v%d", version), func(t *testing.T) {
			s := open(t, tempPath(t))
			d := newDriver(t, s, createRun(t, s, "run-1", limits))
			d.step("printf ok", harness.Usage{}, 1)
			if _, err := s.db.Exec(`UPDATE transitions SET semantics = ?`, version); err != nil {
				t.Fatal(err)
			}
			for name, read := range semanticsReads(s) {
				if err := read(); !errors.Is(err, ErrIncompatible) {
					t.Errorf("%s error = %v, want ErrIncompatible", name, err)
				}
			}
		})
	}
}

func TestSemanticsRetryRejectsIncompatibleHistoricalRecord(t *testing.T) {
	for _, mutation := range []string{
		`semantics = 4`,
		`semantics = 1, from_state = json_set(from_state, '$.Limits.MaxSteps', 0)`,
		`semantics = 1, to_state = json_set(to_state, '$.Limits.MaxSteps', 0)`,
	} {
		t.Run(mutation, func(t *testing.T) {
			s := open(t, tempPath(t))
			d := newDriver(t, s, createRun(t, s, "run-1", limits))
			d.freeze()
			d.send()
			if _, err := s.db.Exec(`UPDATE transitions SET ` + mutation + ` WHERE seq = 1`); err != nil {
				t.Fatal(err)
			}
			if err := s.Commit(context.Background(), d.commits[0]); !errors.Is(err, ErrIncompatible) {
				t.Fatalf("retry incompatible historical record: %v", err)
			}
		})
	}
}

func TestSemanticsLegacyFactsRejectMissingToLimits(t *testing.T) {
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.step("printf ok", harness.Usage{}, 1)
	if _, err := s.db.Exec(`UPDATE transitions SET semantics = 1, to_state = json_remove(to_state, '$.Limits') WHERE seq < 7`); err != nil {
		t.Fatal(err)
	}
	reads := semanticsReads(s)
	reads["Commit"] = func() error { return s.Commit(context.Background(), d.commits[0]) }
	for _, name := range []string{"Request", "Response", "LatestRequest", "Commit"} {
		if err := reads[name](); !errors.Is(err, ErrIncompatible) {
			t.Errorf("%s error = %v, want ErrIncompatible", name, err)
		}
	}
}

func TestSemanticsLegacyContextRequestKeepsSelectedRecordIsolated(t *testing.T) {
	s, d := contextHistory(t)
	w := d.store.(*Store)
	if _, err := w.db.Exec(`UPDATE transitions SET semantics = 1 WHERE seq = 5;
		UPDATE transitions SET semantics = 999, facts = '{', from_state = '{', to_state = '{' WHERE seq != 5;
		UPDATE runs SET semantics = 999, initial = '{', run = '{'`); err != nil {
		t.Fatal(err)
	}
	got, err := s.ContextRequest(context.Background(), "run-1", 1, 2)
	if err != nil || !reflect.DeepEqual(got, d.commits[4]) {
		t.Fatalf("selected v1 request = %#v, %v", got, err)
	}
}

func TestSemanticsBeforeThreeRejectDecisionPurpose(t *testing.T) {
	body := []byte(`{"kind":"exec","purpose":"check","payload":"printf ok"}`)
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			path := tempPath(t)
			s := open(t, path)
			d := newDriver(t, s, createRun(t, s, "run-1", limits))
			d.decide(body, harness.Usage{})
			d.call(harness.Runner.Bind)
			d.call(harness.Runner.Start)
			d.observe(core.ExecutionExited, 1)
			d.decide(body, harness.Usage{})
			if _, err := s.db.Exec(`UPDATE runs SET semantics = ?; UPDATE transitions SET semantics = ?`, version, version); err != nil {
				t.Fatal(err)
			}
			reads := semanticsReads(readOnly(t, path))
			for _, name := range []string{"Latest", "Transitions", "Recent"} {
				if err := reads[name](); !errors.Is(err, ErrIncompatible) {
					t.Errorf("%s error = %v, want ErrIncompatible", name, err)
				}
			}
			if _, err := s.db.Exec(`UPDATE transitions SET semantics = 3 WHERE to_state LIKE '%"purpose"%' OR from_state LIKE '%"purpose"%'`); err != nil {
				t.Fatal(err)
			}
			for name, read := range semanticsReads(readOnly(t, path)) {
				if err := read(); err != nil {
					t.Errorf("%s with only purpose-free legacy records: %v", name, err)
				}
			}
		})
	}
}
