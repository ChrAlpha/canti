package harness_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

const carryNotes = "notes"

var carryLimits = core.Limits{MaxSteps: 12, MaxAttempts: 3}

// carryFixture is a fixture whose loop reads notes as the imported file
// carryNotes and requires a handover after every window step.
func carryFixture(t *testing.T, notes string, replies ...reply) *fixture {
	t.Helper()
	f := newFixture(t, carryLimits, harness.Budget{}, replies...)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, carryNotes), []byte(notes), 0o600); err != nil {
		t.Fatal(err)
	}
	f.cfg.Reader = &archive.Reader{Imports: dir}
	f.cfg.WindowSteps = 1
	return f
}

func readNotes(extra string) reply {
	return execReply(harness.RetrievePrefix + `{"operation":"read","path":"/canti/imports/` + carryNotes + `","reason":"Read the notes."` + extra + `}`)
}

func carryReply(text, entries string) reply {
	quoted, _ := json.Marshal(text)
	return execReply(harness.HandoverCarryPrefix + `{"text":` + string(quoted) + `,"carry":[` + entries + `]}`)
}

// performedAt returns the transition that completed step by Performed.
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

func requestAt(t *testing.T, f *fixture, step int) harness.Request {
	t.Helper()
	req, err := f.store.Request(context.Background(), runID, step, 1)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func sha(text string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(text))) }

// TestCarryCommitsItsOutcomeWindowAndProgressAtOnce requires that an
// accepted carry commits, in its one Performed transition, the handover with
// the references to the committed evidence, the window that carries them,
// and the move to the next step; that the next request renders exactly the
// evidence committed, as read, and captures it; and that the bytes the carry
// takes are the work capacity it costs the window.
func TestCarryCommitsItsOutcomeWindowAndProgressAtOnce(t *testing.T) {
	notes := strings.Repeat("The parser was rewritten in v2.\n", 30)
	f := carryFixture(t, notes, readNotes(""), carryReply("Carry the notes.", `{"step":1}`), returnReply("done"))
	wantState(t, f.run(f.loop()), core.PhaseReturned, "")
	read, tr := performedAt(t, f, 1), performedAt(t, f, 2)
	ref := harness.EvidenceRef{SourceSeq: read.Seq, Step: 1, Length: len(notes), SHA256: sha(notes)}
	o := tr.Facts.Outcome
	if o == nil || o.Status != harness.OutcomeDone || o.Handover == nil || !reflect.DeepEqual(o.Handover.Carry, []harness.EvidenceRef{ref}) || o.Handover.Text != "Carry the notes." {
		t.Fatalf("the carry recorded %+v", o)
	}
	want := harness.Window{FirstStep: 3, SourceStep: 2, Handover: "Carry the notes.", Carry: []harness.EvidenceRef{ref}}
	if w := tr.Facts.Window; w == nil || !w.Equal(want) || tr.To.Step != 3 || tr.To.Phase != core.PhaseIdle {
		t.Fatalf("the carry committed window %+v and state %+v", w, tr.To)
	}
	next := requestAt(t, f, 3)
	if !next.Window.Equal(want) || next.Context == nil || !slices.Equal(next.Context.Carried, want.Carry) || next.Context.Bytes.Carry == 0 {
		t.Fatalf("the next request renders window %+v with capture %+v", next.Window, next.Context)
	}
	wantContains(t, f.model.prompts[2].User(), "## Carried evidence\n", "Carried step 1 source 0: /canti/imports/notes\n",
		fmt.Sprintf("Bytes [0,%d), sha256 %s, as read at step 1:\n", len(notes), sha(notes)), notes)

	plain := carryFixture(t, notes, readNotes(""), execReply(harness.HandoverPrefix+"Carry the notes."), returnReply("done"))
	wantState(t, plain.run(plain.loop()), core.PhaseReturned, "")
	open, left := requestAt(t, plain, 3).Context.Handover.Capacity, next.Context.Handover.Capacity
	if open-left != next.Context.Bytes.Carry {
		t.Fatalf("the carry costs %d bytes of work capacity (%d against %d) but renders %d", open-left, left, open, next.Context.Bytes.Carry)
	}
}

// carryBoundary drives a run, under a total that the first page with the
// largest legal handover fills but for 2000 bytes, that reads three ranges
// of 8192 bytes and then must hand over carrying them with a text of length
// bytes, after an earlier returned run of the same session when session is
// set, and returns the outcome of the carry and the request after it.
func carryBoundary(t *testing.T, session, marginStep bool, length int, after ...reply) (harness.Outcome, harness.Request, *sqlite.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	store, err := sqlite.Open(ctx, filepath.Join(dir, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	imports := filepath.Join(dir, "imports")
	if err := os.Mkdir(imports, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(imports, carryNotes), []byte(strings.Repeat("n", 3*archive.MaxBytes)), 0o600); err != nil {
		t.Fatal(err)
	}
	c := &clock{now: created}
	m := &fakeModel{}
	cfg := harness.LoopConfig{Environment: "fixture shell", History: filepath.Join(dir, "history"), Reader: &archive.Reader{Imports: imports},
		Now: c.Now, Poll: time.Microsecond, Backoff: -1, Limits: harness.DefaultPromptLimits(), WindowSteps: 3, HandoverBytes: 8192}
	run := harness.Run{ID: runID, Goal: "Report the notes.", Created: created}
	fit, err := harness.ContextFit(cfg, run, carryLimits)
	if err != nil {
		t.Fatal(err)
	}
	// The first page admits a handover of 8192 bytes; 2000 more bytes leave
	// room for the earlier dialogue and a boundary within a legal handover.
	cfg.Limits.Total = fit.Required() + 2000
	if marginStep {
		harness.SetCarryMarginStep(&cfg)
	}
	loop, err := harness.NewLoop(store, &fakeExec{clock: c, jobs: map[string]*fakeJob{}, fenced: map[string]bool{}}, m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	state, err := core.NewState(runID, carryLimits)
	if err != nil {
		t.Fatal(err)
	}
	if session {
		earlier, err := core.NewState("run-0", carryLimits)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Submit(ctx, harness.Run{ID: "run-0", Goal: "Collect the notes.", SessionID: "s", InputID: "i0", Created: created}, earlier); err != nil {
			t.Fatal(err)
		}
		m.replies = append(m.replies, returnReply("Collected."))
		if s, err := loop.Run(ctx, "run-0"); err != nil || s.State.Phase != core.PhaseReturned {
			t.Fatalf("earlier run = %+v, %v", s.State, err)
		}
		run.SessionID, run.InputID = "s", "i1"
		if run, err = store.Submit(ctx, run, state); err != nil {
			t.Fatal(err)
		}
	} else if err := store.CreateRun(ctx, run, state); err != nil {
		t.Fatal(err)
	}
	m.replies = append(m.replies, readNotes(`,"limit":8192`), readNotes(`,"offset":8192,"limit":8192`), readNotes(`,"offset":16384,"limit":8192`),
		carryReply(strings.Repeat("h", length), `{"step":1},{"step":2},{"step":3}`))
	if after == nil {
		after = []reply{execReply(harness.HandoverPrefix + "Report the notes."), returnReply("done")}
	}
	m.replies = append(m.replies, after...)
	if s, err := loop.Run(ctx, runID); err != nil || s.State.Phase != core.PhaseReturned {
		t.Fatalf("run = %+v, %v", s.State, err)
	}
	steps, err := store.Recent(ctx, runID, 0, 10)
	if err != nil || len(steps) < 4 || steps[3].Outcome == nil {
		t.Fatalf("steps = %+v, %v", steps, err)
	}
	next, err := store.Request(ctx, runID, 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	if session && next.Context.Bytes.Session == 0 {
		t.Fatal("the run selected no earlier session dialogue")
	}
	return *steps[3].Outcome, next, store
}

// TestCarryAdmissionIsStrictAtTheRenderedCapacity requires that a carry is
// admitted exactly while the window it opens, rendered as its first request
// renders it, with any earlier session dialogue, keeps positive work
// capacity: a handover text one byte longer than one leaving a single byte
// is refused, and keeps the window with a failure. A margin of one maximal
// step refuses the carry that leaves a single byte.
func TestCarryAdmissionIsStrictAtTheRenderedCapacity(t *testing.T) {
	for _, session := range []bool{false, true} {
		t.Run(map[bool]string{false: "alone", true: "after session dialogue"}[session], func(t *testing.T) {
			o, next, _ := carryBoundary(t, session, false, 16)
			if o.Status != harness.OutcomeDone {
				t.Fatalf("a small carry was refused: %+v", o)
			}
			length := 16 + next.Context.Handover.Capacity - 1
			if length > 8192 {
				t.Fatalf("the boundary needs a handover of %d bytes", length)
			}
			o, next, store := carryBoundary(t, session, false, length, execReply("true"), execReply(harness.HandoverPrefix+"Report the notes."), returnReply("done"))
			if o.Status != harness.OutcomeDone || next.Context.Handover.Capacity != 1 || next.Window.FirstStep != 5 || next.Window.Requested {
				t.Fatalf("at one byte of capacity: outcome %+v, next window %+v with capacity %d", o, next.Window, next.Context.Handover.Capacity)
			}
			// The window takes exactly the step its reserve fits, then requires
			// a handover for its capacity.
			crossing, err := store.Request(context.Background(), runID, 6, 1)
			if err != nil || !crossing.Window.Requested || !slices.Contains(crossing.Context.Handover.Reasons, "capacity") || crossing.Window.FirstStep != 5 {
				t.Fatalf("after the crossing step the request renders %+v with check %+v, %v", crossing.Window, crossing.Context.Handover, err)
			}
			o, next, _ = carryBoundary(t, session, false, length+1)
			if o.Status != harness.OutcomeFailed || !strings.Contains(o.Detail, "leaves the next window 0 bytes of work capacity") ||
				next.Window.FirstStep != 1 || next.Window.Failures != 1 || !next.Window.Requested || len(next.Window.Carry) != 0 {
				t.Fatalf("at no capacity: outcome %+v, next window %+v", o, next.Window)
			}
			o, _, _ = carryBoundary(t, session, true, length)
			if o.Status != harness.OutcomeFailed || !strings.Contains(o.Detail, "leaves the next window 1 bytes of work capacity") || !strings.Contains(o.Detail, "a carry needs more than 11398.") {
				t.Fatalf("with a margin of one maximal step: outcome %+v", o)
			}
		})
	}
}

// TestCarryRefusesASelectionTheWindowCannotSatisfy requires that a carry
// naming no retrieved evidence of the window, a source out of range, a range
// twice, more ranges than MaxCarry once expanded, or malformed arguments
// fails as a proposal the agent can correct: the window keeps itself with
// one more failure, and an unsolicited carry changes no window.
func TestCarryRefusesASelectionTheWindowCannotSatisfy(t *testing.T) {
	far := strings.Repeat(strings.Repeat("x", 300)+"match\n", 40)
	search := execReply(harness.RetrievePrefix + `{"operation":"search","path":"/canti/imports/notes","query":"match","limit":8192,"reason":"Find every match."}`)
	for _, tc := range []struct {
		name, notes string
		steps       int
		replies     []reply
		problem     string
	}{
		{"command", "n", 1, []reply{execReply("ls"), carryReply("Carry.", `{"step":1}`)}, "step 1 of this window holds no retrieved evidence"},
		{"list", "n", 1, []reply{execReply(harness.RetrievePrefix + `{"operation":"list","path":"/canti/imports","reason":"List."}`), carryReply("Carry.", `{"step":1}`)}, "step 1 of this window holds no retrieved evidence"},
		{"source out of range", "n", 1, []reply{readNotes(""), carryReply("Carry.", `{"step":1,"source":1}`)}, "step 1 retrieved 1 sources; source 1 is out of range"},
		{"outside the window", "n", 1, []reply{readNotes(""), carryReply("Carry.", `{"step":7}`)}, "step 7 is neither a retrieval step of this window nor carried by it"},
		{"twice", "n", 1, []reply{readNotes(""), carryReply("Carry.", `{"step":1},{"step":1,"source":0}`)}, "carry names step 1 source 0 twice"},
		{"too many once expanded", far, 2, []reply{search, search, carryReply("Carry.", `{"step":1},{"step":2}`)}, "carry names more than 32 source ranges"},
		{"no entry", "n", 1, []reply{readNotes(""), carryReply("Carry.", ``)}, "carry names 0 entries; it must name 1 to 32"},
		{"malformed", "n", 1, []reply{readNotes(""), execReply(harness.HandoverCarryPrefix + `{"text":"Carry.","carry":[{"step":1}],"extra":1}`)}, `unknown field "extra"`},
		{"unsolicited", "n", 0, []reply{readNotes(""), carryReply("Carry.", `{"step":1}`)}, "handover was not requested for this step"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := carryFixture(t, tc.notes, append(tc.replies, returnReply("done"))...)
			f.cfg.WindowSteps = tc.steps
			wantState(t, f.run(f.loop()), core.PhaseReturned, "")
			step := len(tc.replies)
			tr := performedAt(t, f, step)
			if o := tr.Facts.Outcome; o.Status != harness.OutcomeFailed || !strings.Contains(o.Detail, tc.problem) || o.Handover != nil {
				t.Fatalf("the carry ended as %+v; want it failed with %q", o, tc.problem)
			}
			w := tr.Facts.Window
			if tc.steps == 0 && w != nil || tc.steps > 0 && (w == nil || w.FirstStep != 1 || w.Failures != 1 || !w.Requested) {
				t.Fatalf("the refused carry committed window %+v", w)
			}
		})
	}
}

// corrupt rewrites, in the journal at path, the evidence of the retrieval
// that the run performed at step.
func corrupt(t *testing.T, path string, step int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	res, err := db.Exec(`UPDATE transitions SET facts = json_set(facts, '$.Outcome.Retrieval.evidence[0]', 'tampered') WHERE run_id = ? AND kind = 'performed' AND json_extract(from_state, '$.Step') = ?`, runID, step)
	if n, _ := res.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("corrupt step %d: %v, %d rows", step, err, n)
	}
}

// TestCarriedEvidenceThatNoLongerMatchesStopsTheRun requires that committed
// evidence that does not match the reference that names it stops the run as
// a journal integrity failure, before the carry is performed and after the
// window that carries it opened, never reads its source again, and records
// no failed proposal against the agent.
func TestCarriedEvidenceThatNoLongerMatchesStopsTheRun(t *testing.T) {
	t.Run("at the carry", func(t *testing.T) {
		var f *fixture
		hooked := carryReply("Carry the notes.", `{"step":1}`)
		hooked.hook = func() { corrupt(t, f.path, 1) }
		f = carryFixture(t, "The parser was rewritten in v2.\n", readNotes(""), hooked)
		wantState(t, f.run(f.loop()), core.PhaseStopped, core.StopUnrecoverable)
		var failure string
		for _, tr := range f.transitions() {
			if tr.Event.Kind() == "stop_requested" {
				failure = tr.Facts.Failure
			}
			if tr.Event.Kind() == "performed" && tr.From.Step == 2 {
				t.Fatalf("the damaged carry was performed as %+v", tr.Facts.Outcome)
			}
		}
		if !strings.Contains(failure, "journal integrity: the evidence of step 1 source 0 does not match") {
			t.Fatalf("stopped with %q", failure)
		}
		if last := f.last(); last.Event.Kind() != "observed" || last.Facts.Observation.Execution != core.ExecutionCancelled {
			t.Fatalf("the carry ended as %s %+v", last.Event.Kind(), last.Facts)
		}
	})
	t.Run("in the window it opened", func(t *testing.T) {
		var f *fixture
		hooked := execReply("true")
		hooked.hook = func() {
			corrupt(t, f.path, 1)
			if err := os.Remove(filepath.Join(f.cfg.Reader.Imports, carryNotes)); err != nil {
				t.Error(err)
			}
		}
		f = carryFixture(t, "The parser was rewritten in v2.\n", readNotes(""), carryReply("Carry the notes.", `{"step":1}`), hooked)
		wantState(t, f.run(f.loop()), core.PhaseStopped, core.StopUnrecoverable)
		if failure := f.last().Facts.Failure; !strings.Contains(failure, "journal integrity") || performedAt(t, f, 2).Facts.Outcome.Status != harness.OutcomeDone {
			t.Fatalf("stopped with %q after %v", failure, f.kinds())
		}
	})
}

// TestCarriedEvidenceOutlivesItsSource requires that the window a carry
// opens renders the evidence as committed after its source is removed:
// nothing is read again.
func TestCarriedEvidenceOutlivesItsSource(t *testing.T) {
	notes := "The parser was rewritten in v2.\n"
	var f *fixture
	remove := carryReply("Carry the notes.", `{"step":1}`)
	remove.hook = func() {
		if err := os.Remove(filepath.Join(f.cfg.Reader.Imports, carryNotes)); err != nil {
			t.Error(err)
		}
	}
	f = carryFixture(t, notes, readNotes(""), remove, execReply("true"), returnReply("done"))
	wantState(t, f.run(f.loop()), core.PhaseReturned, "")
	for _, p := range f.model.prompts[2:] {
		wantContains(t, p.User(), "Carried step 1 source 0: /canti/imports/notes\n", notes)
	}
}

// TestCarryRestartsAndLostAcknowledgementsCommitOneIdenticalWindow requires
// that a run carried again across two windows commits the same transitions
// and facts whether or not its controller restarts before every step, and
// that a carry whose acknowledgement was lost, landed or not, commits one
// Performed with the facts first attempted.
func TestCarryRestartsAndLostAcknowledgementsCommitOneIdenticalWindow(t *testing.T) {
	replies := []reply{readNotes(""), carryReply("Carry the notes.", `{"step":1}`), readNotes(`,"offset":4`),
		carryReply("Carry both.", `{"step":1},{"step":3}`), returnReply("done")}
	steady := carryFixture(t, "The parser was rewritten in v2.\n", replies...)
	wantState(t, steady.run(steady.loop()), core.PhaseReturned, "")
	restarted := carryFixture(t, "The parser was rewritten in v2.\n", replies...)
	for {
		s, err := restarted.fresh().Step(restarted.bounded(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Intent() == core.IntentHalt {
			break
		}
	}
	want, got := workingSetTrace(t, steady.store), workingSetTrace(t, restarted.store)
	if !slices.Equal(want, got) {
		t.Fatalf("the restarted run committed %d transitions unlike the %d of the steady run", len(got), len(want))
	}
	if again := performedAt(t, steady, 4).Facts.Window.Carry; len(again) != 2 || again[0] != performedAt(t, steady, 2).Facts.Window.Carry[0] {
		t.Fatalf("the range carried again is %+v", again)
	}

	for _, landed := range []bool{true, false} {
		t.Run(map[bool]string{true: "landed", false: "lost"}[landed], func(t *testing.T) {
			f := carryFixture(t, "The parser was rewritten in v2.\n", readNotes(""), carryReply("Carry the notes.", `{"step":1}`), returnReply("done"))
			ctx := f.bounded()
			f.stepUntil(ctx, core.PhaseIdle)
			f.stepUntil(ctx, core.PhaseEffectSaved)
			var first, committed harness.Transition
			var failed bool
			if _, err := f.loopOn(capturing{failingCommit{f.store, "performed", landed, &failed}, "performed", &first}).Step(ctx, runID); err == nil {
				t.Fatal("the lost acknowledgement was not surfaced")
			}
			wantState(t, f.run(f.loopOn(capturing{f.open(), "performed", &committed})), core.PhaseReturned, "")
			performed := slices.DeleteFunc(f.transitions(), func(tr harness.Transition) bool { return tr.Event.Kind() != "performed" || tr.From.Step != 2 })
			if len(performed) != 1 || !reflect.DeepEqual(performed[0].Facts, first.Facts) || len(performed[0].Facts.Window.Carry) != 1 {
				t.Fatalf("committed %+v, first attempted %+v", performed, first)
			}
			if !landed && !reflect.DeepEqual(committed.Facts, first.Facts) {
				t.Fatalf("the carry performed again into %+v, not %+v", committed.Facts, first.Facts)
			}
		})
	}
}

// TestAnActionAcceptedBeforeCarryKeepsItsContract requires that an
// acceptance that context version 13 recorded for the canti:handover-carry
// word, then an unknown action, keeps that contract under the version that
// knows the word: it is read and stopped as recorded, performed as an
// unknown action, never as a carry, and an outcome it recorded with that
// version's wording stays readable.
func TestAnActionAcceptedBeforeCarryKeepsItsContract(t *testing.T) {
	payload := harness.HandoverCarryPrefix + `{"text":"Carry.","carry":[{"step":1}]}`
	v13 := fmt.Sprintf(`{"Name":"unknown","Recovery":"local_change","Args":%d,"Invalid":"unknown_action"}`, len(harness.HandoverCarryPrefix))
	accepted := func(t *testing.T) *fixture {
		f := carryFixture(t, "n", execReply(payload), returnReply("done"))
		f.cfg.WindowSteps = 0
		f.stepUntil(f.bounded(), core.PhaseEffectSaved)
		f.exec1(`UPDATE transitions SET facts = json_set(facts, '$.Action', json(?)) WHERE kind = 'response_loaded'`, v13)
		a, err := f.store.Accepted(context.Background(), runID, 1, 1)
		if err != nil || a.Provenance != harness.ProvenanceRecorded || a.Action.Name != harness.ActionUnknown {
			t.Fatalf("the version-13 acceptance reads as %+v, %v", a, err)
		}
		return f
	}
	t.Run("stopped", func(t *testing.T) {
		f := accepted(t)
		s, err := f.fresh().Stop(f.bounded(), runID, core.StopCancelled)
		if err != nil || s.State.Phase != core.PhaseStopped || f.last().Facts.Observation.Execution != core.ExecutionCancelled {
			t.Fatalf("stop = %+v, %v after %v", s.State, err, f.kinds())
		}
	})
	t.Run("performed", func(t *testing.T) {
		f := accepted(t)
		wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
		o := performedAt(t, f, 1).Facts.Outcome
		if o.Status != harness.OutcomeFailed || o.Handover != nil || !strings.HasPrefix(o.Detail, "unknown harness action") {
			t.Fatalf("the version-13 action was performed as %+v", o)
		}
		old := "unknown harness action; the actions are canti:retrieve and canti:handover"
		f.exec1(`UPDATE transitions SET facts = json_set(facts, '$.Outcome.Detail', ?) WHERE kind = 'performed'`, old)
		steps, err := f.store.Recent(context.Background(), runID, 0, 10)
		if err != nil || len(steps) != 1 || steps[0].Outcome.Detail != old {
			t.Fatalf("the version-13 outcome reads as %+v, %v", steps, err)
		}
	})
}

// exec1 runs one statement on the journal of f that changes at least one
// row.
func (f *fixture) exec1(query string, args ...any) {
	f.t.Helper()
	db, err := sql.Open("sqlite", f.path)
	if err != nil {
		f.t.Fatal(err)
	}
	defer db.Close()
	res, err := db.Exec(query, args...)
	if n, _ := res.RowsAffected(); err != nil || n == 0 {
		f.t.Fatalf("%s: %v, %d rows", query, err, n)
	}
}

// stepCold steps the run of f, on a loop rebuilt over a newly opened
// journal before every step when cold is set, until it halts or until
// reached holds of its latest snapshot.
func stepCold(t *testing.T, f *fixture, cold bool, reached func(harness.Snapshot) bool) harness.Snapshot {
	t.Helper()
	ctx := f.bounded()
	l := f.loop()
	for range 200 {
		if cold {
			l = f.fresh()
		}
		s, err := l.Step(ctx, runID)
		if err != nil {
			t.Fatalf("Step() = %v at %+v", err, s.State)
		}
		if s.State.Intent() == core.IntentHalt || reached != nil && reached(s) {
			return s
		}
	}
	t.Fatal("the run did not halt within 200 steps")
	return harness.Snapshot{}
}

// TestCarryWithoutEntriesFailsAsAProposal requires that a carry whose carry
// is omitted, null, or empty is decided by its action, never taken for a
// plain handover: it fails as a proposal, a requested window keeps itself
// with one more failure, an unrequested carry changes no window, and the
// run, restarted before every step, goes on.
func TestCarryWithoutEntriesFailsAsAProposal(t *testing.T) {
	for name, args := range map[string]string{"omitted": `{"text":"Carry."}`, "null": `{"text":"Carry.","carry":null}`, "empty": `{"text":"Carry.","carry":[]}`} {
		for _, requested := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s requested %t", name, requested), func(t *testing.T) {
				f := carryFixture(t, "n", readNotes(""), execReply(harness.HandoverCarryPrefix+args), execReply(harness.HandoverPrefix+"Next: report."), returnReply("done"))
				if !requested {
					f.cfg.WindowSteps = 0
				}
				if s := stepCold(t, f, true, nil); s.State.Phase != core.PhaseReturned {
					t.Fatalf("the run ended as %+v", s.State)
				}
				tr := performedAt(t, f, 2)
				problem := "carry names 0 entries; it must name 1 to 32"
				if !requested {
					problem = "handover was not requested for this step"
				}
				if o := tr.Facts.Outcome; o.Status != harness.OutcomeFailed || !strings.Contains(o.Detail, problem) {
					t.Fatalf("the carry ended as %+v", o)
				}
				if w := tr.Facts.Window; requested && (w == nil || w.FirstStep != 1 || w.Failures != 1) || !requested && w != nil {
					t.Fatalf("the refused carry committed window %+v", w)
				}
			})
		}
	}
}

// TestDamageBehindACarriedReferenceStopsTheRun requires that a source
// transition of an established carried reference that is gone, of another
// kind, malformed, or holding bytes of another length or hash stops the run
// as a journal integrity failure, both where a request of the window renders
// the reference and where a handover carries it again, also under a loop
// rebuilt before every step, without reading the source again.
func TestDamageBehindACarriedReferenceStopsTheRun(t *testing.T) {
	damages := map[string]string{
		"missing":    `DELETE FROM transitions WHERE run_id = ?1 AND seq = ?2`,
		"wrong kind": `UPDATE transitions SET kind = 'job_started' WHERE run_id = ?1 AND seq = ?2`,
		"malformed":  `UPDATE transitions SET facts = json_set(facts, '$.Outcome.Status', 'bogus') WHERE run_id = ?1 AND seq = ?2`,
		"length":     `UPDATE transitions SET facts = json_set(facts, '$.Outcome.Retrieval.evidence[0]', 'short') WHERE run_id = ?1 AND seq = ?2`,
		"hash":       `UPDATE transitions SET facts = json_set(facts, '$.Outcome.Retrieval.evidence[0]', upper(json_extract(facts, '$.Outcome.Retrieval.evidence[0]'))) WHERE run_id = ?1 AND seq = ?2`,
	}
	for name, damage := range damages {
		for _, at := range []string{"preparation", "perform"} {
			for _, cold := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s at %s cold %t", name, at, cold), func(t *testing.T) {
					f := carryFixture(t, "the parser was rewritten in v2.\n", readNotes(""), carryReply("Carry the notes.", `{"step":1}`),
						execReply("true"), carryReply("Carry the notes again.", `{"step":1}`), returnReply("done"))
					// Preparation renders the carried range from step 3 on; the
					// carry at step 4 reads it again after step 4 was accepted.
					reached := func(s harness.Snapshot) bool { return s.State.Step == 3 && s.State.Phase == core.PhaseIdle }
					if at == "perform" {
						reached = func(s harness.Snapshot) bool { return s.State.Step == 4 && s.State.Phase == core.PhaseEffectSaved }
					}
					if s := stepCold(t, f, cold, reached); !reached(s) {
						t.Fatalf("the run ended as %+v before the damage", s.State)
					}
					f.exec1(damage, runID, performedAt(t, f, 1).Seq)
					if err := os.Remove(filepath.Join(f.cfg.Reader.Imports, carryNotes)); err != nil {
						t.Fatal(err)
					}
					s := stepCold(t, f, cold, nil)
					wantState(t, s, core.PhaseStopped, core.StopUnrecoverable)
					var failure string
					for _, tr := range f.transitions() {
						if tr.Event.Kind() == "stop_requested" {
							failure = tr.Facts.Failure
						}
					}
					if !strings.Contains(failure, "journal integrity") {
						t.Fatalf("stopped with %q after %v", failure, f.kinds())
					}
					if at == "perform" && f.last().Facts.Observation.Execution != core.ExecutionCancelled {
						t.Fatalf("the carry ended as %s %+v", f.last().Event.Kind(), f.last().Facts)
					}
				})
			}
		}
	}
}

// TestASecondRefusedCarryStopsTheRun requires that two refused proposals at
// one required handover stop the run, as two failed plain handovers do.
func TestASecondRefusedCarryStopsTheRun(t *testing.T) {
	f := carryFixture(t, "n", readNotes(""), carryReply("Carry.", `{"step":9}`), carryReply("Carry.", `{"step":9}`), returnReply("unreachable"))
	wantState(t, f.run(f.loop()), core.PhaseStopped, core.StopUnrecoverable)
	if failure := f.last().Facts.Failure; failure != "harness: handover failed twice; refusing to discard the current context" {
		t.Fatalf("stopped with %q", failure)
	}
	if o := performedAt(t, f, 3).Facts; o.Outcome.Status != harness.OutcomeFailed || o.Window == nil || o.Window.Failures != 2 {
		t.Fatalf("the second carry committed %+v", o)
	}
}

// TestACarriedReadKeepsItsContinuation requires that a carried range of a
// read that stopped before the end of its source says where the source
// continues.
func TestACarriedReadKeepsItsContinuation(t *testing.T) {
	notes := strings.Repeat("0123456789", 100)
	f := carryFixture(t, notes, readNotes(`,"limit":256`), carryReply("Carry the start.", `{"step":1}`), returnReply("done"))
	wantState(t, f.run(f.loop()), core.PhaseReturned, "")
	wantContains(t, f.model.prompts[2].User(), "Carried step 1 source 0: /canti/imports/notes\nBytes [0,256), sha256 "+sha(notes[:256])+", as read at step 1; the source continues at offset 256:\n")
}

// TestCarryDropsWhatTheNextWindowNoLongerNeeds requires that a handover
// carries only what it names: the range a later window no longer needs
// leaves the carry, and the carried bytes fall with it.
func TestCarryDropsWhatTheNextWindowNoLongerNeeds(t *testing.T) {
	notes := strings.Repeat("The first phase needs these notes.\n", 40)
	f := carryFixture(t, notes, readNotes(""), carryReply("Carry the notes.", `{"step":1}`), readNotes(`,"offset":0,"limit":64`),
		carryReply("The second phase needs only the start.", `{"step":3}`), returnReply("done"))
	wantState(t, f.run(f.loop()), core.PhaseReturned, "")
	first, second := requestAt(t, f, 3), requestAt(t, f, 5)
	if len(second.Window.Carry) != 1 || second.Window.Carry[0].Step != 3 || second.Context.Bytes.Carry >= first.Context.Bytes.Carry {
		t.Fatalf("the second window carries %+v in %d bytes, the first %d bytes", second.Window.Carry, second.Context.Bytes.Carry, first.Context.Bytes.Carry)
	}
	if body := f.model.prompts[4].User(); strings.Contains(body, "Carried step 1 source 0") || strings.Count(body, "The first phase needs these notes.") > 2 {
		t.Fatal("the second window still carries the range it dropped")
	}
}
