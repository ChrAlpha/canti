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
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

const carryNotes = "notes"

func focusFixture(t *testing.T, replies ...reply) *fixture {
	f := newFixture(t, core.Limits{MaxSteps: 12, MaxAttempts: 4}, harness.Budget{}, replies...)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, carryNotes), []byte("frozen notes"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.cfg.Reader = &archive.Reader{Imports: dir}
	return f
}
func readNotes(extra string) reply {
	return execReply(harness.RetrievePrefix + `{"operation":"read","path":"/canti/imports/notes","reason":"Read notes"` + extra + `}`)
}

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

func TestFocusAcknowledgementLossCommitsOneIdenticalTransition(t *testing.T) {
	for _, landed := range []bool{false, true} {
		t.Run(fmt.Sprint(landed), func(t *testing.T) {
			f := focusFixture(t, readNotes(""), execReply(`canti:focus {"sources":[{"seq":5,"source":0}]}`), returnReply("done"))
			f.steps(f.loop(), 7)
			failed := false
			var first, again harness.Transition
			if _, err := f.loopOn(capturing{failingCommit{f.store, "performed", landed, &failed}, "performed", &first}).Step(f.bounded(), runID); err == nil {
				t.Fatal("acknowledgement failure missing")
			}
			f.run(f.loopOn(capturing{f.open(), "performed", &again}))
			if !landed && !reflect.DeepEqual(first, again) {
				a, _ := json.Marshal(first)
				b, _ := json.Marshal(again)
				t.Fatalf("local replay changed: %s vs %s", a, b)
			}
			n := 0
			for _, tr := range f.transitions() {
				if tr.Event.Kind() == "performed" && tr.From.Step == 2 {
					n++
				}
			}
			if n != 1 || len(requestAt(t, f, 3).ContextState.Focus) != 1 {
				t.Fatal("focus/progress duplicated or lost")
			}
		})
	}
}
func TestFocusInvalidSelectionsLeaveContextUnchanged(t *testing.T) {
	for _, args := range []string{`{}`, `{"sources":null}`, `{"sources":[{"seq":5}]}`, `{"sources":[{"seq":6,"source":0}]}`, `{"sources":[{"seq":5,"source":-1}]}`, `{"sources":[{"seq":5,"source":1}]}`, `{"sources":[{"seq":5,"source":0},{"seq":5,"source":0}]}`, `{"sources":[` + strings.Repeat(`{"seq":5,"source":0},`, 32) + `{"seq":5,"source":0}]}`} {
		t.Run(args, func(t *testing.T) {
			f := focusFixture(t, readNotes(""), execReply(harness.FocusPrefix+args), returnReply("done"))
			f.run(f.loop())
			tr := performedAt(t, f, 2)
			if tr.Facts.Outcome.Status != harness.OutcomeFailed || tr.Facts.ContextState != nil || len(requestAt(t, f, 3).ContextState.Focus) != 0 {
				t.Fatalf("invalid focus changed state: %+v", tr)
			}
		})
	}
}
func TestFocusedJournalEvidenceWithWrongHashStopsRun(t *testing.T) {
	f := focusFixture(t, readNotes(""), execReply(`canti:focus {"sources":[{"seq":5,"source":0}]}`), returnReply("done"))
	f.steps(f.loop(), 8)
	db, err := sql.Open("sqlite", f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE transitions SET facts=json_set(facts,'$.Outcome.Retrieval.evidence[0]','broken notes') WHERE seq=5 AND run_id=?`, runID); err != nil {
		t.Fatal(err)
	}
	snap := f.run(f.fresh())
	if snap.State.Reason != core.StopUnrecoverable || !strings.Contains(f.last().Facts.Failure, "integrity") {
		t.Fatalf("corrupt focus continued: %+v", snap)
	}
}
