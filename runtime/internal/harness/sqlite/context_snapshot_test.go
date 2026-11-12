package sqlite

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	modernc "modernc.org/sqlite"
)

var contextSnapshotFunctions atomic.Uint64

func TestContextRecordsKeepsOneSnapshotWhileWriterCommits(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	w := open(t, path)
	d := newDriver(t, w, createRun(t, w, "run-1", limits))
	d.step("ls", harness.Usage{}, 1)
	before := d.snap.Seq
	next := derive(t, d.snap, core.RequestFrozen{}, harness.Facts{Request: requestFor(d.snap.State)})
	var armed, committed atomic.Bool
	name := fmt.Sprintf("context_snapshot_%d", contextSnapshotFunctions.Add(1))
	err := modernc.RegisterDeterministicScalarFunction(name, 1, func(_ *modernc.FunctionContext, args []sqldriver.Value) (sqldriver.Value, error) {
		if armed.CompareAndSwap(true, false) {
			if err := w.Commit(ctx, next); err != nil {
				return nil, err
			}
			committed.Store(true)
		}
		return args[0], nil
	})
	if err != nil {
		t.Fatal(err)
	}
	w.db.SetMaxIdleConns(0)
	s := readOnly(t, path)
	if _, err := w.db.Exec(`ALTER TABLE runs RENAME COLUMN run TO stored_run;
		ALTER TABLE runs ADD COLUMN run TEXT GENERATED ALWAYS AS (` + name + `(stored_run)) VIRTUAL`); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	page, err := s.ContextRecords(ctx, "run-1", 0, 0, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !committed.Load() || page.AsOfSeq != before || page.Request.Seq != 1 || page.Next != nil || len(page.Events) != 4 || page.Events[3].Seq != before {
		t.Fatalf("mixed snapshot: committed = %v, page = %#v", committed.Load(), page)
	}
	page, err = s.ContextRecords(ctx, "run-1", 0, 0, 0, 100)
	if err != nil || page.AsOfSeq != next.Seq || page.Request.Seq != next.Seq || page.Previous == nil || page.Previous.Seq != 1 || len(page.Events) != 0 {
		t.Fatalf("next snapshot: %#v, %v", page, err)
	}
}

func TestContextRecordsRequestQueriesUseIndexedRanges(t *testing.T) {
	s := open(t, tempPath(t))
	for _, tc := range []struct {
		selection string
		args      []any
	}{
		{" ORDER BY step DESC, rev DESC LIMIT 1", []any{"run-1"}},
		{" AND step = ? ORDER BY rev DESC LIMIT 1", []any{"run-1", 30}},
		{" AND step = ? AND rev = ?", []any{"run-1", 30, 2}},
		{" AND (step, rev) < (?, ?) ORDER BY step DESC, rev DESC LIMIT 1", []any{"run-1", 30, 2}},
		{" AND (step, rev) > (?, ?) ORDER BY step, rev LIMIT 1", []any{"run-1", 30, 2}},
	} {
		rows, err := s.db.Query("EXPLAIN QUERY PLAN "+contextFrozenSQL+tc.selection, tc.args...)
		if err != nil {
			t.Fatal(err)
		}
		var details []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			details = append(details, detail)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if len(details) != 1 || !strings.Contains(details[0], "SEARCH transitions USING INDEX transitions_request") {
			t.Fatalf("query %s plan = %v; want one indexed search without sort", tc.selection, details)
		}
	}
}

func TestContextHeadResetIsAtomicAndRejectsStaleReaders(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s := open(t, path)
	d := newDriver(t, s, createRun(t, s, "anchor", core.Limits{MaxAttempts: 3}))
	initial := d.snap
	if c, err := s.Context(ctx, initial); err != nil || !c.Equal(harness.InitialContext()) {
		t.Fatalf("initial=%+v %v", c, err)
	}
	d.freeze()
	d.send()
	before := d.snap
	rejection := &harness.ContextRejection{Previous: harness.InitialContext(), Signal: harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}, EpochSteps: 1}
	next := harness.ContextState{AnchorSeq: before.Seq + 1, FirstStep: 1, Rejection: &harness.RejectionRef{Seq: before.Seq + 1, Step: 1}}
	facts := harness.Facts{Response: &harness.Response{Body: []byte(`{"error":{"code":"context_length_exceeded"}}`)}, Failure: "confirmed overflow", Rejection: rejection, ContextState: &next}
	bad := facts
	invalid := next
	invalid.AnchorSeq++
	bad.ContextState = &invalid
	if err := d.try(core.RequestRejected{Step: 1, Attempt: 1}, bad); !errors.Is(err, harness.ErrFacts) {
		t.Fatalf("bad anchor error=%v", err)
	}
	if got := latestOf(t, s, "anchor"); got != before {
		t.Fatal("failed reset advanced state")
	}
	d.do(core.RequestRejected{Step: 1, Attempt: 1}, facts)
	committed := d.commits[len(d.commits)-1]
	if err := s.Commit(ctx, committed); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, path)
	if c, err := reopened.Context(ctx, d.snap); err != nil || !c.Equal(next) {
		t.Fatalf("reopened=%+v %v", c, err)
	}
	if _, err := reopened.Context(ctx, before); !errors.Is(err, harness.ErrStaleSnapshot) {
		t.Fatalf("stale context error=%v", err)
	}
	if got := totalsOf(t, s, "anchor"); got.Coverage.Attempts != 1 {
		t.Fatalf("reset report counted twice: %+v", got)
	}
	request := requestFor(d.snap.State)
	wrong := harness.InitialContext()
	request.ContextState = &wrong
	if err := d.try(core.RequestFrozen{}, harness.Facts{Request: request}); !errors.Is(err, harness.ErrFacts) {
		t.Fatalf("old context accepted: %v", err)
	}
	request.ContextState = &next
	d.do(core.RequestFrozen{}, harness.Facts{Request: request})
}

func TestExhaustedRejectionDoesNotOpenContext(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "exhausted", core.Limits{MaxAttempts: 1}))
	d.freeze()
	d.send()
	facts := harness.Facts{Response: &harness.Response{Body: []byte(`{}`)}, Failure: "confirmed overflow", Rejection: &harness.ContextRejection{Previous: harness.InitialContext(), Signal: harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}, EpochSteps: 1}}
	next := harness.ContextState{AnchorSeq: d.snap.Seq + 1, FirstStep: 1, Rejection: &harness.RejectionRef{Seq: d.snap.Seq + 1, Step: 1}}
	facts.ContextState = &next
	if err := d.try(core.RequestRejected{Step: 1, Attempt: 1}, facts); !errors.Is(err, harness.ErrFacts) {
		t.Fatalf("opened exhausted context: %v", err)
	}
	facts.ContextState = nil
	d.do(core.RequestRejected{Step: 1, Attempt: 1}, facts)
	if c, err := s.Context(ctx, d.snap); err != nil || !c.Equal(harness.InitialContext()) {
		t.Fatalf("exhausted context=%+v %v", c, err)
	}
}

func TestContextHeadAndUsageOverflowRollBackTogether(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "reset-overflow", core.Limits{MaxAttempts: 3}))
	d.freeze()
	d.send()
	before := d.snap
	if _, err := s.db.Exec(`UPDATE runs SET input_tokens=? WHERE id=?`, int64(9223372036854775807), before.State.RunID); err != nil {
		t.Fatal(err)
	}
	next := harness.ContextState{AnchorSeq: before.Seq + 1, FirstStep: 1, Rejection: &harness.RejectionRef{Seq: before.Seq + 1, Step: 1}}
	facts := harness.Facts{Response: &harness.Response{Body: []byte(`{"error":{"code":"context_length_exceeded"}}`), Usage: harness.Usage{InputTokens: 1, InputReported: true}}, Failure: "confirmed overflow", Rejection: &harness.ContextRejection{Previous: harness.InitialContext(), Signal: harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}, EpochSteps: 1}, ContextState: &next}
	if err := d.try(core.RequestRejected{Step: 1, Attempt: 1}, facts); err == nil {
		t.Fatal("overflowing reset committed")
	}
	if got := latestOf(t, s, before.State.RunID); got != before {
		t.Fatalf("reset changed state: %+v", got)
	}
	if got, err := s.Context(ctx, before); err != nil || !got.Equal(harness.InitialContext()) {
		t.Fatalf("reset changed context: %+v %v", got, err)
	}
	var pointer int64
	if err := s.db.QueryRow(`SELECT context_seq FROM runs WHERE id=?`, before.State.RunID).Scan(&pointer); err != nil || pointer != 0 {
		t.Fatalf("reset changed pointer: %d %v", pointer, err)
	}
	if got := totalsOf(t, s, before.State.RunID); got.Usage.InputTokens != 9223372036854775807 || got.Coverage.Attempts != 0 {
		t.Fatalf("partial usage commit: %+v", got)
	}
}

func TestContextHeadRejectsInvalidPointerAndCancelledRead(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "head-pointers", core.Limits{MaxAttempts: 3}))
	d.freeze()
	d.send()
	for _, pointer := range []int64{-1, d.snap.Seq + 1, 1} {
		if _, err := s.db.Exec(`UPDATE runs SET context_seq=? WHERE id=?`, pointer, d.snap.State.RunID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Context(ctx, d.snap); !errors.Is(err, ErrIncompatible) {
			t.Errorf("pointer %d accepted: %v", pointer, err)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Context(cancelled, d.snap); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read: %v", err)
	}
}

func TestContextHeadReadKeepsSnapshotWhenResetCommitsDuringPointerRead(t *testing.T) {
	ctx := context.Background()
	file := tempPath(t)
	w := open(t, file)
	d := newDriver(t, w, createRun(t, w, "head-race", core.Limits{MaxAttempts: 3}))
	d.freeze()
	d.send()
	before := d.snap
	next := harness.ContextState{AnchorSeq: before.Seq + 1, FirstStep: 1, Rejection: &harness.RejectionRef{Seq: before.Seq + 1, Step: 1}}
	facts := harness.Facts{Response: &harness.Response{Body: []byte(`{}`)}, Failure: "confirmed overflow", Rejection: &harness.ContextRejection{Previous: harness.InitialContext(), Signal: harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}, EpochSteps: 1}, ContextState: &next}
	transition := derive(t, before, core.RequestRejected{Step: 1, Attempt: 1}, facts)
	var armed, committed atomic.Bool
	name := fmt.Sprintf("head_snapshot_%d", contextSnapshotFunctions.Add(1))
	if err := modernc.RegisterDeterministicScalarFunction(name, 1, func(_ *modernc.FunctionContext, args []sqldriver.Value) (sqldriver.Value, error) {
		if armed.CompareAndSwap(true, false) {
			// A generated pointer column lets this callback run precisely between
			// the production reader's head query and context dereference.
			// Publish a valid derived transition with its stored pointer in
			// one real SQLite transaction; production commit is covered above.
			encoded, err := encode(transition)
			if err != nil {
				return nil, err
			}
			tx, err := w.db.BeginTx(ctx, nil)
			if err != nil {
				return nil, err
			}
			defer tx.Rollback()
			pos, posErr := nextPos(ctx, tx)
			if posErr != nil {
				return nil, posErr
			}
			if _, err = tx.ExecContext(ctx, insertSQL, encoded.run, transition.Seq, pos, transition.EventID, encoded.kind, encoded.semantics, encoded.event, encoded.from, encoded.to, encoded.facts, transition.To.Step, transition.To.Attempt, transition.To.RequestRev); err != nil {
				return nil, err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE runs SET stored_context_seq=? WHERE id=?`, transition.Seq, encoded.run); err != nil {
				return nil, err
			}
			if err = w.addTotals(ctx, tx, encoded.run, delta(transition)); err != nil {
				return nil, err
			}
			if err = tx.Commit(); err != nil {
				return nil, err
			}
			committed.Store(true)
		}
		return args[0], nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.Exec(`ALTER TABLE runs RENAME COLUMN context_seq TO stored_context_seq; ALTER TABLE runs ADD COLUMN context_seq INTEGER GENERATED ALWAYS AS (` + name + `(stored_context_seq)) VIRTUAL`); err != nil {
		t.Fatal(err)
	}
	w.db.SetMaxIdleConns(0)
	r := readOnly(t, file)
	armed.Store(true)
	got, err := r.Context(ctx, before)
	if err != nil || !committed.Load() || !got.Equal(harness.InitialContext()) {
		t.Fatalf("mixed context snapshot: %+v %v committed=%v", got, err, committed.Load())
	}
	if _, err := r.Context(ctx, before); !errors.Is(err, harness.ErrStaleSnapshot) {
		t.Fatalf("old snapshot accepted: %v", err)
	}
}

func TestHandoverOpensTheMaterializedHeadAtomically(t *testing.T) {
	ctx := context.Background()
	file := tempPath(t)
	s := open(t, file)
	d := newDriver(t, s, createRun(t, s, "materialized-handover", core.Limits{MaxSteps: 10, MaxAttempts: 3}))
	imported := t.TempDir()
	original := filepath.Join(imported, "source")
	if err := os.WriteFile(original, []byte("EXACT_EARLIER_BYTES"), 0600); err != nil {
		t.Fatal(err)
	}
	retrieval, err := (archive.Reader{Imports: imported}).Retrieve(ctx, archive.RetrievalQuery{Operation: "read", Path: "/canti/imports/source", Reason: "freeze earlier evidence"})
	if err != nil || retrieval.Error != "" {
		t.Fatalf("retrieval: %+v %v", retrieval, err)
	}
	d.decide(execBody(`canti:retrieve {"operation":"read","path":"/canti/imports/source","reason":"capture"}`), harness.Usage{})
	d.do(core.Performed{EffectID: d.snap.State.EffectID}, harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeDone, Retrieval: &retrieval}})
	// A handover at step 2, after the resident retrieval.
	const text = "phase one: the source was captured"
	d.freeze()
	d.send()
	body := []byte(execBody("canti:handover " + text))
	d.save(body, harness.Usage{})
	d.load(body)
	before := d.snap
	seq := before.Seq + 1
	head := harness.ContextState{AnchorSeq: seq, FirstStep: before.State.Step + 1}
	outcome := &harness.Outcome{Status: harness.OutcomeDone}
	if err := d.try(core.Performed{EffectID: before.State.EffectID}, harness.Facts{Outcome: outcome}); err == nil {
		t.Fatal("a handover outcome without its head committed")
	}
	if got := latestOf(t, s, before.State.RunID); got != before {
		t.Fatal("a headless handover advanced progress")
	}
	d.do(core.Performed{EffectID: before.State.EffectID}, harness.Facts{Outcome: outcome, ContextState: &head})
	saved := d.commits[len(d.commits)-1]
	if err := s.Commit(ctx, saved); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, file)
	if got, err := reopened.Context(ctx, d.snap); err != nil || !got.Equal(head) {
		t.Fatalf("handover head after reopen: %+v %v", got, err)
	}
	if got := totalsOf(t, reopened, before.State.RunID); got.Steps != 2 {
		t.Fatalf("handover progress counted twice: %+v", got)
	}
	// A second handover opens a newer head.
	d.freeze()
	d.send()
	second := []byte(execBody("canti:handover phase two"))
	d.save(second, harness.Usage{})
	d.load(second)
	before = d.snap
	seq = before.Seq + 1
	next := harness.ContextState{AnchorSeq: seq, FirstStep: before.State.Step + 1}
	d.do(core.Performed{EffectID: before.State.EffectID}, harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeDone}, ContextState: &next})
	if got, err := reopened.Context(ctx, d.snap); err != nil || !got.Equal(next) {
		t.Fatalf("replacement lost the newer handover: %+v %v", got, err)
	}
}

func TestContextHeadRequiresSuccessfulHandoverAndRealProvenance(t *testing.T) {
	for _, damage := range []string{"unrelated-performed", "missing-handover", "failed-handover", "fabricated-anchor", "mismatched-outcome"} {
		t.Run(damage, func(t *testing.T) {
			ctx := context.Background()
			s := open(t, tempPath(t))
			d := newDriver(t, s, createRun(t, s, "head-provenance", core.Limits{MaxSteps: 5, MaxAttempts: 3}))
			text := "phase one"
			if damage == "unrelated-performed" {
				d.decide(execBody("canti:unknown"), harness.Usage{})
				d.perform()
				if _, err := s.db.Exec(`UPDATE transitions SET facts=json_set(facts,'$.ContextState',json('{"AnchorSeq":0,"FirstStep":1}')) WHERE run_id=? AND seq=?`, d.snap.State.RunID, d.snap.Seq); err != nil {
					t.Fatal(err)
				}
			} else {
				d.decide(execBody("canti:handover "+text), harness.Usage{})
				before := d.snap
				seq := before.Seq + 1
				head := harness.ContextState{AnchorSeq: seq, FirstStep: before.State.Step + 1}
				d.do(core.Performed{EffectID: before.State.EffectID}, harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeDone}, ContextState: &head})
				var mutation string
				switch damage {
				case "missing-handover":
					mutation = `json_remove(facts,'$.Outcome')`
				case "failed-handover":
					mutation = `json_set(facts,'$.Outcome.Status','failed')`
				case "fabricated-anchor":
					mutation = `json_set(facts,'$.ContextState.AnchorSeq',3)`
				case "mismatched-outcome":
					mutation = `json_set(facts,'$.Outcome.Detail','another phase')`
				}
				if _, err := s.db.Exec(`UPDATE transitions SET facts=`+mutation+` WHERE run_id=? AND seq=?`, d.snap.State.RunID, d.snap.Seq); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.db.Exec(`UPDATE runs SET context_seq=? WHERE id=?`, d.snap.Seq, d.snap.State.RunID); err != nil {
				t.Fatal(err)
			}
			if got, err := s.Context(ctx, d.snap); err == nil {
				t.Fatalf("corrupt %s context head accepted: %+v", damage, got)
			}
		})
	}
}

func TestAutomaticRejectionRequiresRecordedSignalEvidence(t *testing.T) {
	for _, mode := range []string{"reset", "exhausted", "terminal"} {
		for _, shape := range []string{"empty", "status-only", "discriminator-only"} {
			t.Run(mode+"/"+shape, func(t *testing.T) {
				s := open(t, tempPath(t))
				attempts := 3
				if mode == "exhausted" {
					attempts = 1
				}
				d := newDriver(t, s, createRun(t, s, "signal-evidence", core.Limits{MaxAttempts: attempts}))
				d.freeze()
				d.send()
				var signal harness.ContextLimitError
				if shape == "status-only" {
					signal.Status = 400
				}
				if shape == "discriminator-only" {
					signal.Code = "context_length_exceeded"
				}
				facts := harness.Facts{Response: &harness.Response{Body: []byte(`{}`)}, Failure: "automatic overflow reset", Rejection: &harness.ContextRejection{Previous: harness.InitialContext(), Signal: signal}}
				var event core.Event = core.RequestRejected{Step: 1, Attempt: 1}
				if mode == "reset" {
					facts.ContextState = &harness.ContextState{AnchorSeq: d.snap.Seq + 1, FirstStep: 1, Rejection: &harness.RejectionRef{Seq: d.snap.Seq + 1, Step: 1}}
				}
				if mode == "terminal" {
					event = core.StopRequested{Reason: core.StopUnrecoverable}
				}
				before := d.snap
				if err := d.try(event, facts); err == nil {
					t.Fatalf("automatic %s accepted missing signal evidence: %+v", mode, signal)
				}
				if got := latestOf(t, s, before.State.RunID); got != before {
					t.Fatal("invalid rejection changed progress")
				}
			})
		}
	}
}

type completionQueryDriver struct {
	inner  sqldriver.Driver
	record func(string)
}

func (d completionQueryDriver) Open(name string) (sqldriver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &completionQueryConn{Conn: conn, record: d.record}, nil
}

type completionQueryConn struct {
	sqldriver.Conn
	record func(string)
}

func (c *completionQueryConn) QueryContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	c.record(query)
	underlying, ok := c.Conn.(sqldriver.QueryerContext)
	if !ok {
		return nil, sqldriver.ErrSkip
	}
	return underlying.QueryContext(ctx, query, args)
}

func TestCompletionAtStepUsesUniqueTargetIndexWithUnrelatedHistory(t *testing.T) {
	ctx := context.Background()
	file := tempPath(t)
	s := open(t, file)
	seed(t, s, "lookup-target", 128)
	seed(t, s, "unrelated-run", 96)
	original := s.db
	t.Cleanup(func() { original.Close() })
	var captured []string
	name := fmt.Sprintf("completion_query_%d", contextSnapshotFunctions.Add(1))
	sql.Register(name, completionQueryDriver{inner: original.Driver(), record: func(query string) { captured = append(captured, query) }})
	wrapped, err := sql.Open(name, (&url.URL{Scheme: "file", OmitHost: true, Path: file, RawQuery: "mode=ro"}).String())
	if err != nil {
		t.Fatal(err)
	}
	s.db = wrapped
	for _, target := range []struct {
		run  string
		step int
	}{{"lookup-target", 1}, {"lookup-target", 128}, {"unrelated-run", 64}} {
		got, err := s.CompletionAtStep(ctx, target.run, target.step)
		if err != nil || got.Step != target.step {
			t.Fatalf("target %+v: %+v %v", target, got, err)
		}
	}
	if len(captured) != 3 {
		t.Fatalf("lookup query count=%d want3", len(captured))
	}
	for i, query := range captured {
		if query != captured[0] {
			t.Fatal("target changes changed lookup SQL")
		}
		details := plan(t, s, query, []any{"lookup-target", 1}...)
		// plan invokes the production query captured above, not a mirrored SQL
		// literal. The expression target and run must both be indexed searches.
		if !strings.Contains(details, "SEARCH transitions USING INDEX transitions_completion_step (run_id=? AND <expr>=?)") || strings.Contains(details, "SCAN") || strings.Contains(details, "TEMP B-TREE") {
			t.Fatalf("lookup%d query plan=%s", i, details)
		}
	}
	rows, err := original.QueryContext(ctx, `PRAGMA index_list('transitions')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var seq, unique, partial int
		var index, origin string
		if err := rows.Scan(&seq, &index, &unique, &origin, &partial); err != nil {
			t.Fatal(err)
		}
		if index == "transitions_completion_step" {
			found = true
			if unique != 1 || partial != 1 {
				t.Fatalf("completion target index is not unique/partial: %d %d", unique, partial)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("completion target index absent")
	}
}

func TestRejectionPointerKeepsTheHeadAcrossTerminalRejections(t *testing.T) {
	for _, form := range []string{"minimal-floor", "exhaustion"} {
		t.Run(form, func(t *testing.T) {
			ctx := context.Background()
			path := tempPath(t)
			s := open(t, path)
			attempts := 3
			if form == "exhaustion" {
				attempts = 2
			}
			d := newDriver(t, s, createRun(t, s, "rejection-after-head", core.Limits{MaxSteps: 5, MaxAttempts: attempts}))
			signal := harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}
			freeze := func(optional bool) harness.ContextState {
				current, err := s.Context(ctx, d.snap)
				if err != nil {
					t.Fatal(err)
				}
				capture := &harness.ContextCapture{Anchor: current}
				if optional {
					capture.Bytes.Observations = 1
				}
				request := requestFor(d.snap.State)
				request.ContextState, request.Context = &current, capture
				d.do(core.RequestFrozen{}, harness.Facts{Request: request})
				d.send()
				return current
			}
			// The first rejection opens a minimal head: no resident step, but
			// optional content shown by the frozen capture.
			current := freeze(true)
			seq := d.snap.Seq + 1
			rejection := &harness.ContextRejection{Previous: current, Signal: signal, OptionalContent: true}
			head, class := rejection.Head(seq, 1, current)
			if class != harness.RejectionOpensHead || !head.Minimal {
				t.Fatalf("first rejection head = %+v, %v", head, class)
			}
			d.do(core.RequestRejected{Step: 1, Attempt: 1}, harness.Facts{Response: &harness.Response{Body: []byte(`{}`)},
				Failure: "confirmed overflow", Rejection: rejection, ContextState: &head})
			// A later rejection of the minimal head opens no head: either the
			// minimal floor is terminal, or the attempts are exhausted.
			current = freeze(false)
			rejection = &harness.ContextRejection{Previous: current, Signal: signal, Minimal: true}
			switch form {
			case "minimal-floor":
				d.do(core.StopRequested{Reason: core.StopUnrecoverable}, harness.Facts{Response: &harness.Response{Body: []byte(`{}`)},
					Failure: "provider rejected the minimal context", Rejection: rejection})
			case "exhaustion":
				d.do(core.RequestRejected{Step: 1, Attempt: 2}, harness.Facts{Response: &harness.Response{Body: []byte(`{}`)},
					Failure: "confirmed overflow", Rejection: rejection})
			}
			var pointer int64
			if err := s.db.QueryRow(`SELECT rejection_seq FROM runs WHERE id='rejection-after-head'`).Scan(&pointer); err != nil {
				t.Fatal(err)
			}
			if pointer != seq {
				t.Fatalf("a rejection that opened no head moved the pointer to %d, want %d", pointer, seq)
			}
			reopened := open(t, path)
			got, err := reopened.Context(ctx, d.snap)
			if err != nil || got.AnchorSeq != seq || !got.Minimal || got.Rejection == nil || got.Rejection.Seq != seq {
				t.Fatalf("reopened head = %+v, %v", got, err)
			}
		})
	}
}
