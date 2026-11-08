package sqlite

import (
	"context"
	sqldriver "database/sql/driver"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
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
