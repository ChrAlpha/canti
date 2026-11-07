package sqlite

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func contextHistory(t *testing.T) (*Store, *driver) {
	t.Helper()
	path := tempPath(t)
	w := open(t, path)
	d := newDriver(t, w, createRun(t, w, "run-1", core.Limits{MaxSteps: 3, MaxAttempts: 4}))
	d.decide([]byte("invalid decision"), harness.Usage{})
	d.freeze()
	d.send()
	d.do(core.ResponseIncomplete{Step: 1, Attempt: 2}, harness.Facts{Failure: "disconnected"})
	d.send()
	d.save(execBody("ls"), harness.Usage{})
	d.load(execBody("ls"))
	d.observe(core.ExecutionExited, 1)
	d.freeze()
	d.send()
	return readOnly(t, path), d
}

func TestContextRecordsSelectsFrozenRevisionsAndPagesTheirInterval(t *testing.T) {
	s, d := contextHistory(t)
	ctx := context.Background()
	for _, tc := range []struct {
		step, revision, index, previous, next int
	}{
		{0, 0, 11, 4, -1},
		{1, 0, 4, 0, 11},
		{1, 1, 0, -1, 4},
		{1, 2, 4, 0, 11},
		{2, 1, 11, 4, -1},
	} {
		got, err := s.ContextRecords(ctx, "run-1", tc.step, tc.revision, 0, 100)
		if err != nil {
			t.Fatalf("ContextRecords(%d, %d): %v", tc.step, tc.revision, err)
		}
		if got.AsOfSeq != d.snap.Seq || !sameRun(got.Run, testRun("run-1")) || !reflect.DeepEqual(got.Request, d.commits[tc.index]) {
			t.Fatalf("selection(%d, %d) = %#v", tc.step, tc.revision, got)
		}
		for label, pair := range map[string]struct {
			got   *harness.Transition
			index int
		}{"previous": {got.Previous, tc.previous}, "next": {got.Next, tc.next}} {
			if pair.index < 0 {
				if pair.got != nil {
					t.Fatalf("%s = %#v, want nil", label, pair.got)
				}
			} else if pair.got == nil || !reflect.DeepEqual(*pair.got, d.commits[pair.index]) {
				t.Fatalf("%s = %#v, want index %d", label, pair.got, pair.index)
			}
		}
		end := len(d.commits)
		if tc.next >= 0 {
			end = tc.next
		}
		want := d.commits[tc.index+1 : end]
		if !reflect.DeepEqual(got.Events, want) || got.More {
			t.Fatalf("events = %#v, more = %v; want %#v", got.Events, got.More, want)
		}
		for _, limit := range []int{1, 2, 3, 100} {
			var all []harness.Transition
			for after := int64(0); ; {
				page, err := s.ContextRecords(ctx, "run-1", tc.step, tc.revision, after, limit)
				if err != nil || len(page.Events) > limit {
					t.Fatalf("page after %d limit %d: %#v, %v", after, limit, page, err)
				}
				all = append(all, page.Events...)
				if !page.More {
					break
				}
				if len(page.Events) == 0 || page.Events[len(page.Events)-1].Seq <= after {
					t.Fatal("page failed to advance")
				}
				after = page.Events[len(page.Events)-1].Seq
			}
			if !reflect.DeepEqual(all, want) {
				t.Fatalf("paged events = %#v, want %#v", all, want)
			}
		}
	}
}

func TestContextRecordsRejectsInvalidSelectionAndPropagatesCancellation(t *testing.T) {
	s, d := contextHistory(t)
	ctx := context.Background()
	for _, tc := range []struct{ step, revision, limit int }{
		{-1, 0, 1}, {0, -1, 1}, {0, 1, 1}, {1, 1, 0}, {1, 1, -1}, {1, 1, 101},
	} {
		if _, err := s.ContextRecords(ctx, "run-1", tc.step, tc.revision, 0, tc.limit); err == nil {
			t.Errorf("accepted invalid selection %#v", tc)
		}
	}
	if _, err := s.ContextRecords(ctx, "run-1", 1, 1, -1, 1); err == nil {
		t.Fatal("accepted negative cursor")
	}
	for _, tc := range []struct{ step, revision int }{{1, 3}, {3, 0}, {3, 1}} {
		if _, err := s.ContextRecords(ctx, "run-1", tc.step, tc.revision, 0, 1); !errors.Is(err, harness.ErrNotFound) {
			t.Errorf("missing request %#v error = %v", tc, err)
		}
	}
	if _, err := s.ContextRecords(ctx, "missing", 0, 0, 0, 1); !errors.Is(err, harness.ErrNotFound) {
		t.Errorf("missing run error = %v", err)
	}
	page, err := s.ContextRecords(ctx, "run-1", 1, 1, d.snap.Seq+100, 1)
	if err != nil || len(page.Events) != 0 || page.More {
		t.Errorf("cursor beyond interval: %#v, %v", page, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ContextRecords(cancelled, "run-1", 0, 0, 0, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled query error = %v", err)
	}
}

func TestContextRecordsBeforeFirstFreezeAndImmediatelyAfterFreeze(t *testing.T) {
	path := tempPath(t)
	w := open(t, path)
	d := newDriver(t, w, createRun(t, w, "run-1", limits))
	s := readOnly(t, path)
	if _, err := s.ContextRecords(context.Background(), "run-1", 0, 0, 0, 1); !errors.Is(err, harness.ErrNotFound) || !strings.Contains(err.Error(), "frozen request") {
		t.Fatalf("before first request error = %v", err)
	}
	d.freeze()
	page, err := s.ContextRecords(context.Background(), "run-1", 0, 0, 0, 1)
	if err != nil || page.Request.Seq != 1 || page.AsOfSeq != 1 || len(page.Events) != 0 || page.More {
		t.Fatalf("immediately after freeze: %#v, %v", page, err)
	}
}

func TestContextRecordsStrictlyDecodesSelectedRecords(t *testing.T) {
	for _, tc := range []struct {
		update string
		step   int
	}{
		{`UPDATE transitions SET facts = json_insert(facts, '$.Unknown', 1) WHERE seq = 1`, 0},
		{`UPDATE transitions SET facts = '{}' WHERE seq = 1`, 0},
		{`UPDATE transitions SET semantics = 999 WHERE seq = 1`, 0},
		{`UPDATE transitions SET step = 9 WHERE seq = 1`, 0},
		{`UPDATE transitions SET rev = 9 WHERE seq = 1`, 0},
		{`UPDATE transitions SET from_state = json_set(from_state, '$.RunID', 'other') WHERE seq = 1`, 0},
		{`UPDATE transitions SET facts = json_insert(facts, '$.Unknown', 1) WHERE seq = 2`, 1},
		{`UPDATE transitions SET facts = json_insert(facts, '$.Unknown', 1) WHERE seq = 5`, 1},
		{`UPDATE runs SET semantics = 999`, 0},
	} {
		t.Run(tc.update, func(t *testing.T) {
			path := tempPath(t)
			w := open(t, path)
			d := newDriver(t, w, createRun(t, w, "run-1", limits))
			d.step("ls", harness.Usage{}, 1)
			d.freeze()
			if _, err := w.db.Exec(tc.update); err != nil {
				t.Fatal(err)
			}
			s := readOnly(t, path)
			if _, err := s.ContextRecords(context.Background(), "run-1", tc.step, 0, 0, 100); !errors.Is(err, ErrIncompatible) {
				t.Fatalf("corrupt records error = %v, want ErrIncompatible", err)
			}
		})
	}
}

func TestContextRecordsBoundsLargeHistoryAndLongRequestInterval(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	w := open(t, path)
	d := newDriver(t, w, createRun(t, w, "run-1", core.Limits{MaxSteps: 301, MaxAttempts: 202}))
	for range 300 {
		d.step("ls", harness.Usage{}, 1)
	}
	d.freeze()
	selected := d.snap.Seq
	for range 200 {
		d.send()
		d.do(core.ResponseIncomplete{Step: d.snap.State.Step, Attempt: d.snap.State.Attempt}, harness.Facts{Failure: "disconnected"})
	}
	if _, err := w.db.Exec(`UPDATE transitions SET facts = '{"Unknown": 1}' WHERE seq < ?`, selected-7); err != nil {
		t.Fatal(err)
	}
	s := readOnly(t, path)
	after, pages, count := int64(0), 0, 0
	for {
		page, err := s.ContextRecords(ctx, "run-1", 0, 0, after, 100)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if page.Request.Seq != selected || len(page.Events) != 100 || page.Previous == nil || page.Previous.To.Step != 300 {
			t.Fatalf("bounded query got seq %d, events %d, previous %#v", page.Request.Seq, len(page.Events), page.Previous)
		}
		count += len(page.Events)
		after = page.Events[len(page.Events)-1].Seq
		if !page.More {
			break
		}
		if pages >= 4 {
			t.Fatal("pagination failed to finish in four pages")
		}
	}
	if pages != 4 || count != 400 || after != d.snap.Seq {
		t.Fatalf("pages = %d, events = %d, cursor = %d; want 4, 400, %d", pages, count, after, d.snap.Seq)
	}
}
