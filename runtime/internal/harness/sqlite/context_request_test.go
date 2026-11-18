package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestContextRequestSelectsOnlyFrozenRevision(t *testing.T) {
	s, d := contextHistory(t)
	for _, tc := range []struct{ step, revision, index int }{
		{0, 0, 11}, {1, 0, 4}, {1, 1, 0}, {1, 2, 4}, {2, 1, 11},
	} {
		got, err := s.ContextRequest(context.Background(), "run-1", tc.step, tc.revision)
		if err != nil || !reflect.DeepEqual(got, d.commits[tc.index]) {
			t.Fatalf("ContextRequest(%d, %d) = %#v, %v; want %#v", tc.step, tc.revision, got, err, d.commits[tc.index])
		}
	}
}

func TestContextRequestIsolatesSelectedRecord(t *testing.T) {
	s, d := contextHistory(t)
	w := d.store.(*Store)
	if _, err := w.db.Exec(`UPDATE transitions SET facts = '{', event = '{', from_state = CASE WHEN kind IN ('observed','resolved','performed') THEN json_set(from_state,'$.Step',9999) ELSE '{' END, to_state = '{', semantics = 999 WHERE seq != 5;
		UPDATE runs SET run = '{', semantics = 999`); err != nil {
		t.Fatal(err)
	}
	got, err := s.ContextRequest(context.Background(), "run-1", 1, 2)
	if err != nil || !reflect.DeepEqual(got, d.commits[4]) {
		t.Fatalf("isolated request = %#v, %v; want %#v", got, err, d.commits[4])
	}
}

func TestContextRequestRejectsMalformedSelectedRecord(t *testing.T) {
	for _, update := range []string{
		`UPDATE transitions SET facts = '{'`,
		`UPDATE transitions SET facts = json_insert(facts, '$.Unknown', 1)`,
		`UPDATE transitions SET facts = '{}'`,
		`UPDATE transitions SET facts = json_set(facts, '$.Request.Body', 'not base64')`,
		`UPDATE transitions SET facts = json_remove(facts,'$.Request.ContextState')`,
		`UPDATE transitions SET facts = json_set(facts,'$.Request.ContextState.AnchorSeq',1)`,
		`UPDATE transitions SET event = json_insert(event, '$.Unknown', 1)`,
		`UPDATE transitions SET semantics = 999`,
		`UPDATE transitions SET from_state = json_set(from_state, '$.RunID', 'other')`,
		`UPDATE transitions SET to_state = json_set(to_state, '$.RunID', 'other')`,
		`UPDATE transitions SET step = 9`,
		`UPDATE transitions SET rev = 9`,
		`PRAGMA ignore_check_constraints=ON; UPDATE transitions SET seq = 0`,
	} {
		t.Run(update, func(t *testing.T) {
			path := tempPath(t)
			w := open(t, path)
			d := newDriver(t, w, createRun(t, w, "run-1", limits))
			d.freeze()
			if _, err := w.db.Exec(update); err != nil {
				t.Fatal(err)
			}
			s := readOnly(t, path)
			if _, err := s.ContextRequest(context.Background(), "run-1", 0, 0); !errors.Is(err, ErrIncompatible) {
				t.Fatalf("malformed selected request error = %v, want ErrIncompatible", err)
			}
		})
	}
}

func TestContextRequestRejectsInvalidSelectionAndMissingRequest(t *testing.T) {
	s, _ := contextHistory(t)
	ctx := context.Background()
	for _, tc := range []struct{ step, revision int }{{-1, 0}, {0, -1}, {0, 1}} {
		if _, err := s.ContextRequest(ctx, "run-1", tc.step, tc.revision); err == nil {
			t.Errorf("accepted invalid selection %#v", tc)
		}
	}
	for _, tc := range []struct {
		runID          string
		step, revision int
	}{{"missing", 0, 0}, {"run-1", 1, 3}, {"run-1", 3, 0}, {"run-1", 3, 1}} {
		if _, err := s.ContextRequest(ctx, tc.runID, tc.step, tc.revision); !errors.Is(err, harness.ErrNotFound) {
			t.Errorf("missing request %#v error = %v", tc, err)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ContextRequest(cancelled, "run-1", 0, 0); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled query error = %v", err)
	}
}

func TestContextRequestBeforeFirstFreezeAndCurrentMetadata(t *testing.T) {
	path := tempPath(t)
	w := open(t, path)
	d := newDriver(t, w, createRun(t, w, "run-1", core.Limits{MaxSteps: 1, MaxAttempts: 1}))
	s := readOnly(t, path)
	if _, err := s.ContextRequest(context.Background(), "run-1", 0, 0); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("before first freeze error = %v", err)
	}
	request := requestFor(d.snap.State)
	d.do(core.RequestFrozen{}, harness.Facts{Request: request})
	s = readOnly(t, path)
	got, err := s.ContextRequest(context.Background(), "run-1", 0, 0)
	if err != nil || !reflect.DeepEqual(got, d.commits[0]) || got.Facts.Request.Context != nil {
		t.Fatalf("legacy request = %#v, %v; want %#v", got, err, d.commits[0])
	}
}
