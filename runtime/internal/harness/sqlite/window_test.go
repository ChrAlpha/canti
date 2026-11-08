package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestLatestRequestPreservesWindowOnReopen(t *testing.T) {
	ctx := context.Background()
	p := tempPath(t)
	s, err := Open(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	d := newDriver(t, s, createRun(t, s, "window", limits))
	if _, err := s.LatestRequest(ctx, "window"); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("empty request: %v", err)
	}
	want := harness.Request{Body: []byte("frozen"), Window: &harness.Window{FirstStep: 1, Requested: true}}
	d.do(core.RequestFrozen{}, harness.Facts{Request: &want})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.LatestRequest(ctx, "window")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("request %#v, %v; want %#v", got, err, want)
	}
	rows, err := s.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+latestRequestSQL, "window")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		t.Log(detail)
		if detail == "USE TEMP B-TREE FOR ORDER BY" {
			t.Fatal("unbounded request sort")
		}
	}
}
