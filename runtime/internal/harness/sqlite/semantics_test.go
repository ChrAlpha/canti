package sqlite

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func recordBytes(t *testing.T, s *Store, through int64) string {
	t.Helper()
	var data string
	err := s.db.QueryRow(`SELECT json_array((SELECT json_array(semantics,run,initial) FROM runs WHERE id='run-1'),(SELECT json_group_array(json_array(seq,event_id,kind,semantics,event,from_state,to_state,facts,step,attempt,rev)) FROM (SELECT * FROM transitions WHERE run_id='run-1' AND seq<=? ORDER BY seq)))`, through).Scan(&data)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func TestSemanticsReadersRefuseUnsupportedVersions(t *testing.T) {
	for _, version := range []int{0, core.SemanticsVersion - 1, core.SemanticsVersion + 1} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			s := open(t, tempPath(t))
			d := newDriver(t, s, createRun(t, s, "run-1", limits))
			d.freeze()
			if _, err := s.db.Exec(`UPDATE runs SET semantics=?`, version); err != nil {
				t.Fatal(err)
			}
			before := recordBytes(t, s, d.snap.Seq)
			if _, err := s.Run(context.Background(), "run-1"); !errors.Is(err, ErrIncompatible) {
				t.Fatalf("unsupported run was read: %v", err)
			}
			if after := recordBytes(t, s, d.snap.Seq); after != before {
				t.Fatal("unsupported read rewrote committed facts")
			}
		})
	}
}
func TestSemanticsCurrentRestartAndLostAcknowledgementKeepFacts(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s := open(t, path)
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.decide(execBody("printf ok"), harness.Usage{})
	before := recordBytes(t, s, d.snap.Seq)
	last := d.commits[len(d.commits)-1]
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path)
	if got := latestOf(t, s, "run-1"); got != d.snap {
		t.Fatalf("restart: %+v, want %+v", got, d.snap)
	}
	if err := s.Commit(ctx, last); err != nil {
		t.Fatalf("lost acknowledgement retry: %v", err)
	}
	if after := recordBytes(t, s, d.snap.Seq); after != before {
		t.Fatal("retry/reopen rewrote committed bytes")
	}
	d = newDriver(t, s, d.snap)
	d.observe(core.ExecutionExited, 2)
	if d.snap.State.Phase != core.PhaseIdle || d.snap.State.Step != 2 {
		t.Fatalf("completion: %+v", d.snap)
	}
}
func TestSemanticsNewWritesUseCurrentVersion(t *testing.T) {
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.freeze()
	var version int
	if err := s.db.QueryRow(`SELECT semantics FROM runs`).Scan(&version); err != nil || version != core.SemanticsVersion {
		t.Fatalf("run version %d: %v", version, err)
	}
	if err := s.db.QueryRow(`SELECT semantics FROM transitions`).Scan(&version); err != nil || version != core.SemanticsVersion {
		t.Fatalf("transition version %d: %v", version, err)
	}
}
