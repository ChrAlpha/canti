package sqlite

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

// The current journal retains byte-identical acceptance/receipt facts across
// reopen and an acknowledged completion retry; core and wire separately pin
// their exact public encoding.
func TestPersistedRecordsKeepTheirEncoding(t *testing.T) {
	path := tempPath(t)
	s := open(t, path)
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.step("printf ok", harness.Usage{InputTokens: 4}, 2)
	before := recordBytes(t, s, d.snap.Seq)
	last := d.commits[len(d.commits)-1]
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path)
	if err := s.Commit(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	if after := recordBytes(t, s, d.snap.Seq); after != before {
		t.Fatal("same-version retry changed persisted encoding")
	}
	rows, err := s.Transitions(context.Background(), "run-1", 0, 10)
	if err != nil || len(rows) != 5 {
		t.Fatalf("current cycle: %d %v", len(rows), err)
	}
	if rows[3].Facts.Execution == nil || rows[4].Facts.Receipt == nil {
		t.Fatal("current identity or receipt was not persisted")
	}
}
func TestPersistedExecutionIdentityAndResolutionKeepTheirEncoding(t *testing.T) {
	for _, tc := range []struct {
		value any
		want  string
	}{
		{harness.ExecutionIdentity{Spec: "spec", ConfigDigest: "config"}, `{"Spec":"spec","ConfigDigest":"config"}`},
		{harness.Resolution{Source: harness.ResolutionOperator, OwnerID: "owner"}, `{"Source":"operator","OwnerID":"owner"}`},
		{core.EffectRecovered{EffectID: "effect"}, `{"EffectID":"effect"}`},
	} {
		got, err := json.Marshal(tc.value)
		if err != nil || string(got) != tc.want {
			t.Errorf("encoding: %s %v, want %s", got, err, tc.want)
		}
	}
}
