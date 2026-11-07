package harness_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

// TestLegacyJournals opens every journal of the corpus with the current
// store, read-only and then writable, reads it through every journal,
// session, and context read, and requires that no recorded row changes,
// apart from the session table the migration of a schema-1 journal adds and
// the index of performed steps that every writable open adds.
func TestLegacyJournals(t *testing.T) {
	names := legacyCaseNames(t)
	if len(names) == 0 {
		t.Fatal("the legacy corpus is empty")
	}
	var stored int64
	for _, name := range names {
		t.Run(name[:7]+"/"+filepath.Base(name), func(t *testing.T) { stored += testLegacyJournal(t, name) })
	}
	if stored > legacyMaxCorpus {
		t.Errorf("the legacy corpus stores %d bytes, more than %d", stored, legacyMaxCorpus)
	}
}

func testLegacyJournal(t *testing.T, name string) int64 {
	c := openLegacyCase(t, name)
	before := snapshotLegacy(t, c.db)
	summary := legacySummary(t, before)
	summary.DB = c.manifest.Journal.DB
	if !reflect.DeepEqual(summary, c.manifest.Journal) {
		t.Fatalf("the journal does not match its manifest:\n got %+v\nwant %+v", summary, c.manifest.Journal)
	}
	checkLegacyManifest(t, c, before)
	runs := before.runs(t)
	paused := runs[len(runs)-1].text(t, "id")
	ctx := context.Background()
	store, err := sqlite.OpenReadOnly(ctx, c.db)
	if err != nil {
		t.Fatal(err)
	}
	exerciseLegacyReads(t, store, before, paused, before.version != 1)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(c.db); err != nil || legacyHash(data) != c.manifest.Journal.DB.SHA256 {
		t.Fatalf("the read-only store changed the journal file: %v", err)
	}
	compareLegacy(t, before, snapshotLegacy(t, c.db), before.version, false)
	if store, err = sqlite.Open(ctx, c.db); err != nil {
		t.Fatal(err)
	}
	exerciseLegacyReads(t, store, before, paused, true)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	compareLegacy(t, before, snapshotLegacy(t, c.db), 2, true)
	return c.stored
}

// checkLegacyManifest checks the manifest's claims about the journal: one
// scripted reply per send, each to the bytes frozen for that attempt, the
// recorded producer paths, and the producer's last capacity check.
func checkLegacyManifest(t *testing.T, c legacyCopy, db legacyDB) {
	t.Helper()
	m, runs := c.manifest, db.runs(t)
	sends := 0
	for _, row := range runs {
		var run harness.Run
		var cfg legacyConfig
		if err := json.Unmarshal([]byte(row.text(t, "run")), &run); err != nil {
			t.Fatal(err)
		}
		if err := legacyStrict(run.Config, &cfg); err != nil || cfg.Workspace != m.Paths.Workspace || cfg.HistoryImports != m.Paths.Imports {
			t.Fatalf("run %s config %+v does not match the recorded paths %+v: %v", run.ID, cfg, m.Paths, err)
		}
		if m.Capacity != nil && row.text(t, "id") == runs[len(runs)-1].text(t, "id") &&
			(cfg.ContextBytes != m.Capacity.ContextBytes || !reflect.DeepEqual(legacyLastCheck(t, db, run.ID), m.Capacity.Last)) {
			t.Fatalf("run %s does not record the manifest's last capacity check %+v", run.ID, m.Capacity.Last)
		}
		frozen := map[[2]int64][]byte{}
		reply := func(v legacyView) legacyReply {
			i := slices.IndexFunc(m.Replies, func(p legacyReply) bool {
				return int64(p.Run) == row.num(t, "n") && p.Step == v.Step && p.Attempt == v.Attempt
			})
			if i < 0 {
				t.Fatalf("run %s step %d attempt %d has no reply", run.ID, v.Step, v.Attempt)
			}
			return m.Replies[i]
		}
		for _, r := range db.transitions(t, run.ID) {
			v := legacyViewOf(t, r.text(t, "to_state"))
			switch r.text(t, "kind") {
			case "request_frozen":
				frozen[[2]int64{r.num(t, "step"), r.num(t, "rev")}] = legacyFactsOf(t, r).Request.Body
			case "attempt_started":
				if reply(v).Request != legacyHash(frozen[[2]int64{int64(v.Step), int64(v.RequestRev)}]) {
					t.Fatalf("run %s step %d attempt %d has no reply to its frozen request", run.ID, v.Step, v.Attempt)
				}
				sends++
			case "response_saved":
				var body struct{ Content []struct{ Text string } }
				p := reply(v)
				if err := json.Unmarshal(legacyFactsOf(t, r).Response.Body, &body); err != nil || p.Hold || len(body.Content) != 1 || body.Content[0].Text != p.Decision {
					t.Fatalf("run %s step %d attempt %d saved another response than the scripted %q: %v", run.ID, v.Step, v.Attempt, p.Decision, err)
				}
			}
		}
	}
	if sends != len(m.Replies) {
		t.Fatalf("the journal records %d sends, the manifest %d replies", sends, len(m.Replies))
	}
}
