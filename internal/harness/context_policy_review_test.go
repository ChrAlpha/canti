package harness_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestRetrievalRenderedSourceOverheadDoesNotHideNewestObservation(t *testing.T) {
	for _, changed := range []bool{false, true} {
		name := "unchanged"
		if changed {
			name = "changed"
		}
		t.Run(name, func(t *testing.T) {
			testRetrievalRenderedSourceOverhead(t, changed)
		})
	}
}

func testRetrievalRenderedSourceOverhead(t *testing.T, changed bool) {
	archive := t.TempDir()
	relative := strings.Repeat(strings.Repeat("a", 100)+"/", 20) + "evidence"
	file := filepath.Join(archive, relative)
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(strings.Repeat("x", 10000)), 0600); err != nil {
		t.Fatal(err)
	}
	query, err := json.Marshal(harness.RetrievalQuery{
		Operation: "search", Path: "/canti/imports/" + relative, Reason: "recover earlier facts", Query: "x", Limit: 8192,
	})
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, limits, harness.Budget{}, execReply("canti:retrieve "+string(query)), returnReply("done"))
	f.cfg.Reader = &harness.Reader{Imports: archive}
	f.cfg.WindowBytes = 48 << 10
	ctx := f.bounded()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Step == 2 {
			break
		}
	}
	if changed {
		if err := os.WriteFile(file, []byte(strings.Repeat("y", 10000)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.fresh().Run(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if len(f.model.sent) != 2 {
		t.Fatalf("sent %d requests", len(f.model.sent))
	}
	last := string(f.model.sent[1])
	if !strings.Contains(last, "### Step 1") || !strings.Contains(last, "Retrieval continuation:") {
		t.Fatal("source citation overhead caused the whole newest observation and its continuation to disappear")
	}
	if changed && !strings.Contains(last, "sha256 mismatch") {
		t.Fatal("source change diagnostic did not reach the model")
	}
}

func TestEmptyWindowAblationRecoversRetriesAndProtocolRepair(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 6, MaxAttempts: 3}, harness.Budget{},
		execReply("echo first-window-marker"),
		reply{err: errors.New("transport interruption")},
		reply{body: "invalid decision", usage: usage},
		execReply("echo second-window-marker"),
		returnReply("done"))
	f.cfg.WindowSteps = 1
	f.cfg.WithoutHandover = true
	ctx := f.bounded()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Intent() == core.IntentHalt {
			if s.State.Phase != core.PhaseReturned {
				t.Fatalf("unexpected halt: %+v", s.State)
			}
			break
		}
	}
	if len(f.model.sent) != 5 || !bytes.Equal(f.model.sent[1], f.model.sent[2]) {
		t.Fatal("ablation transport retry did not retain frozen request")
	}
	for _, revision := range []int{1, 2} {
		req, err := f.store.Request(ctx, runID, 2, revision)
		if err != nil || req.Window == nil || req.Window.FirstStep != 2 || !req.Window.WithoutHandover || req.Window.Requested || req.Window.Handover != "" {
			t.Fatalf("revision %d window=%+v err=%v", revision, req.Window, err)
		}
	}
	for _, request := range f.model.sent[1:] {
		if strings.Contains(string(request), "first-window-marker") || strings.Contains(string(request), "Handover required") || strings.Contains(string(request), "## Notes") {
			t.Fatal("empty-window ablation inherited old work or requested handover")
		}
	}
	last := string(f.model.sent[4])
	if strings.Contains(last, "second-window-marker") || !strings.Contains(last, "This window begins at step 3") {
		t.Fatal("second ablation reset did not establish an empty new window")
	}
}
