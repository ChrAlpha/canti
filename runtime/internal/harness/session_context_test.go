package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

type sessionFixture struct {
	Journal
	runs     []Run
	states   map[string]core.State
	steps    map[string]int
	requests map[string]Request
	recent   map[string]StepRecord
	failing  string
	before   int64
	limit    int
}

func (j *sessionFixture) SessionRuns(_ context.Context, _ string, before int64, limit int) ([]Run, error) {
	j.before, j.limit = before, limit
	return j.runs, nil
}

func TestPublishSessionLinksPreviousRun(t *testing.T) {
	j := &sessionFixture{runs: []Run{{ID: "r1", SessionID: "s", SessionSeq: 1}}}
	dir := t.TempDir()
	if err := publishSession(context.Background(), j, dir, Run{ID: "r2", SessionID: "s", SessionSeq: 2, InputID: "i2"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "session"))
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		SessionID string `json:"session_id"`
		Sequence  int64  `json:"sequence"`
		Previous  string `json:"previous_run_id"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.SessionID != "s" || got.Sequence != 2 || got.Previous != "r1" {
		t.Fatalf("session metadata = %+v", got)
	}
}

func TestSessionContextUsesOneNavigationLinkWithoutDialogue(t *testing.T) {
	j := &sessionFixture{runs: []Run{{ID: "prior", SessionID: "s", SessionSeq: 1, Goal: "OLD_GOAL_SENTINEL"}}}
	l := &Loop{journal: j}
	f := testFrame()
	if err := l.sessionContext(context.Background(), Run{ID: "current", SessionID: "s", SessionSeq: 2}, &f); err != nil {
		t.Fatal(err)
	}
	if j.limit != 1 || j.before != 2 || !strings.Contains(f.SessionContext, "/canti/runs/prior/") || strings.Contains(f.SessionContext, "OLD_GOAL_SENTINEL") {
		t.Fatalf("navigation=%q limit=%d", f.SessionContext, j.limit)
	}
	for _, prior := range []Run{{ID: "prior", SessionID: "other", SessionSeq: 1}, {ID: "prior", SessionID: "s", SessionSeq: 2}} {
		j.runs = []Run{prior}
		if err := l.sessionContext(context.Background(), Run{ID: "current", SessionID: "s", SessionSeq: 2}, &f); err == nil {
			t.Fatal("invalid session link accepted")
		}
	}
}
