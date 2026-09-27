package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

type sessionFixture struct {
	Journal
	runs   []Run
	states map[string]core.State
	before int64
	limit  int
}

func (j *sessionFixture) SessionRuns(_ context.Context, _ string, before int64, limit int) ([]Run, error) {
	j.before, j.limit = before, limit
	return j.runs, nil
}

func (j *sessionFixture) Latest(_ context.Context, id string) (Snapshot, error) {
	return Snapshot{State: j.states[id]}, nil
}

func returned(payload string) core.State {
	return core.State{Phase: core.PhaseReturned, Decision: core.Decision{Kind: core.Return, Payload: payload}}
}

func TestSessionContextUsesCommittedEarlierPairs(t *testing.T) {
	j := &sessionFixture{
		runs:   []Run{{ID: "r2", Goal: "second request", SessionID: "s", SessionSeq: 2}, {ID: "r1", Goal: "first request", SessionID: "s", SessionSeq: 1}},
		states: map[string]core.State{"r2": returned("second answer"), "r1": returned("first answer")},
	}
	l := &Loop{journal: j, cfg: LoopConfig{Limits: DefaultPromptLimits()}}
	f := budgetFrame()
	if err := l.sessionContext(context.Background(), Run{ID: "r3", Goal: f.Goal, SessionID: "s", SessionSeq: 3}, &f); err != nil {
		t.Fatal(err)
	}
	if j.before != 3 || j.limit != sessionContextCandidates {
		t.Fatalf("SessionRuns before=%d limit=%d", j.before, j.limit)
	}
	for _, part := range []string{"r2", "second request", "second answer", "r1", "first request", "first answer", "Current goal and constraints are authoritative"} {
		if !strings.Contains(f.SessionContext, part) {
			t.Fatalf("missing %q in context: %q", part, f.SessionContext)
		}
	}
	if strings.Index(f.SessionContext, "first answer") > strings.Index(f.SessionContext, "second request") {
		t.Fatalf("dialogue is not chronological: %q", f.SessionContext)
	}
	if err := f.ValidateContext(l.cfg.Limits); err != nil {
		t.Fatal(err)
	}
	p, err := f.Prompt(l.cfg.Limits)
	if err != nil || !strings.Contains(strings.Join(p.Blocks, ""), "Earlier session dialogue") {
		t.Fatalf("prompt = %#v, %v", p, err)
	}
}

func TestSessionContextDropsOversizedPairIntact(t *testing.T) {
	j := &sessionFixture{
		runs:   []Run{{ID: "r2", Goal: "large", SessionID: "s", SessionSeq: 2}, {ID: "r1", Goal: "older", SessionID: "s", SessionSeq: 1}},
		states: map[string]core.State{"r2": returned(strings.Repeat("X", 100000)), "r1": returned("older answer")},
	}
	l := &Loop{journal: j, cfg: LoopConfig{Limits: DefaultPromptLimits()}}
	f := budgetFrame()
	if err := l.sessionContext(context.Background(), Run{ID: "r3", SessionID: "s", SessionSeq: 3}, &f); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(f.SessionContext, "X") || strings.Contains(f.SessionContext, "older answer") {
		t.Fatalf("pair was cut or older dialogue skipped over it: %q", f.SessionContext)
	}
	if !strings.Contains(f.SessionContext, "/r2/session") {
		t.Fatalf("missing raw navigation: %q", f.SessionContext)
	}
	if err := f.ValidateContext(l.cfg.Limits); err != nil {
		t.Fatalf("optional history rejected legal current input: %v", err)
	}
}

func TestSessionContextStoppedRunIsNotAssistantAnswer(t *testing.T) {
	j := &sessionFixture{
		runs:   []Run{{ID: "r2", Goal: "stopped request", SessionID: "s", SessionSeq: 2}, {ID: "r1", Goal: "older", SessionID: "s", SessionSeq: 1}},
		states: map[string]core.State{"r2": {Phase: core.PhaseStopped, Reason: core.StopCancelled, Decision: core.Decision{Payload: "stale payload"}}, "r1": returned("older answer")},
	}
	l := &Loop{journal: j, cfg: LoopConfig{Limits: DefaultPromptLimits()}}
	f := budgetFrame()
	if err := l.sessionContext(context.Background(), Run{ID: "r3", SessionID: "s", SessionSeq: 3}, &f); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(f.SessionContext, "stale payload") || strings.Contains(f.SessionContext, "older answer") {
		t.Fatalf("stopped run misrepresented as returned dialogue: %q", f.SessionContext)
	}
	if !strings.Contains(f.SessionContext, "/r2/session") {
		t.Fatalf("missing stopped source: %q", f.SessionContext)
	}
	if !strings.Contains(f.SessionContext, "stopped") || !strings.Contains(f.SessionContext, "cancelled") {
		t.Fatalf("missing stopped status and reason: %q", f.SessionContext)
	}
}

func TestSessionContextRejectsNonterminalEarlierRun(t *testing.T) {
	j := &sessionFixture{runs: []Run{{ID: "r1", SessionID: "s", SessionSeq: 1}}, states: map[string]core.State{"r1": {Phase: core.PhaseIdle}}}
	l := &Loop{journal: j, cfg: LoopConfig{Limits: DefaultPromptLimits()}}
	f := budgetFrame()
	if err := l.sessionContext(context.Background(), Run{ID: "r2", SessionID: "s", SessionSeq: 2}, &f); err == nil || !strings.Contains(err.Error(), "not terminal") {
		t.Fatalf("nonterminal prior run: %v", err)
	}
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
