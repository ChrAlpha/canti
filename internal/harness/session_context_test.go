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
	steps  map[string]int
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

func (j *sessionFixture) Totals(_ context.Context, id string) (Totals, error) {
	return Totals{Steps: j.steps[id]}, nil
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

func TestSessionContextKeepsDialogueAcrossStoppedRuns(t *testing.T) {
	j := &sessionFixture{
		runs: []Run{{ID: "r3", Goal: "interrupted again", SessionID: "s", SessionSeq: 3}, {ID: "r2", Goal: "stopped request", SessionID: "s", SessionSeq: 2}, {ID: "r1", Goal: "older", SessionID: "s", SessionSeq: 1}},
		states: map[string]core.State{
			"r3": {Phase: core.PhaseStopped, Reason: core.StopCancelled},
			"r2": {Phase: core.PhaseStopped, Reason: core.StopCancelled, Decision: core.Decision{Kind: core.Exec, Payload: "stale payload"}},
			"r1": returned("older answer"),
		},
		steps: map[string]int{"r2": 3},
	}
	l := &Loop{journal: j, cfg: LoopConfig{Limits: DefaultPromptLimits()}}
	f := budgetFrame()
	if err := l.sessionContext(context.Background(), Run{ID: "r4", SessionID: "s", SessionSeq: 4}, &f); err != nil {
		t.Fatal(err)
	}
	c := f.SessionContext
	if strings.Contains(c, "stale payload") {
		t.Fatalf("stopped run misrepresented as returned dialogue: %q", c)
	}
	for _, part := range []string{"/r2/goal, steps, and result): stopped, reason cancelled, completed steps 3; no assistant return", "/r3/goal, steps, and result): stopped, reason cancelled, completed steps 0; no assistant return"} {
		if !strings.Contains(c, part) {
			t.Fatalf("missing stopped status %q: %q", part, c)
		}
	}
	older, answer, stopped, again := strings.Index(c, "older\n"), strings.Index(c, "older answer"), strings.Index(c, "stopped request"), strings.Index(c, "interrupted again")
	if older < 0 || !(older < answer && answer < stopped && stopped < again) {
		t.Fatalf("dialogue before or across stopped runs was lost or reordered: %q", c)
	}
	if err := f.ValidateContext(l.cfg.Limits); err != nil {
		t.Fatal(err)
	}
}

func TestSessionContextStoppedRunKeepsItsStatusWhenItsGoalDoesNotFit(t *testing.T) {
	j := &sessionFixture{
		runs:   []Run{{ID: "r2", Goal: strings.Repeat("G", 100000), SessionID: "s", SessionSeq: 2}, {ID: "r1", Goal: "older", SessionID: "s", SessionSeq: 1}},
		states: map[string]core.State{"r2": {Phase: core.PhaseStopped, Reason: core.StopCancelled}, "r1": returned("older answer")},
	}
	l := &Loop{journal: j, cfg: LoopConfig{Limits: DefaultPromptLimits()}}
	f := budgetFrame()
	if err := l.sessionContext(context.Background(), Run{ID: "r3", SessionID: "s", SessionSeq: 3}, &f); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.SessionContext, "stopped, reason cancelled") || strings.Contains(f.SessionContext, "G") || strings.Contains(f.SessionContext, "older answer") {
		t.Fatalf("stopped status was not kept alone at the budget boundary: %q", f.SessionContext)
	}
}

func TestSessionContextStoppedRunAtTheBudgetBoundaries(t *testing.T) {
	l := &Loop{cfg: LoopConfig{Limits: DefaultPromptLimits()}}
	answer := strings.Repeat("Ω", 10000)
	regime := func(n int) int {
		l.journal = &sessionFixture{
			runs:   []Run{{ID: "r2", Goal: strings.Repeat("Ψ", n), SessionID: "s", SessionSeq: 2}, {ID: "r1", Goal: "older", SessionID: "s", SessionSeq: 1}},
			states: map[string]core.State{"r2": {Phase: core.PhaseStopped, Reason: core.StopCancelled}, "r1": returned(answer)},
		}
		f := budgetFrame()
		if err := l.sessionContext(context.Background(), Run{ID: "r3", SessionID: "s", SessionSeq: 3}, &f); err != nil {
			t.Fatal(err)
		}
		if err := f.ValidateContext(l.cfg.Limits); err != nil {
			t.Fatalf("goal of %d runes: %v", n, err)
		}
		c := f.SessionContext
		goal, older := strings.Count(c, "Ψ"), strings.Contains(c, answer)
		switch {
		case !strings.Contains(c, "stopped, reason cancelled") || goal != 0 && goal != n || older && goal != n || strings.Contains(c, "Ω") && !older:
			t.Fatalf("goal of %d runes: a unit was cut or skipped over: %q", n, c)
		case older:
			return 2
		case goal == n:
			return 1
		}
		return 0
	}
	edge := func(r int) int {
		lo, hi := 0, 1<<16
		for lo < hi {
			if mid := (lo + hi) / 2; regime(mid) < r {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		return lo
	}
	whole, alone := edge(2), edge(1)
	if regime(0) != 2 || whole == 0 || whole >= alone || alone >= 1<<16 {
		t.Fatalf("edges %d and %d", whole, alone)
	}
	for n, want := range map[int]int{whole - 1: 2, whole: 1, alone - 1: 1, alone: 0} {
		if got := regime(n); got != want {
			t.Fatalf("goal of %d runes: regime %d, want %d", n, got, want)
		}
	}
	t.Logf("older dialogue fits below a stopped goal of %d runes, the whole stopped unit below %d", whole, alone)
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
