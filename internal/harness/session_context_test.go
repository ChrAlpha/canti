package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
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

func (j *sessionFixture) LatestRequest(_ context.Context, id string) (Request, error) {
	r, ok := j.requests[id]
	switch {
	case j.failing == "LatestRequest":
		return Request{}, errors.New("journal read failed")
	case !ok:
		return Request{}, ErrNotFound
	}
	return r, nil
}

func (j *sessionFixture) Recent(_ context.Context, id string, before int64, limit int) ([]StepRecord, error) {
	r, ok := j.recent[id]
	switch {
	case j.failing == "Recent":
		return nil, errors.New("journal read failed")
	case before != 0 || limit != 1:
		return nil, fmt.Errorf("Recent(%d, %d) reads more than the latest step", before, limit)
	case !ok:
		return nil, nil
	}
	return []StepRecord{r}, nil
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
	for _, part := range []string{"/r2/goal, steps, and result): stopped, reason cancelled, recorded steps 3; no assistant return", "/r3/goal, steps, and result): stopped, reason cancelled, recorded steps 0; no assistant return"} {
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

func TestSessionContextStoppedStatusFitsWithNewerDialogue(t *testing.T) {
	l := &Loop{cfg: LoopConfig{Limits: DefaultPromptLimits()}}
	regime := func(n int) int {
		answer := strings.Repeat("N", n)
		l.journal = &sessionFixture{
			runs: []Run{{ID: "r3", Goal: "newer", SessionID: "s", SessionSeq: 3}, {ID: "r2", Goal: strings.Repeat("G", 30000), SessionID: "s", SessionSeq: 2}, {ID: "r1", Goal: "older", SessionID: "s", SessionSeq: 1}},
			states: map[string]core.State{
				"r3": returned(answer),
				"r2": {Phase: core.PhaseStopped, Reason: core.StopCancelled},
				"r1": returned("older answer"),
			},
		}
		f := budgetFrame()
		if err := l.sessionContext(context.Background(), Run{ID: "r4", SessionID: "s", SessionSeq: 4}, &f); err != nil {
			t.Fatal(err)
		}
		if err := f.ValidateContext(l.cfg.Limits); err != nil {
			t.Fatalf("answer of %d bytes: optional history rejected legal current input: %v", n, err)
		}
		c := f.SessionContext
		newer, stopped := strings.Contains(c, answer), strings.Contains(c, "/r2/goal, steps, and result): stopped")
		switch {
		case strings.Contains(c, "G") || strings.Contains(c, "older answer") || stopped && !newer || strings.Contains(c, "NNN") && !newer:
			t.Fatalf("answer of %d bytes: a unit was cut or skipped over: %d bytes", n, len(c))
		case stopped:
			return 2
		case newer:
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
	status, newer := edge(2), edge(1)
	if status < 1000 || status >= newer || newer >= 1<<16 {
		t.Fatalf("edges %d and %d", status, newer)
	}
	for n, want := range map[int]int{status - 1: 2, status: 1, newer - 1: 1, newer: 0} {
		if got := regime(n); got != want {
			t.Fatalf("answer of %d bytes: regime %d, want %d", n, got, want)
		}
	}
}

func handoverPath(run string, step int) string {
	return fmt.Sprintf("Latest handover of this run, agent-written and possibly overtaken by later steps: /canti/runs/%s/steps/%08d/command\n", run, step)
}

// handoverSession is a session of six earlier runs: r6 returned after a
// window its handover in step 5 opened; r5 stopped right after its handover
// in step 4 was accepted; r4 stopped after a failed handover in a window
// opened by step 2; r3 never handed over; r2 stopped before any request; r1
// holds a request of a journal without windows.
func handoverSession() *sessionFixture {
	ok, failed := 0, 1
	exited := func(code *int, h *Handover) Observation {
		return Observation{Execution: core.ExecutionExited, ExitCode: code, Handover: h}
	}
	return &sessionFixture{
		runs: []Run{{ID: "r6", Goal: "sixth", SessionID: "s", SessionSeq: 6}, {ID: "r5", Goal: "fifth", SessionID: "s", SessionSeq: 5},
			{ID: "r4", Goal: "fourth", SessionID: "s", SessionSeq: 4}, {ID: "r3", Goal: "third", SessionID: "s", SessionSeq: 3},
			{ID: "r2", Goal: "second", SessionID: "s", SessionSeq: 2}, {ID: "r1", Goal: "first", SessionID: "s", SessionSeq: 1}},
		states: map[string]core.State{
			"r6": returned("sixth answer"), "r5": {Phase: core.PhaseStopped, Reason: core.StopStepsExhausted},
			"r4": {Phase: core.PhaseStopped, Reason: core.StopCancelled}, "r3": returned("third answer"),
			"r2": {Phase: core.PhaseStopped, Reason: core.StopCancelled}, "r1": returned("first answer"),
		},
		requests: map[string]Request{
			"r6": {Window: &Window{FirstStep: 6, SourceStep: 5, Handover: "h6"}},
			"r5": {Window: &Window{FirstStep: 1, Requested: true, Failures: 1}},
			"r4": {Window: &Window{FirstStep: 3, SourceStep: 2, Handover: "h4", Requested: true, Failures: 1}},
			"r3": {Window: &Window{FirstStep: 1}},
			"r1": {},
		},
		recent: map[string]StepRecord{
			"r6": {Step: 7, Observation: exited(&ok, nil)},
			"r5": {Step: 4, Observation: exited(&ok, &Handover{RunID: "r5", FirstStep: 1, Text: "h5"})},
			"r4": {Step: 5, Observation: exited(&failed, nil)},
			"r3": {Step: 2, Observation: exited(&ok, nil)},
			"r1": {Step: 9, Observation: exited(&ok, nil)},
		},
	}
}

func TestSessionContextPointsToTheLatestHandover(t *testing.T) {
	j := handoverSession()
	l := &Loop{journal: j, cfg: LoopConfig{Limits: DefaultPromptLimits()}}
	f := budgetFrame()
	if err := l.sessionContext(context.Background(), Run{ID: "r7", SessionID: "s", SessionSeq: 7}, &f); err != nil {
		t.Fatal(err)
	}
	c := f.SessionContext
	for run, step := range map[string]int{"r6": 5, "r5": 4, "r4": 2} {
		path := handoverPath(run, step)
		header := strings.Index(c, "Run "+run+" ")
		if strings.Count(c, path) != 1 || header < 0 || !strings.HasPrefix(c[strings.Index(c[header:], "\n")+header+1:], path) {
			t.Errorf("run %s: want its header followed by %q in\n%s", run, path, c)
		}
	}
	if n := strings.Count(c, "Latest handover of this run"); n != 3 {
		t.Errorf("%d handover paths; want none for runs that never handed over or hold no window", n)
	}
	want := map[string]int{"r6": 5, "r5": 4, "r4": 2, "r3": 0, "r2": 0, "r1": 0}
	if len(f.sessionSources) != len(want) {
		t.Fatalf("sources %+v", f.sessionSources)
	}
	for _, src := range f.sessionSources {
		if !src.IncludedGoal || src.HandoverStep != want[src.RunID] {
			t.Errorf("source %+v; want goal included and handover step %d", src, want[src.RunID])
		}
	}
	if err := f.ValidateContext(l.cfg.Limits); err != nil {
		t.Fatal(err)
	}
}

func TestSessionContextReturnsAFailedHandoverRead(t *testing.T) {
	for _, method := range []string{"Recent", "LatestRequest"} {
		j := handoverSession()
		j.failing = method
		l := &Loop{journal: j, cfg: LoopConfig{Limits: DefaultPromptLimits()}}
		f := budgetFrame()
		err := l.sessionContext(context.Background(), Run{ID: "r7", SessionID: "s", SessionSeq: 7}, &f)
		if err == nil || errors.As(err, new(contextError)) || f.SessionContext != "" {
			t.Errorf("failing %s: err %v, context %q; want the read error to retry, with no context", method, err, f.SessionContext)
		}
	}
}

// TestSessionContextAddsHandoverPathsOnlyWhereTheyFit grows the newest run's
// answer, or a stopped run's goal, across the edges where each handover path
// and each unit stop fitting: paths never change which runs and goals are
// selected, and each is left out only where it would not fit.
func TestSessionContextAddsHandoverPathsOnlyWhereTheyFit(t *testing.T) {
	l := &Loop{cfg: LoopConfig{Limits: DefaultPromptLimits()}}
	paths := map[string]int{"r6": 5, "r5": 4, "r4": 2}
	selection := func(j *sessionFixture) Frame {
		t.Helper()
		l.journal = j
		f := budgetFrame()
		if err := l.sessionContext(context.Background(), Run{ID: "r7", SessionID: "s", SessionSeq: 7}, &f); err != nil {
			t.Fatal(err)
		}
		if err := f.ValidateContext(l.cfg.Limits); err != nil {
			t.Fatalf("optional history rejected legal current input: %v", err)
		}
		return f
	}
	sized := func(size func(*sessionFixture), handovers bool) *sessionFixture {
		j := handoverSession()
		size(j)
		if !handovers {
			j.requests, j.recent = nil, nil
		}
		return j
	}
	pointed := func(f Frame, run string) bool {
		for _, src := range f.sessionSources {
			if src.RunID == run {
				return src.HandoverStep != 0
			}
		}
		return false
	}
	dropped := 0
	check := func(size func(*sessionFixture)) {
		t.Helper()
		f, want := selection(sized(size, true)), selection(sized(size, false)).sessionSources
		got := slices.Clone(f.sessionSources)
		if len(got) != len(want) {
			t.Fatalf("paths changed the selection: %+v, without paths %+v", got, want)
		}
		for i := range got {
			step := got[i].HandoverStep
			got[i].HandoverStep = 0
			if got[i] != want[i] || step != 0 && step != paths[got[i].RunID] {
				t.Fatalf("paths changed the selection or point elsewhere: %+v, without paths %+v", f.sessionSources, want)
			}
			if step != 0 || paths[got[i].RunID] == 0 {
				continue
			}
			dropped++
			path := handoverPath(got[i].RunID, paths[got[i].RunID])
			header := strings.Index(f.SessionContext, "Run "+got[i].RunID+" ")
			end := header + strings.Index(f.SessionContext[header:], "\n") + 1
			g := f
			g.SessionContext = f.SessionContext[:end] + path + f.SessionContext[end:]
			if g.ValidateContext(l.cfg.Limits) == nil {
				t.Fatalf("run %s left out a handover path that fits", got[i].RunID)
			}
		}
	}
	// edge returns the least n below 1<<16 at which stop holds, which it must
	// not at 0, or 0 when there is none.
	edge := func(stop func(n int) bool) int {
		lo, hi := 0, 1<<16
		for lo < hi {
			if mid := (lo + hi) / 2; stop(mid) {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		if lo == 0 {
			t.Fatal("the edge is at 0")
		}
		return lo % (1 << 16)
	}
	answer := func(n int) func(*sessionFixture) {
		return func(j *sessionFixture) { j.states["r6"] = returned(strings.Repeat("Ω", n)) }
	}
	goal := func(n int) func(*sessionFixture) {
		return func(j *sessionFixture) { j.runs[1].Goal = strings.Repeat("Ψ", n) }
	}
	for _, size := range []func(int) func(*sessionFixture){answer, goal} {
		edges := []int{edge(func(n int) bool { return len(selection(sized(size(n), false)).sessionSources) < 6 })}
		for run := range paths {
			edges = append(edges, edge(func(n int) bool { return !pointed(selection(sized(size(n), true)), run) }))
		}
		for _, n := range edges {
			if n > 0 {
				check(size(n - 1))
				check(size(n))
			}
		}
		for n := 0; n < 1<<16; n += 4099 {
			check(size(n))
		}
	}
	if dropped == 0 {
		t.Fatal("no edge reached a selection that leaves a handover path out")
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
