package harness_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestNewLoopRequiresDependencies(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	tests := []struct {
		name  string
		build func() (*harness.Loop, error)
	}{
		{"journal", func() (*harness.Loop, error) { return harness.NewLoop(nil, f.exec, f.model, f.cfg) }},
		{"executor", func() (*harness.Loop, error) { return harness.NewLoop(f.store, nil, f.model, f.cfg) }},
		{"model", func() (*harness.Loop, error) { return harness.NewLoop(f.store, f.exec, nil, f.cfg) }},
	}
	for _, edit := range []func(*harness.LoopConfig){
		func(c *harness.LoopConfig) { c.Environment = " " },
		func(c *harness.LoopConfig) { c.ArtifactDir = "" },
		func(c *harness.LoopConfig) { c.Workspace = "" },
		func(c *harness.LoopConfig) { c.NotesFile = "" },
		func(c *harness.LoopConfig) { c.NotesPath = "" },
		func(c *harness.LoopConfig) { c.History = -1 },
		func(c *harness.LoopConfig) { c.Poll = -1 },
		func(c *harness.LoopConfig) { c.InspectRetries = -1 },
	} {
		cfg := f.cfg
		edit(&cfg)
		tests = append(tests, struct {
			name  string
			build func() (*harness.Loop, error)
		}{"config", func() (*harness.Loop, error) { return harness.NewLoop(f.store, f.exec, f.model, cfg) }})
	}
	for _, tt := range tests {
		if l, err := tt.build(); err == nil || l != nil {
			t.Errorf("NewLoop without a valid %s = %v, %v; want an error", tt.name, l, err)
		}
	}
}

func TestLoopRunsExecObserveExecObserveReturn(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, twoSteps()...)
	snap := f.run(f.loop())
	wantState(t, snap, core.PhaseReturned, "")
	f.wantKinds(cycle, cycle, closing)
	if snap.State.Step != 3 || snap.State.Decision.Payload != "There are 3 Go files." {
		t.Fatalf("final state = %+v, want the return decision at step 3", snap.State)
	}
	if got := f.exec.log(); !slices.Equal(got, []string{"bind", "start", "inspect", "bind", "start", "inspect"}) {
		t.Fatalf("executor calls = %v", got)
	}
	if n, _ := f.model.calls(); n != 3 {
		t.Fatalf("prompts = %d, want 3", n)
	}
	wantContains(t, f.model.prompts[1].User, "### Step 1", "ls -la /workspace", "ran: ls -la /workspace", "Execution: exited, exit code 0")
	wantContains(t, f.model.prompts[2].User, "### Step 1", "### Step 2", "ran: find . -name '*.go' | wc -l")
	if strings.Contains(f.model.prompts[0].User, "ls -la") {
		t.Errorf("first prompt shows a step that has not run:\n%s", f.model.prompts[0].User)
	}
	for i, sent := range f.model.sent {
		if want := f.frozen(i+1, 1); !bytes.Equal(sent, want) {
			t.Errorf("request %d sent %q, want the frozen bytes %q", i+1, sent, want)
		}
	}
	totals, err := f.store.Totals(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	want := harness.Totals{
		Usage:       harness.Usage{InputTokens: 300, OutputTokens: 30},
		OutputBytes: int64(len("ran: ls -la /workspace") + len("ran: find . -name '*.go' | wc -l")),
		Steps:       2,
	}
	if totals != want {
		t.Fatalf("totals = %+v, want %+v", totals, want)
	}
}

func TestLoopLogsEveryCommitWithoutBodies(t *testing.T) {
	h := &logs{}
	f := newFixture(t, limits, harness.Budget{}, reply{err: errors.New("stream reset")}, reply{err: errors.New("stream reset")}, returnReply("done"))
	f.cfg.Logger, f.cfg.Backoff = slog.New(h), 2*time.Millisecond
	f.run(f.loop())
	commits, ts := h.find("committed"), f.transitions()
	if len(commits) != len(ts) {
		t.Fatalf("logged %d commits, want %d", len(commits), len(ts))
	}
	for i, c := range commits {
		want := map[string]string{"run_id": runID, "seq": strconv.FormatInt(ts[i].Seq, 10), "phase": string(ts[i].To.Phase), "event": ts[i].Event.Kind()}
		for k, v := range want {
			if c[k] != v {
				t.Errorf("commit log %d has %s = %q, want %q", i, k, c[k], v)
			}
		}
		if (c["level"] == "WARN") != (ts[i].Facts.Failure != "") {
			t.Errorf("commit log %d at level %s with failure %q", i, c["level"], ts[i].Facts.Failure)
		}
	}
	var waits []string
	for _, r := range h.find("retrying an incomplete response") {
		waits = append(waits, r["backoff"])
	}
	if !slices.Equal(waits, []string{"2ms", "4ms"}) {
		t.Fatalf("backoff waits = %v, want doubling from 2ms", waits)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		r.Attrs(func(a slog.Attr) bool {
			if v := a.Value.String(); strings.Contains(v, "Count the Go files") || strings.Contains(v, `"kind"`) {
				t.Errorf("log %q attribute %s carries a body: %q", r.Message, a.Key, v)
			}
			return true
		})
	}
}

func TestLoopResendsFrozenBytesAfterTransportFailure(t *testing.T) {
	partial := harness.Usage{InputTokens: 40, OutputTokens: 2}
	f := newFixture(t, limits, harness.Budget{}, reply{body: `{"kind":"ret`, usage: partial, err: errors.New("stream reset"), complete: true}, returnReply("done"))
	f.cfg.Backoff = 5 * time.Millisecond
	begin := time.Now()
	snap := f.run(f.loop())
	elapsed := time.Since(begin)
	wantState(t, snap, core.PhaseReturned, "")
	f.wantKinds([]string{"request_frozen", "attempt_started", "response_incomplete"}, closing[1:])
	ts := f.transitions()
	if ts[1].Event != (core.AttemptStarted{Step: 1, Attempt: 1}) || ts[3].Event != (core.AttemptStarted{Step: 1, Attempt: 2}) {
		t.Fatalf("attempts = %#v, %#v; want 1 then 2", ts[1].Event, ts[3].Event)
	}
	frozen := f.frozen(1, 1)
	if len(f.model.sent) != 2 || !bytes.Equal(f.model.sent[0], frozen) || !bytes.Equal(f.model.sent[1], frozen) {
		t.Fatalf("sent %q, want the frozen bytes twice", f.model.sent)
	}
	got := ts[2].Facts
	if got.Failure != "stream reset" || got.Response == nil || got.Response.Complete ||
		string(got.Response.Body) != `{"kind":"ret` || got.Response.Usage != partial {
		t.Fatalf("incomplete response facts = %+v, response %+v", got, got.Response)
	}
	totals, err := f.store.Totals(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if want := (harness.Usage{InputTokens: 140, OutputTokens: 12}); totals.Usage != want {
		t.Fatalf("usage = %+v, want %+v", totals.Usage, want)
	}
	if elapsed < 5*time.Millisecond {
		t.Fatalf("run took %v, want at least the 5ms backoff", elapsed)
	}
}

func TestLoopBackoffIsCancellable(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, reply{err: errors.New("connection refused")})
	f.cfg.Backoff = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := f.loopOn(hookJournal{Store: f.store, after: func(t harness.Transition) {
		if t.Event.Kind() == "response_incomplete" {
			cancel()
		}
	}})
	snap, err := l.Run(ctx, runID)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want cancellation", err)
	}
	wantState(t, snap, core.PhaseRequestFrozen, "")
	f.wantKinds([]string{"request_frozen", "attempt_started", "response_incomplete"})
	if got := f.last().Facts; got != (harness.Facts{Failure: "connection refused"}) || snap.State.Attempt != 2 {
		t.Fatalf("facts = %+v at attempt %d; want the failure alone before attempt 2", got, snap.State.Attempt)
	}
}

func TestLoopStopsOnModelStopError(t *testing.T) {
	for _, reason := range []core.StopReason{core.StopUnrecoverable, core.StopBudgetExhausted} {
		t.Run(string(reason), func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{}, reply{err: &harness.StopError{Reason: reason, Err: errors.New("provider refused")}})
			wantState(t, f.run(f.loop()), core.PhaseStopped, reason)
			f.wantKinds([]string{"request_frozen", "attempt_started", "stop_requested"})
			if got := f.last().Facts; got != (harness.Facts{Failure: string(reason) + ": provider refused"}) {
				t.Fatalf("stop facts = %+v", got)
			}
		})
	}
}

func TestLoopStopsWhenRequestCannotBeBuilt(t *testing.T) {
	tests := []struct {
		name    string
		edit    func(*fixture)
		failure string
	}{
		{"freeze", func(f *fixture) { f.model.freezeErr = errors.New("request too large") }, "request too large"},
		{"prompt", func(f *fixture) { f.cfg.Limits = harness.DefaultPromptLimits(); f.cfg.Limits.Total = 64 }, "exceeds"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{})
			tt.edit(f)
			wantState(t, f.run(f.loop()), core.PhaseStopped, core.StopUnrecoverable)
			f.wantKinds([]string{"stop_requested"})
			wantContains(t, f.last().Facts.Failure, tt.failure)
			if _, sent := f.model.calls(); sent != 0 {
				t.Fatalf("sent %d requests, want none", sent)
			}
		})
	}
}

func TestLoopRepairsAnUndecodableResponse(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, reply{body: "Sure! Here is the plan.", usage: usage}, returnReply("done"))
	snap := f.run(f.loop())
	wantState(t, snap, core.PhaseReturned, "")
	f.wantKinds(closing, closing)
	if repair := f.transitions()[4]; repair.To.RequestRev != 2 || repair.To.Attempt != 2 {
		t.Fatalf("repair froze %+v, want revision 2 at attempt 2", repair.To)
	}
	wantContains(t, f.model.prompts[1].User, "## Rejected response", "Sure! Here is the plan.", "invalid decision", "attempt 2 of 3")
	if strings.Contains(f.model.prompts[0].User, "Rejected") {
		t.Errorf("first prompt carries a rejection:\n%s", f.model.prompts[0].User)
	}
	if !bytes.Equal(f.model.sent[1], f.frozen(1, 2)) || bytes.Equal(f.model.sent[0], f.model.sent[1]) {
		t.Fatal("the repair did not send the newly frozen revision")
	}
}

func TestLoopStopsWhenCoreBudgetsRunOut(t *testing.T) {
	lost := reply{err: errors.New("timeout")}
	tests := []struct {
		name    string
		limits  core.Limits
		replies []reply
		reason  core.StopReason
		kinds   []string
	}{
		{"protocol failure", core.Limits{MaxSteps: 5, MaxAttempts: 2}, []reply{{body: "no"}, {body: "still no"}}, core.StopProtocolFailure, slices.Concat(closing, closing)},
		{"attempts exhausted", core.Limits{MaxSteps: 5, MaxAttempts: 2}, []reply{lost, lost}, core.StopAttemptsExhausted,
			[]string{"request_frozen", "attempt_started", "response_incomplete", "attempt_started", "response_incomplete"}},
		{"steps exhausted", core.Limits{MaxSteps: 1, MaxAttempts: 3}, []reply{execReply("true")}, core.StopStepsExhausted, cycle},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, tt.limits, harness.Budget{}, tt.replies...)
			wantState(t, f.run(f.loop()), core.PhaseStopped, tt.reason)
			f.wantKinds(tt.kinds)
			if _, sent := f.model.calls(); sent != len(tt.replies) {
				t.Fatalf("sent %d requests, want %d", sent, len(tt.replies))
			}
		})
	}
}

func TestLoopStopsWhenMeasuredBudgetsRunOut(t *testing.T) {
	tests := []struct {
		name    string
		budget  harness.Budget
		kinds   []string
		failure string
	}{
		{"tokens", harness.Budget{Tokens: 110}, []string{"request_frozen", "attempt_started", "response_saved"}, "110 tokens"},
		{"output bytes", harness.Budget{OutputBytes: 10}, cycle, "15 output bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, limits, tt.budget, execReply("echo hello"), returnReply("done"))
			wantState(t, f.run(f.loop()), core.PhaseStopped, core.StopBudgetExhausted)
			f.wantKinds(tt.kinds, []string{"stop_requested"})
			wantContains(t, f.last().Facts.Failure, tt.failure)
		})
	}
}

func TestLoopShowsNotes(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, returnReply("done"))
	if err := os.MkdirAll(filepath.Join(f.ws, ".canti"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.ws, ".canti", "notes.md"), []byte("plan: count the files first\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f.run(f.loop())
	wantContains(t, f.model.prompts[0].User, "/workspace/.canti/notes.md", "plan: count the files first")
}

func TestLoopToleratesUnreadableNotes(t *testing.T) {
	h := &logs{}
	f := newFixture(t, limits, harness.Budget{}, returnReply("done"))
	f.cfg.Logger = slog.New(h)
	if err := os.MkdirAll(filepath.Join(f.ws, ".canti", "notes.md"), 0o755); err != nil {
		t.Fatal(err)
	}
	snap, err := f.loop().Step(context.Background(), runID)
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	wantState(t, snap, core.PhaseRequestFrozen, "")
	wantContains(t, f.model.prompts[0].User, "notes unreadable: ", "not a regular file")
	if w := h.find("notes unreadable"); len(w) != 1 || w[0]["level"] != "WARN" {
		t.Fatalf("notes warnings = %v, want one", w)
	}
}
