package harness_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
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
		func(c *harness.LoopConfig) { c.History = "" },
		func(c *harness.LoopConfig) { c.Poll = -1 },
		func(c *harness.LoopConfig) { c.InspectRetries = -1 },
		func(c *harness.LoopConfig) { c.Limits = harness.PromptLimits{Total: 64 << 10} },
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
	if got := f.exec.log(); !slices.Equal(got, []string{"drive", "drive"}) {
		t.Fatalf("executor calls = %v", got)
	}
	if n, _ := f.model.calls(); n != 3 {
		t.Fatalf("prompts = %d, want 3", n)
	}
	wantContains(t, f.model.prompts[1].User(), "### Step 1", "ls -la /workspace", "ran: ls -la /workspace", "Execution: exited, exit code 0")
	wantContains(t, f.model.prompts[2].User(), "### Step 1", "### Step 2", "ran: find . -name '*.go' | wc -l")
	if strings.Contains(f.model.prompts[0].User(), "ls -la") {
		t.Errorf("first prompt shows a step that has not run:\n%s", f.model.prompts[0].User())
	}
	for i, p := range f.model.prompts[1:] {
		prev := f.model.prompts[i]
		if p.Stable != prev.Stable+1 || !slices.Equal(p.Blocks[:prev.Stable], prev.Blocks[:prev.Stable]) {
			t.Errorf("prompt %d does not extend the stable blocks of prompt %d by one step:\n%q\n%q", i+2, i+1, p.Blocks[:p.Stable], prev.Blocks[:prev.Stable])
		}
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
		Coverage:    harness.UsageCoverage{Attempts: 3, InputReports: 3, OutputReports: 3},
		OutputBytes: int64(len("ran: ls -la /workspace") + len("ran: find . -name '*.go' | wc -l")),
		Steps:       2,
	}
	if totals != want {
		t.Fatalf("totals = %+v, want %+v", totals, want)
	}
}

func TestLoopReadsTheCompleteActivePage(t *testing.T) {
	const n = 128
	var replies []reply
	for i := 1; i <= n; i++ {
		replies = append(replies, execReply(fmt.Sprintf("echo %d %s", i, strings.Repeat("x", 200))))
	}
	f := newFixture(t, core.Limits{MaxSteps: n + 1, MaxAttempts: 1}, harness.Budget{}, replies...)
	f.cfg.Limits = harness.DefaultPromptLimits()
	f.cfg.Limits.Total = 1 << 20
	f.steps(f.loop(), 4*n)
	ctx := context.Background()
	run, err := f.store.Run(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	all, err := f.store.Recent(ctx, runID, 0, n)
	if err != nil || len(all) != n {
		t.Fatalf("Recent() = %d records, %v; want %d", len(all), err, n)
	}
	// Preparation reads the previous step's completion, one record in one
	// page, and then the window in pages of historyPage records.
	for _, tc := range []struct{ total, pages int }{{1 << 20, 2}} {
		f.cfg.Limits = harness.DefaultPromptLimits()
		f.cfg.Limits.Total = tc.total
		p := &probe{Store: f.store}
		if snap, err := f.loopOn(p).Step(f.bounded(), runID); err != nil || p.frozen == nil {
			t.Fatalf("Total %d: Step() = %+v, %v; want a frozen request", tc.total, snap.State, err)
		}
		whole := harness.Frame{
			Goal: run.Goal, Constraints: run.Constraints, Environment: f.cfg.Environment,
			Step: n + 1, MaxSteps: n + 1, Attempt: 1, MaxAttempts: 1, Latest: &all[len(all)-1],
			RunID: runID, ContextState: &harness.ContextState{FirstStep: 1},
		}
		whole.Receipts, err = f.store.Receipts(ctx, runID, 0, n)
		if err != nil {
			t.Fatal(err)
		}
		want, err := whole.Prompt(f.cfg.Limits)
		got := f.model.prompts[len(f.model.prompts)-1]
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("Total %d: prompt differs from the one of the whole history (%v):\n%s", tc.total, err, got.User())
		}
		shown := got.Stable - 1
		earlier := fmt.Sprintf("\n%d earlier steps are not shown.\n", n-shown)
		if shown != n || strings.Contains(got.User(), earlier) != (shown < n) || !strings.Contains(got.User(), fmt.Sprintf("\n### Step %d (sequence ", n-shown+1)) {
			t.Fatalf("Total %d: %d steps shown, want more than 8 ending at step %d with the rest counted as earlier:\n%s", tc.total, shown, n, got.User())
		}
		if p.pages != tc.pages+1 || p.records != min(n, tc.pages*historyPage)+1 {
			t.Fatalf("Total %d: read %d records in %d pages, want %d pages", tc.total, p.records, p.pages, tc.pages)
		}
	}

	prompts, _ := f.model.calls()
	if _, err := f.loopOn(&failingReceiptPage{Store: f.store}).Step(f.bounded(), runID); !errors.Is(err, errRecent) {
		t.Fatalf("Step() with a failing second page = %v, want %v", err, errRecent)
	}
	if after, _ := f.model.calls(); after != prompts || len(f.transitions()) != 5*n {
		t.Fatalf("a failing page froze %d prompts and left %d transitions, want none and %d", after-prompts, len(f.transitions()), 5*n)
	}
}

var errRecent = errors.New("disk I/O error")

// failingRecent fails every Recent read of a journal after the first.
type failingRecent struct {
	*sqlite.Store
	reads int
}

func (j *failingRecent) Recent(ctx context.Context, id string, before int64, limit int) ([]harness.StepRecord, error) {
	if j.reads++; j.reads > 1 {
		return nil, errRecent
	}
	return j.Store.Recent(ctx, id, before, limit)
}

type failingReceiptPage struct {
	*sqlite.Store
	reads int
}

func (j *failingReceiptPage) Receipts(ctx context.Context, id string, before int64, limit int) ([]harness.StepReceipt, error) {
	if j.reads++; j.reads > 1 {
		return nil, errRecent
	}
	return j.Store.Receipts(ctx, id, before, limit)
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
		if r["run_id"] != runID || r["phase"] != string(core.PhaseRequestFrozen) || r["step"] != "1" {
			t.Errorf("retry log = %v, want the run, phase, and step", r)
		}
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

type failingJournal struct {
	*sqlite.Store
	kind string
}

func (j failingJournal) Commit(ctx context.Context, t harness.Transition) error {
	if t.Event.Kind() == j.kind {
		return errors.New("disk full")
	}
	return j.Store.Commit(ctx, t)
}

func TestLoopLogsTheFailureOfACommitTheStoreRefused(t *testing.T) {
	h := &logs{}
	f := newFixture(t, limits, harness.Budget{}, reply{err: &harness.StopError{Reason: core.StopUnrecoverable, Err: errors.New("bad key")}})
	f.cfg.Logger = slog.New(h)
	if _, err := f.loopOn(failingJournal{f.store, "stop_requested"}).Run(f.bounded(), runID); err == nil {
		t.Fatal("Run() succeeded although the store refused the stop")
	}
	failed := h.find("commit failed")
	if len(failed) != 1 || failed[0]["event"] != "stop_requested" || !strings.Contains(failed[0]["failure"], "bad key") || failed[0]["error"] != "disk full" {
		t.Fatalf("commit failure logs = %v, want the stop's failure text and the store error", failed)
	}
}

func TestLoopResendsFrozenBytesAfterTransportFailure(t *testing.T) {
	partial := harness.Usage{InputTokens: 40, OutputTokens: 2, InputReported: true, OutputReported: true}
	f := newFixture(t, limits, harness.Budget{}, reply{body: `{"kind":"ret`, usage: partial, err: errors.New("stream reset"), flip: true}, returnReply("done"))
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

func TestLoopCapsBackoff(t *testing.T) {
	h := &logs{}
	f := newFixture(t, limits, harness.Budget{}, reply{err: errors.New("reset")}, reply{err: errors.New("reset")})
	f.cfg.Logger, f.cfg.Backoff = slog.New(h), 20*time.Second
	cancel := func() {}
	l := f.loopOn(hookJournal{Store: f.store, after: func(t harness.Transition) {
		if t.Event.Kind() == "response_incomplete" {
			cancel()
		}
	}})
	f.steps(l, 1)
	for range 2 {
		ctx, stop := context.WithCancel(context.Background())
		cancel = stop
		_, err := l.Step(ctx, runID)
		stop()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Step() error = %v, want the backoff cancelled", err)
		}
	}
	var waits []string
	for _, r := range h.find("retrying an incomplete response") {
		waits = append(waits, r["backoff"])
	}
	if !slices.Equal(waits, []string{"20s", "30s"}) {
		t.Fatalf("backoff waits = %v, want 20s then the 30s cap", waits)
	}
}

func TestLoopRecordsAnUnconfirmedSuccessAsIncomplete(t *testing.T) {
	unconfirmed := returnReply("done")
	unconfirmed.flip = true
	for _, tt := range []struct {
		name  string
		reply reply
		want  *harness.Response
	}{
		{"incomplete", unconfirmed, &harness.Response{Body: []byte(unconfirmed.body), Model: "fake-1", Protocol: "fake/v1", Adapter: "fake", Usage: usage}},
		{"empty", reply{usage: usage}, &harness.Response{Body: []byte{}, Model: "fake-1", Protocol: "fake/v1", Adapter: "fake", Usage: usage}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{}, tt.reply, returnReply("done"))
			wantState(t, f.run(f.loop()), core.PhaseReturned, "")
			f.wantKinds([]string{"request_frozen", "attempt_started", "response_incomplete"}, closing[1:])
			got := f.transitions()[2].Facts
			wantContains(t, got.Failure, "without a complete response")
			if !reflect.DeepEqual(got.Response, tt.want) {
				t.Fatalf("incomplete response = %+v, want %+v", got.Response, tt.want)
			}
		})
	}
}

func TestLoopRejectsAnEmptyDecision(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, reply{body: "<html>busy</html>", usage: usage}, returnReply("done"))
	f.model.empty = true
	wantState(t, f.run(f.loop()), core.PhaseReturned, "")
	f.wantKinds(closing, closing)
	wantContains(t, f.model.prompts[1].User(), "## Protocol repair", "<html>busy</html>")
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
	if got := f.last().Facts; got.Failure != "connection refused" || got.Response == nil || got.Response.Complete || got.Response.Usage != (harness.Usage{}) || snap.State.Attempt != 2 {
		t.Fatalf("facts = %+v at attempt %d; want failure and received response before attempt 2", got, snap.State.Attempt)
	}
}

func TestLoopStopsOnModelStopError(t *testing.T) {
	for _, reason := range []core.StopReason{core.StopUnrecoverable, core.StopBudgetExhausted} {
		t.Run(string(reason), func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{}, reply{err: &harness.StopError{Reason: reason, Err: errors.New("provider refused")}})
			wantState(t, f.run(f.loop()), core.PhaseStopped, reason)
			f.wantKinds([]string{"request_frozen", "attempt_started", "stop_requested"})
			if got := f.last().Facts; got.Failure != string(reason)+": provider refused" || got.Response == nil || got.Response.Usage != (harness.Usage{}) {
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
		{"prompt", func(f *fixture) { f.cfg.Limits = harness.DefaultPromptLimits(); f.cfg.Limits.Total = 64 }, "request exceeds 64-byte resource cap"},
		{"goal", func(f *fixture) { f.cfg.Limits = harness.DefaultPromptLimits(); f.cfg.Limits.Goal = 8 }, "goal exceeds its 8-byte limit"},
		{"constraints", func(f *fixture) { f.cfg.Limits = harness.DefaultPromptLimits(); f.cfg.Limits.Constraints = 8 }, "constraints exceeds its 8-byte limit"},
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
	wantContains(t, f.model.prompts[1].User(), "## Protocol repair", "Sure! Here is the plan.", "invalid decision", "attempt 2 of 3")
	if strings.Contains(f.model.prompts[0].User(), "Rejected") {
		t.Errorf("first prompt carries a rejection:\n%s", f.model.prompts[0].User())
	}
	if first, repair := f.model.prompts[0], f.model.prompts[1]; !slices.Equal(repair.Blocks[:repair.Stable], first.Blocks[:first.Stable]) {
		t.Errorf("the repair changed the stable blocks:\n%q\n%q", repair.Blocks[:repair.Stable], first.Blocks[:first.Stable])
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
		{"tokens", harness.Budget{Tokens: 110}, []string{"request_frozen", "attempt_started", "response_saved", "response_loaded", "stop_requested", "observed"}, "110 tokens"},
		{"output bytes", harness.Budget{OutputBytes: 10}, append(slices.Clone(cycle), "stop_requested"), "15 output bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, limits, tt.budget, execReply("echo hello"), returnReply("done"))
			wantState(t, f.run(f.loop()), core.PhaseStopped, core.StopBudgetExhausted)
			f.wantKinds(tt.kinds)
			stops := slices.DeleteFunc(f.transitions(), func(tr harness.Transition) bool { return tr.Event.Kind() != "stop_requested" })
			wantContains(t, stops[0].Facts.Failure, tt.failure)
		})
	}
}

func TestLoopDecodesAResponseSavedWithinBudget(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{Tokens: 110}, returnReply("done"))
	wantState(t, f.run(f.loop()), core.PhaseReturned, "")
	f.wantKinds([]string{"request_frozen", "attempt_started", "response_saved", "response_loaded"})
}

func TestLoopDoesNotAutomaticallyReadWorkspaceNotes(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, returnReply("done"))
	if err := os.MkdirAll(filepath.Join(f.ws, ".canti"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.ws, ".canti", "notes.md"), []byte("plan: count the files first\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f.run(f.loop())
	if p := f.model.prompts[0].User(); strings.Contains(p, "plan: count the files first") || strings.Contains(p, "## Notes") {
		t.Fatalf("notes were automatically loaded: %s", p)
	}
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
	if p := f.model.prompts[0].User(); strings.Contains(p, "notes unreadable") || strings.Contains(p, "## Notes") {
		t.Fatalf("notes were read: %s", p)
	}
	if w := h.find("notes unreadable"); len(w) != 0 {
		t.Fatalf("notes warnings = %v, want none", w)
	}
}

func TestLoopSettlesReportedUsageWithTerminalProviderFailure(t *testing.T) {
	report := harness.Usage{InputTokens: 3, OutputTokens: 7, InputReported: true, OutputReported: true}
	f := newFixture(t, limits, harness.Budget{}, reply{body: `{"error":"provider failure","usage":{"input_tokens":3,"output_tokens":7}}`, usage: report, err: &harness.StopError{Reason: core.StopUnrecoverable, Err: errors.New("provider failure")}})
	wantState(t, f.run(f.loop()), core.PhaseStopped, core.StopUnrecoverable)
	last := f.last()
	if last.Event.Kind() != "stop_requested" || last.Facts.Response == nil || last.Facts.Response.Usage != report || last.Facts.Failure == "" {
		t.Fatalf("terminal usage not attached atomically: %+v", last)
	}
	got, err := f.store.Totals(context.Background(), runID)
	if err != nil || got.Usage.InputTokens != 3 || got.Usage.OutputTokens != 7 || got.Coverage != (harness.UsageCoverage{Attempts: 1, InputReports: 1, OutputReports: 1}) {
		t.Fatalf("terminal totals: %+v %v", got, err)
	}
	if err := f.store.Commit(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	again, err := f.store.Totals(context.Background(), runID)
	if err != nil || again != got {
		t.Fatalf("terminal replay double counted: %+v %v", again, err)
	}
}

func TestLoopCancellationKeepsLostAttemptDistinctFromReportedZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := returnReply("interrupted")
	first.hook = cancel
	zero := returnReply("done")
	zero.usage = harness.Usage{InputReported: true, OutputReported: true}
	f := newFixture(t, limits, harness.Budget{}, first, zero)
	loop := f.loop()
	if _, err := loop.Step(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := loop.Step(ctx, runID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled send: %v", err)
	}
	before, err := f.store.Latest(context.Background(), runID)
	if err != nil || before.State.Phase != core.PhaseRequestSent {
		t.Fatalf("cancelled send settled unexpectedly: %+v %v", before, err)
	}
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
	totals, err := f.store.Totals(context.Background(), runID)
	if err != nil || totals.Usage.InputTokens != 0 || totals.Usage.OutputTokens != 0 || totals.Coverage != (harness.UsageCoverage{Attempts: 2, InputReports: 1, OutputReports: 1, Lost: 1}) {
		t.Fatalf("lost/zero conflated: %+v %v", totals, err)
	}
}

func TestLoopReportedBudgetStopsAfterPartialUsageOvershoot(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{Tokens: 3}, reply{body: `{"usage":{"output_tokens":4}}`, usage: harness.Usage{OutputTokens: 4, OutputReported: true}, err: errors.New("incomplete compatible-provider response")}, returnReply("must not be sent"))
	wantState(t, f.run(f.loop()), core.PhaseStopped, core.StopBudgetExhausted)
	_, sent := f.model.calls()
	totals, err := f.store.Totals(context.Background(), runID)
	if sent != 1 || err != nil || totals.Usage.OutputTokens != 4 || totals.Coverage != (harness.UsageCoverage{Attempts: 1, OutputReports: 1}) {
		t.Fatalf("reported overshoot/coverage: calls=%d totals=%+v %v", sent, totals, err)
	}
}
