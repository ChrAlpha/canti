package harness_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/executortest"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

const runID = "run-1"

const historyPage = 64 // records per Recent read of the loop

var (
	created = time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	limits  = core.Limits{MaxSteps: 5, MaxAttempts: 3}
	usage   = harness.Usage{InputTokens: 100, OutputTokens: 10}
	cycle   = []string{"request_frozen", "attempt_started", "response_saved", "response_loaded", "job_bound", "job_started", "observed"}
	closing = []string{"request_frozen", "attempt_started", "response_saved", "response_loaded"}
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type reply struct {
	body  string
	usage harness.Usage
	err   error
	flip  bool // reports the opposite completeness, as a faulty adapter might
	late  bool // completes even though ctx is done
	block bool // answers only once ctx is done
	hook  func()
}

func decision(kind core.Kind, payload string) reply {
	b, _ := json.Marshal(core.Decision{Kind: kind, Payload: payload})
	return reply{body: string(b), usage: usage}
}

func execReply(command string) reply { return decision(core.Exec, command) }

func returnReply(text string) reply { return decision(core.Return, text) }

type fakeModel struct {
	mu        sync.Mutex
	replies   []reply
	prompts   []harness.Prompt
	sent      [][]byte
	freezeErr error
	empty     bool // Decision extracts nothing
}

func (m *fakeModel) Freeze(p harness.Prompt) (harness.Request, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prompts = append(m.prompts, p)
	return harness.Request{Body: []byte(p.System + "\n\n" + p.User()), Model: "fake-1", Protocol: "fake/v1", Adapter: "fake"}, m.freezeErr
}

func (m *fakeModel) Complete(ctx context.Context, req harness.Request) (harness.Response, error) {
	m.mu.Lock()
	m.sent = append(m.sent, bytes.Clone(req.Body))
	r := reply{err: &harness.StopError{Reason: core.StopUnrecoverable, Err: errors.New("unscripted request")}}
	if len(m.replies) > 0 {
		r, m.replies = m.replies[0], m.replies[1:]
	}
	m.mu.Unlock()
	if r.hook != nil {
		r.hook()
	}
	if r.block {
		<-ctx.Done()
	}
	if err := ctx.Err(); err != nil && !r.late {
		return harness.Response{}, err
	}
	return harness.Response{Body: []byte(r.body), Complete: (r.err == nil) != r.flip, Model: "fake-1", Protocol: "fake/v1", Adapter: "fake", Usage: r.usage}, r.err
}

func (m *fakeModel) Decision(resp harness.Response) []byte {
	if m.empty {
		return nil
	}
	return resp.Body
}

func (m *fakeModel) calls() (prompts, sent int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.prompts), len(m.sent)
}

type fakeJob struct {
	harness.Job
	command string
	runs    int
}

// fakeExec keeps its jobs across loops, as a backend outlives controllers.
// A started job finishes at its first inspection unless hold is set, in
// which case it runs until cancelled and tick runs at every inspection.
type fakeExec struct {
	mu      sync.Mutex
	clock   *clock
	jobs    map[string]*fakeJob
	fenced  map[string]bool
	calls   []string
	hold    bool
	tick    func()
	inspect func(effectID string) (harness.Job, bool, error)
}

func jobID(effectID string) string { return "job-" + strings.TrimPrefix(effectID, "effect_")[:12] }

func (x *fakeExec) Bind(_ context.Context, e harness.Effect) (harness.Job, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.calls = append(x.calls, "bind")
	j := x.jobs[e.ID]
	if j == nil {
		j = &fakeJob{Job: harness.Job{ID: jobID(e.ID), Status: harness.JobPending}, command: e.Command}
		if x.fenced[e.ID] {
			j.Status = harness.JobCancelled
		}
		x.jobs[e.ID] = j
	}
	if j.command != e.Command {
		return harness.Job{}, errors.New("effect is bound to a different command")
	}
	return j.Job, nil
}

func (x *fakeExec) Start(_ context.Context, effectID string) (harness.Job, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.calls = append(x.calls, "start")
	j := x.jobs[effectID]
	if j == nil {
		return harness.Job{}, errors.New("unbound effect")
	}
	if j.Status == harness.JobPending {
		j.Status, j.StartedAt = harness.JobRunning, x.clock.Now()
		j.runs++
	}
	return j.Job, nil
}

func (x *fakeExec) Inspect(_ context.Context, effectID string) (harness.Job, bool, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.calls = append(x.calls, "inspect")
	if x.inspect != nil {
		return x.inspect(effectID)
	}
	j := x.jobs[effectID]
	if j == nil {
		return harness.Job{}, false, nil
	}
	if j.Status == harness.JobRunning {
		if !x.hold {
			x.finish(j, harness.JobSucceeded)
		} else if x.tick != nil {
			x.tick()
		}
	}
	return j.Job, true, nil
}

func (x *fakeExec) Cancel(_ context.Context, effectID string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.calls = append(x.calls, "cancel")
	switch j := x.jobs[effectID]; {
	case j == nil:
		x.fenced[effectID] = true
	case j.Status == harness.JobPending || j.Status == harness.JobRunning:
		x.finish(j, harness.JobCancelled)
	}
	return nil
}

func (x *fakeExec) finish(j *fakeJob, status harness.JobStatus) {
	j.Status, j.FinishedAt, j.Preview = status, x.clock.Now(), "ran: "+j.command
	if status == harness.JobSucceeded {
		code := 0
		j.ExitCode = &code
	}
	j.Output = harness.ArtifactRef{Path: j.ID + ".log", Bytes: int64(len(j.Preview)), Complete: true, Available: true}
}

func TestFakeExecutorPassesStartLatchCheck(t *testing.T) {
	x := &fakeExec{clock: &clock{now: created}, jobs: map[string]*fakeJob{}, fenced: map[string]bool{}}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err := executortest.Check(ctx, executortest.Fixture{
		Open: func(context.Context) (harness.Executor, error) { return x, nil },
		Effect: func(name string) harness.Effect {
			return harness.Effect{ID: fmt.Sprintf("effect_%x", sha256.Sum256([]byte(name))), Command: "run " + name}
		},
		Runs: func(_ context.Context, effectID string) (int, error) {
			x.mu.Lock()
			defer x.mu.Unlock()
			if j := x.jobs[effectID]; j != nil {
				return j.runs, nil
			}
			return 0, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (x *fakeExec) log() []string {
	x.mu.Lock()
	defer x.mu.Unlock()
	return slices.Clone(x.calls)
}

func (x *fakeExec) set(effectID string, update func(*harness.Job)) {
	x.mu.Lock()
	defer x.mu.Unlock()
	update(&x.jobs[effectID].Job)
}

// hookJournal calls after once a transition is committed.
type hookJournal struct {
	*sqlite.Store
	after func(harness.Transition)
}

func (j hookJournal) Commit(ctx context.Context, t harness.Transition) error {
	err := j.Store.Commit(ctx, t)
	if err == nil {
		j.after(t)
	}
	return err
}

// probe counts the journal reads of a loop and captures, without recording,
// the request it would commit.
type probe struct {
	*sqlite.Store
	reads, pages, records, recordBytes int
	frozen                             *harness.Request
}

func (p *probe) Run(ctx context.Context, runID string) (harness.Run, error) {
	p.reads++
	return p.Store.Run(ctx, runID)
}

func (p *probe) Latest(ctx context.Context, runID string) (harness.Snapshot, error) {
	p.reads++
	return p.Store.Latest(ctx, runID)
}

func (p *probe) Request(ctx context.Context, runID string, step, rev int) (harness.Request, error) {
	p.reads++
	return p.Store.Request(ctx, runID, step, rev)
}

func (p *probe) Response(ctx context.Context, runID string, step, attempt int) (harness.Response, error) {
	p.reads++
	return p.Store.Response(ctx, runID, step, attempt)
}

func (p *probe) Totals(ctx context.Context, runID string) (harness.Totals, error) {
	p.reads++
	return p.Store.Totals(ctx, runID)
}

func (p *probe) Recent(ctx context.Context, runID string, before int64, limit int) ([]harness.StepRecord, error) {
	p.reads++
	p.pages++
	rs, err := p.Store.Recent(ctx, runID, before, limit)
	for _, r := range rs {
		p.records++
		p.recordBytes += len(r.Command) + len(r.Observation.Preview)
	}
	return rs, err
}

func (p *probe) Commit(_ context.Context, t harness.Transition) error {
	p.frozen = t.Facts.Request
	return nil
}

type logs struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *logs) Enabled(context.Context, slog.Level) bool { return true }

func (h *logs) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *logs) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *logs) WithGroup(string) slog.Handler { return h }

func (h *logs) find(message string) []map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []map[string]string
	for _, r := range h.records {
		if r.Message == message {
			m := map[string]string{"level": r.Level.String()}
			r.Attrs(func(a slog.Attr) bool {
				m[a.Key] = a.Value.String()
				return true
			})
			out = append(out, m)
		}
	}
	return out
}

type fixture struct {
	t     *testing.T
	path  string
	ws    string
	store *sqlite.Store
	clock *clock
	model *fakeModel
	exec  *fakeExec
	cfg   harness.LoopConfig
}

func newFixture(t *testing.T, limits core.Limits, budget harness.Budget, replies ...reply) *fixture {
	t.Helper()
	dir := t.TempDir()
	c := &clock{now: created}
	f := &fixture{
		t: t, path: filepath.Join(dir, "canti.db"), ws: filepath.Join(dir, "workspace"), clock: c,
		model: &fakeModel{replies: replies},
		exec:  &fakeExec{clock: c, jobs: map[string]*fakeJob{}, fenced: map[string]bool{}},
	}
	if err := os.Mkdir(f.ws, 0o755); err != nil {
		t.Fatal(err)
	}
	f.store = f.open()
	state, err := core.NewState(runID, limits)
	if err != nil {
		t.Fatal(err)
	}
	run := harness.Run{ID: runID, Goal: "Count the Go files.", Constraints: "Do not modify files.", Budget: budget, Created: created}
	if err := f.store.CreateRun(context.Background(), run, state); err != nil {
		t.Fatal(err)
	}
	f.cfg = harness.LoopConfig{
		Environment: "Alpine Linux container",
		History:     filepath.Join(dir, "history"),
		Poll:        time.Microsecond, Backoff: -1, Now: c.Now,
	}
	return f
}

func (f *fixture) open() *sqlite.Store {
	f.t.Helper()
	s, err := sqlite.Open(context.Background(), f.path)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { s.Close() })
	return s
}

func (f *fixture) loopOn(j harness.Journal) *harness.Loop {
	f.t.Helper()
	l, err := harness.NewLoop(j, f.exec, f.model, f.cfg)
	if err != nil {
		f.t.Fatal(err)
	}
	return l
}

func (f *fixture) loop() *harness.Loop { return f.loopOn(f.store) }

// fresh returns a new loop on a newly opened store, as a restarted
// controller would build it.
func (f *fixture) fresh() *harness.Loop { return f.loopOn(f.open()) }

// bounded guards a test against a regression that polls forever.
func (f *fixture) bounded() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	f.t.Cleanup(cancel)
	return ctx
}

func (f *fixture) run(l *harness.Loop) harness.Snapshot {
	f.t.Helper()
	snap, err := l.Run(f.bounded(), runID)
	if err != nil {
		f.t.Fatalf("Run() error = %v at %+v", err, snap.State)
	}
	return snap
}

func (f *fixture) steps(l *harness.Loop, n int) harness.Snapshot {
	f.t.Helper()
	var snap harness.Snapshot
	for range n {
		var err error
		if snap, err = l.Step(f.bounded(), runID); err != nil {
			f.t.Fatalf("Step() error = %v at %+v", err, snap.State)
		}
	}
	return snap
}

func (f *fixture) transitions() []harness.Transition {
	f.t.Helper()
	ts, err := f.store.Transitions(context.Background(), runID, 0, 1000)
	if err != nil {
		f.t.Fatal(err)
	}
	return ts
}

func (f *fixture) kinds() []string {
	var kinds []string
	for _, t := range f.transitions() {
		kinds = append(kinds, t.Event.Kind())
	}
	return kinds
}

func (f *fixture) last() harness.Transition {
	ts := f.transitions()
	if len(ts) == 0 {
		f.t.Fatal("no transitions committed")
	}
	return ts[len(ts)-1]
}

func (f *fixture) wantKinds(want ...[]string) {
	f.t.Helper()
	if got, want := f.kinds(), slices.Concat(want...); !slices.Equal(got, want) {
		f.t.Fatalf("committed kinds = %v, want %v", got, want)
	}
}

func (f *fixture) frozen(step, rev int) []byte {
	f.t.Helper()
	req, err := f.store.Request(context.Background(), runID, step, rev)
	if err != nil {
		f.t.Fatal(err)
	}
	return req.Body
}

func wantState(t *testing.T, snap harness.Snapshot, phase core.Phase, reason core.StopReason) {
	t.Helper()
	if snap.State.Phase != phase || snap.State.Reason != reason {
		t.Fatalf("state = %+v, want phase %q with reason %q", snap.State, phase, reason)
	}
}

func wantContains(t *testing.T, text string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if !strings.Contains(text, p) {
			t.Errorf("text does not contain %q:\n%s", p, text)
		}
	}
}

func twoSteps() []reply {
	return []reply{execReply("ls -la /workspace"), execReply("find . -name '*.go' | wc -l"), returnReply("There are 3 Go files.")}
}
