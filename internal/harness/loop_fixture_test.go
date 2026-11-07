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
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
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
	cycle   = []string{"request_frozen", "attempt_started", "response_saved", "response_loaded", "observed"}
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

func (x *fakeExec) Specification(command string) string {
	return harness.ConfigDigest([]byte("fixture:" + command))
}

func (x *fakeExec) Drive(ctx context.Context, e harness.Effect) (harness.Job, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.calls = append(x.calls, "drive")
	if err := ctx.Err(); err != nil {
		return harness.Job{}, err
	}
	if e.Spec != x.Specification(e.Command) {
		return harness.Job{}, errors.New("changed specification")
	}
	j := x.jobs[e.ID]
	if j == nil {
		if x.fenced[e.ID] {
			return harness.Job{EffectID: e.ID, Spec: e.Spec, Status: harness.JobCancelled, Fenced: true}, nil
		}
		j = &fakeJob{Job: harness.Job{ID: jobID(e.ID), EffectID: e.ID, Spec: e.Spec, Status: harness.JobRunning, StartedAt: x.clock.Now()}, command: e.Command, runs: 1}
		x.jobs[e.ID] = j
	}
	if j.command != e.Command {
		return harness.Job{}, errors.New("changed command")
	}
	return x.sample(e)
}

func (x *fakeExec) sample(e harness.Effect) (harness.Job, error) {
	if x.inspect != nil {
		j, found, err := x.inspect(e.ID)
		if err != nil {
			return j, err
		}
		if !found {
			status := harness.JobAbsent
			if x.fenced[e.ID] {
				status = harness.JobCancelled
			}
			return harness.Job{EffectID: e.ID, Spec: e.Spec, Status: status, Fenced: x.fenced[e.ID]}, nil
		}
		if j.EffectID == "" {
			j.EffectID = e.ID
		}
		if j.Spec == "" {
			j.Spec = e.Spec
		}
		if x.fenced[e.ID] {
			j.Fenced = true
		}
		if j.Status == harness.JobRunning && j.StartedAt.IsZero() {
			j.StartedAt = created
		}
		if j.Status == harness.JobSucceeded || j.Status == harness.JobFailed {
			if j.StartedAt.IsZero() {
				j.StartedAt = created
			}
			if j.FinishedAt.IsZero() {
				j.FinishedAt = x.clock.Now()
			}
		}
		if j.Status == harness.JobCancelled && !j.StartedAt.IsZero() {
			if j.ExitCode == nil {
				code := 143
				j.ExitCode = &code
			}
			if j.FinishedAt.IsZero() {
				j.FinishedAt = x.clock.Now()
			}
		}
		if j.Status == harness.JobCancelled {
			j.Fenced = true
		}
		return j, nil
	}
	j := x.jobs[e.ID]
	if j == nil {
		status := harness.JobAbsent
		if x.fenced[e.ID] {
			status = harness.JobCancelled
		}
		return harness.Job{EffectID: e.ID, Spec: e.Spec, Status: status, Fenced: x.fenced[e.ID]}, nil
	}
	if j.Status == harness.JobRunning {
		if !x.hold {
			x.finish(j, harness.JobSucceeded)
		} else if x.tick != nil {
			x.tick()
		}
	}
	return j.Job, nil
}
func (x *fakeExec) Inspect(ctx context.Context, e harness.Effect) (harness.Job, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.calls = append(x.calls, "inspect")
	if err := ctx.Err(); err != nil {
		return harness.Job{}, err
	}
	if e.Spec != x.Specification(e.Command) {
		return harness.Job{}, errors.New("changed specification")
	}
	if j := x.jobs[e.ID]; j != nil && j.command != e.Command {
		return harness.Job{}, errors.New("changed command")
	}
	return x.sample(e)
}
func (x *fakeExec) Cancel(ctx context.Context, e harness.Effect) (harness.Job, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.calls = append(x.calls, "cancel")
	if err := ctx.Err(); err != nil {
		return harness.Job{}, err
	}
	if e.Spec != x.Specification(e.Command) {
		return harness.Job{}, errors.New("changed specification")
	}
	j := x.jobs[e.ID]
	if j != nil && j.command != e.Command {
		return harness.Job{}, errors.New("changed command")
	}
	if j == nil {
		x.fenced[e.ID] = true
	} else if j.Status == harness.JobPending || j.Status == harness.JobRunning {
		x.fenced[e.ID] = true
		x.finish(j, harness.JobCancelled)
		j.Fenced = true
	}
	return x.sample(e)
}

func (x *fakeExec) finish(j *fakeJob, status harness.JobStatus) {
	j.Status, j.FinishedAt, j.Preview = status, x.clock.Now(), "ran: "+j.command
	if status == harness.JobCancelled && j.StartedAt.IsZero() {
		j.FinishedAt = time.Time{}
		j.Preview = ""
		j.Output = harness.ArtifactRef{}
		return
	}
	if status == harness.JobCancelled && !j.StartedAt.IsZero() {
		code := 143
		j.ExitCode = &code
	}
	if status == harness.JobSucceeded {
		code := 0
		j.ExitCode = &code
	}
	j.Output = harness.ArtifactRef{Path: j.ID + ".log", Bytes: int64(len(j.Preview)), Complete: true, Available: true}
}

func TestFakeExecutorPassesStartLatchCheck(t *testing.T) {
	x := &fakeExec{clock: &clock{now: created}, jobs: map[string]*fakeJob{}, fenced: map[string]bool{}}
	effect := func(name string) harness.Effect {
		return harness.Effect{ID: fmt.Sprintf("effect_%x", sha256.Sum256([]byte(name))), Command: "run " + name, Spec: x.Specification("run " + name)}
	}
	held := effect("start-cancel").ID
	x.inspect = func(effectID string) (harness.Job, bool, error) {
		j := x.jobs[effectID]
		if j == nil {
			return harness.Job{}, false, nil
		}
		if j.Status == harness.JobRunning && effectID != held {
			x.finish(j, harness.JobSucceeded)
		}
		return j.Job, true, nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err := executortest.Check(ctx, executortest.Fixture{
		// fakeExec keeps no state per controller, so every controller opens
		// the same backend.
		Open:   func(context.Context) (harness.Executor, error) { return x, nil },
		Effect: effect,
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

// workingSetScenario is a synthetic task whose evidence is ranges files of
// archive.MaxBytes each under the imports root. The agent completes units of
// work, no-op commands; unit u needs range u/perRange, or every range when
// perRange is zero, shown whole in the request that decides it. An oversized
// scenario's joint set exceeds the work capacity of its window.
type workingSetScenario struct {
	name      string
	ranges    int
	units     int
	perRange  int
	phases    int // when positive, unit u needs the ranges of its phase: an equal share of the ranges, in order
	pad       int // bytes of padding in the command of a unit of work
	total     int // PromptLimits.Total
	maxSteps  int
	oversized bool
	// carry makes the agent hand over carrying the ranges its next unit
	// needs, leaving out the last of them when miss is set; marginStep makes
	// the loop admit a carry only with one maximal step of work capacity to
	// spare.
	carry, miss, marginStep bool
}

func (sc workingSetScenario) needs(unit int) []int {
	first, count := 0, sc.ranges
	switch {
	case sc.perRange > 0:
		first, count = unit/sc.perRange, 1
	case sc.phases > 0:
		count = sc.ranges / sc.phases
		first = unit * sc.phases / sc.units * count
	}
	out := make([]int, count)
	for i := range out {
		out[i] = first + i
	}
	return out
}

func workingSetMarker(r int) string { return fmt.Sprintf("EVIDENCE-%02d-BEGIN", r) }

func workingSetPath(r int) string { return fmt.Sprintf("%s/range-%02d", archive.ImportsRoot, r) }

func workingSetContent(r int) string {
	marker := workingSetMarker(r) + "\n"
	line := strings.Repeat(".", 63) + "\n"
	body := strings.Repeat(line, (archive.MaxBytes-len(marker))/len(line)+1)
	return (marker + body)[:archive.MaxBytes]
}

const workingSetUnit = ": work unit "

var (
	workingSetDone    = regexp.MustCompile(`Completed units: (\d+)\.`)
	workingSetWork    = regexp.MustCompile("(?m)^" + workingSetUnit + `(\d+) `)
	workingSetRead    = regexp.MustCompile("### Step (\\d+)\nPurpose: load evidence\nCommand:\n```\n" + regexp.QuoteMeta(harness.RetrievePrefix) + `\{"operation":"read","path":"` + regexp.QuoteMeta(archive.ImportsRoot) + `/range-(\d+)"`)
	workingSetCarried = regexp.MustCompile(`Carried step (\d+) source 0: ` + regexp.QuoteMeta(archive.ImportsRoot) + `/range-(\d+)\n`)
)

// decide is the agent: a pure function of the frozen request body, so a
// retry or a restarted controller decides the same. A range counts as seen
// only when the request shows all of its bytes, so a policy that cuts or
// stubs evidence cannot pass for one that shows it. The agent knows the
// progress its handover and visible units of work report, as the workspace
// would let a real agent check, and rereads exactly the whole ranges the
// next unit needs. That is an assumption of this workload, not a bound on a
// real model, which may search first, or work from a checkpoint or from a
// smaller representation that suffices.
func (sc workingSetScenario) decide(body string) core.Decision {
	done := 0
	for _, m := range workingSetDone.FindAllStringSubmatch(body, -1) {
		n, _ := strconv.Atoi(m[1])
		done = max(done, n)
	}
	for _, m := range workingSetWork.FindAllStringSubmatch(body, -1) {
		n, _ := strconv.Atoi(m[1])
		done = max(done, n+1)
	}
	if done >= sc.units {
		return core.Decision{Kind: core.Return, Purpose: "all units done", Payload: "done"}
	}
	if strings.Contains(body, "\n## Handover required\n") {
		text := fmt.Sprintf("Completed units: %d. Reread the ranges under %s that the next unit needs, then continue.", done, archive.ImportsRoot)
		// After a refused proposal the agent hands over without evidence.
		if entries := sc.carried(body, done); sc.carry && len(entries) > 0 && !strings.Contains(body, "This step is the last chance to hand over") {
			args, _ := json.Marshal(struct {
				Text  string           `json:"text"`
				Carry []map[string]int `json:"carry"`
			}{text, entries})
			return core.Decision{Kind: core.Exec, Purpose: "hand over with evidence", Payload: harness.HandoverCarryPrefix + string(args)}
		}
		return core.Decision{Kind: core.Exec, Purpose: "hand over", Payload: harness.HandoverPrefix + text}
	}
	for _, r := range sc.needs(done) {
		if !strings.Contains(body, workingSetContent(r)) {
			q, _ := json.Marshal(archive.RetrievalQuery{Operation: "read", Path: workingSetPath(r), Reason: "the next unit needs this range", Limit: archive.MaxBytes})
			return core.Decision{Kind: core.Exec, Purpose: "load evidence", Payload: harness.RetrievePrefix + string(q)}
		}
	}
	return core.Decision{Kind: core.Exec, Purpose: "work", Payload: fmt.Sprintf("%s%d %s", workingSetUnit, done, strings.Repeat("w", sc.pad))}
}

// carried returns the carry entries for the ranges that unit, the next one,
// needs: the step that read or carries each range the request shows, but
// the last one when the scenario misses it.
func (sc workingSetScenario) carried(body string, unit int) []map[string]int {
	steps := map[int]int{}
	for _, re := range []*regexp.Regexp{workingSetCarried, workingSetRead} {
		for _, m := range re.FindAllStringSubmatch(body, -1) {
			step, _ := strconv.Atoi(m[1])
			r, _ := strconv.Atoi(m[2])
			steps[r] = step
		}
	}
	needs := sc.needs(unit)
	if sc.miss {
		needs = needs[:len(needs)-1]
	}
	var out []map[string]int
	seen := map[int]bool{}
	for _, r := range needs {
		if step, ok := steps[r]; ok && !seen[step] {
			seen[step] = true
			out = append(out, map[string]int{"step": step})
		}
	}
	return out
}

type workingSetAgent struct{ sc workingSetScenario }

func (a workingSetAgent) Freeze(p harness.Prompt) (harness.Request, error) {
	return harness.Request{Body: []byte(p.System + "\n\n" + p.User()), Model: "working-set-1", Protocol: "fake/v1", Adapter: "fake"}, nil
}

// Complete reports no usage: an offline agent has no tokens to report.
func (a workingSetAgent) Complete(_ context.Context, req harness.Request) (harness.Response, error) {
	body, err := json.Marshal(a.sc.decide(string(req.Body)))
	return harness.Response{Body: body, Complete: true, Model: "working-set-1", Protocol: "fake/v1", Adapter: "fake"}, err
}

func (a workingSetAgent) Decision(r harness.Response) []byte { return r.Body }

// runWorkingSet drives sc to a halt over a SQLite journal and returns the
// open journal and the final snapshot. With restart set, each Loop.Step runs
// on a new loop over a journal opened after the previous one is closed. The
// restarts fall only between steps, and the fake executor and clock are
// shared across them, as a backend outlives its controllers; crashes inside
// a step are the subject of the recovery tests. The run halts within
// sc.maxSteps, each a few commits; the deadline only stops a regression that
// polls without committing.
func runWorkingSet(tb testing.TB, sc workingSetScenario, restart bool) (*sqlite.Store, harness.Snapshot) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	dir := tb.TempDir()
	imports := filepath.Join(dir, "imports")
	if err := os.Mkdir(imports, 0o700); err != nil {
		tb.Fatal(err)
	}
	for r := range sc.ranges {
		if err := os.WriteFile(filepath.Join(imports, path.Base(workingSetPath(r))), []byte(workingSetContent(r)), 0o600); err != nil {
			tb.Fatal(err)
		}
	}
	open := func() *sqlite.Store {
		s, err := sqlite.Open(ctx, filepath.Join(dir, "canti.db"))
		if err != nil {
			tb.Fatal(err)
		}
		return s
	}
	store := open()
	tb.Cleanup(func() { store.Close() })
	state, err := core.NewState(runID, core.Limits{MaxSteps: sc.maxSteps, MaxAttempts: 3})
	if err != nil {
		tb.Fatal(err)
	}
	// Every arm of a scenario has its goal, so arms render the same goal.
	base, _, _ := strings.Cut(sc.name, "/")
	if err := store.CreateRun(ctx, harness.Run{ID: runID, Goal: "Complete every unit of the scenario " + base + ".", Created: created}, state); err != nil {
		tb.Fatal(err)
	}
	c := &clock{now: created}
	x := &fakeExec{clock: c, jobs: map[string]*fakeJob{}, fenced: map[string]bool{}}
	cfg := harness.LoopConfig{Environment: "fixture shell", History: filepath.Join(dir, "history"),
		Reader: &archive.Reader{Imports: imports}, Now: c.Now, Poll: time.Microsecond, Backoff: -1, Limits: harness.DefaultPromptLimits()}
	cfg.Limits.Total = sc.total
	if sc.marginStep {
		harness.SetCarryMarginStep(&cfg)
	}
	loop := func() *harness.Loop {
		l, err := harness.NewLoop(store, x, workingSetAgent{sc}, cfg)
		if err != nil {
			tb.Fatal(err)
		}
		return l
	}
	l := loop()
	for {
		if restart {
			if err := store.Close(); err != nil {
				tb.Fatal(err)
			}
			store = open()
			l = loop()
		}
		snap, err := l.Step(ctx, runID)
		if err != nil {
			tb.Fatalf("Step() error = %v at %+v", err, snap.State)
		}
		if snap.State.Intent() == core.IntentHalt {
			return store, snap
		}
	}
}

func fixtureExecution(command string) *harness.ExecutionIdentity {
	return &harness.ExecutionIdentity{Spec: harness.ConfigDigest([]byte("fixture:" + command)), ConfigDigest: harness.ConfigDigest(nil)}
}
func fixtureReceipt(s core.State, obs harness.Observation) harness.Job {
	j := harness.Job{EffectID: s.EffectID, Spec: fixtureExecution(s.Decision.Payload).Spec, ID: jobID(s.EffectID), StartedAt: created, FinishedAt: created, ExitCode: obs.ExitCode, Preview: obs.Preview, Output: obs.Output}
	switch obs.Execution {
	case core.ExecutionExited:
		j.Status = harness.JobSucceeded
		if obs.ExitCode != nil && *obs.ExitCode != 0 {
			j.Status = harness.JobFailed
		}
	case core.ExecutionCancelled, core.ExecutionTimedOut:
		j.Status, j.Fenced = harness.JobCancelled, true
		if j.ExitCode == nil {
			code := 143
			j.ExitCode = &code
		}
	case core.ExecutionFailedToStart:
		j.Status = harness.JobFailedToStart
		j.StartedAt = time.Time{}
		j.ExitCode = nil
	case core.ExecutionUnknown:
		j.Status = harness.JobUnknown
	}
	return j
}

func (f *fixture) runningEffect() harness.Snapshot {
	f.t.Helper()
	snap := f.steps(f.loop(), 3)
	e := harness.Effect{ID: snap.State.EffectID, Command: snap.State.Decision.Payload, Spec: f.exec.Specification(snap.State.Decision.Payload)}
	if _, err := f.exec.Drive(f.bounded(), e); err != nil {
		f.t.Fatal(err)
	}
	return snap
}
