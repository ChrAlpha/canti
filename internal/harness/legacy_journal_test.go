package harness_test

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

// legacyExpectation is what the current harness does when it resumes a
// legacy journal: the decisions its scripted model returns, the event kinds
// it appends, and the run's answer.
type legacyExpectation struct {
	replies func(runID string) []string
	kinds   []string
	answer  string
	check   func(t *testing.T, r legacyResult)
}

func legacyExpectations() map[string]legacyExpectation {
	notes := "The notes are numbered plain-text lines."
	return map[string]legacyExpectation{
		legacySchema1 + "/retrieval-awaiting-observation": {
			replies: func(string) []string { return []string{corpusDecision(core.Return, notes)} },
			kinds:   append([]string{"observed"}, closing...),
			answer:  notes,
			check: func(t *testing.T, r legacyResult) {
				preview := "/canti/runs/" + r.run.ID + "/steps/00000001/preview"
				data, ok := r.copy.files["history"+preview[len("/canti/runs"):]]
				o := r.suffix[0].Facts.Observation
				if !ok || o == nil || o.Execution != core.ExecutionExited || o.ExitCode == nil || *o.ExitCode != 0 ||
					o.Retrieval == nil || o.Retrieval.Error != "" || len(o.Retrieval.Sources) != 1 {
					t.Fatalf("the paused retrieval observed %+v", o)
				}
				want := harness.SourceRange{Path: preview, Length: len(data), SHA256: legacyHash(data)}
				if got := o.Retrieval.Sources[0]; got != want {
					t.Errorf("the retrieval read %+v, not the producer's published preview %+v", got, want)
				}
				legacySamePrefix(t, r.frozen(t, 2, 1), r.suffix[1].Facts.Request.Body, 2)
			},
		},
	}
}

// TestLegacyJournals opens every journal of the corpus with the current
// store, reads it through every journal, session, and context read, resumes
// its paused run with the current loop, and requires that no recorded row
// changes except the run counters a commit raises.
func TestLegacyJournals(t *testing.T) {
	names, expectations := legacyCaseNames(t), legacyExpectations()
	for name := range expectations {
		if !slices.Contains(names, name) {
			t.Errorf("legacy journal %s is missing", name)
		}
	}
	var stored int64
	for _, name := range names {
		x, ok := expectations[name]
		if !ok {
			t.Errorf("legacy journal %s has no expectation", name)
			continue
		}
		t.Run(name[:7]+"/"+filepath.Base(name), func(t *testing.T) { stored += testLegacyJournal(t, name, x) })
	}
	if stored > legacyMaxCorpus {
		t.Errorf("the legacy corpus stores %d bytes, more than %d", stored, legacyMaxCorpus)
	}
}

func testLegacyJournal(t *testing.T, name string, x legacyExpectation) int64 {
	c := openLegacyCase(t, name)
	before := snapshotLegacy(t, c.db)
	summary := legacySummary(t, before)
	summary.DB = c.manifest.Journal.DB
	if !reflect.DeepEqual(summary, c.manifest.Journal) {
		t.Fatalf("the journal does not match its manifest:\n got %+v\nwant %+v", summary, c.manifest.Journal)
	}
	runs := before.runs(t)
	paused := runs[len(runs)-1].text(t, "id")
	store, err := sqlite.Open(context.Background(), c.db)
	if err != nil {
		t.Fatal(err)
	}
	exerciseLegacyReads(t, store, before, paused)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	compareLegacy(t, before, snapshotLegacy(t, c.db), nil, nil)
	r := resumeLegacy(t, c, before, x)
	var grown legacyCounters
	for _, tr := range r.suffix {
		switch tr.Event.(type) {
		case core.ResponseSaved:
			grown.input, grown.output = grown.input+legacyUsage.InputTokens, grown.output+legacyUsage.OutputTokens
		case core.Observed, core.Resolved:
			grown.steps++
		}
	}
	after := snapshotLegacy(t, c.db)
	compareLegacy(t, before, after, map[string]legacyCounters{paused: grown}, map[string]int{paused: len(r.suffix)})
	if store, err = sqlite.Open(context.Background(), c.db); err != nil {
		t.Fatal(err)
	}
	exerciseLegacyReads(t, store, after, "")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	x.check(t, r)
	return c.stored
}

// legacyUsage is what corpusModel reports for every response.
var legacyUsage = harness.Usage{InputTokens: 1000, OutputTokens: 20}

type legacyResult struct {
	copy   legacyCopy
	before legacyDB
	run    harness.Run
	paused core.State
	suffix []harness.Transition
	sent   [][]byte
}

// frozen returns the body of a request the producer froze, from the raw
// journal.
func (r legacyResult) frozen(t *testing.T, step, rev int) []byte {
	t.Helper()
	for _, row := range r.before.transitions(t, r.run.ID) {
		if row.text(t, "kind") == "request_frozen" && row.num(t, "step") == int64(step) && row.num(t, "rev") == int64(rev) {
			return legacyFactsOf(t, row).Request.Body
		}
	}
	t.Fatalf("the producer froze no request for step %d revision %d", step, rev)
	return nil
}

// legacyConfig is the CLI configuration the producers stored with each run.
type legacyConfig struct {
	Workspace      string `json:"workspace"`
	Image          string `json:"image"`
	Network        string `json:"network"`
	Memory         string `json:"memory"`
	CPUs           string `json:"cpus"`
	Pids           int    `json:"pids"`
	OutputLimit    int64  `json:"output_limit"`
	Model          string `json:"model"`
	MaxTokens      int    `json:"max_tokens"`
	Structured     bool   `json:"structured"`
	DecisionFormat string `json:"decision_format,omitempty"`
	Fallbacks      bool   `json:"fallbacks"`
	ContextBytes   int    `json:"context_bytes"`
	ContextVersion int    `json:"context_version"`
	HistoryImports string `json:"history_imports,omitempty"`
	WindowSteps    int    `json:"window_steps,omitempty"`
	WindowBytes    int    `json:"window_bytes,omitempty"`
	HandoverBytes  int    `json:"handover_bytes,omitempty"`
}

// legacyEnvironment is the environment text the CLI renders from c.
func legacyEnvironment(c legacyConfig, b harness.Budget) string {
	pids, timeout := "unlimited", "unlimited"
	if c.Pids > 0 {
		pids = strconv.Itoa(c.Pids)
	}
	if b.JobTimeout > 0 {
		timeout = b.JobTimeout.String()
	}
	return fmt.Sprintf("Each command runs in a new Linux container from image %s with working directory /workspace, "+
		"which is read-write and persists between steps. Network: %s. Per-command limits: memory %s, CPUs %s, processes %s, run time %s.",
		c.Image, c.Network, cmp.Or(c.Memory, "unlimited"), cmp.Or(c.CPUs, "unlimited"), pids, timeout)
}

// legacyExecutor records every call and runs nothing: the pilots act only
// through built-in actions.
type legacyExecutor struct {
	mu    sync.Mutex
	calls []string
}

func (x *legacyExecutor) note(call string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.calls = append(x.calls, call)
	return errors.New("legacy journal pilots run no jobs")
}

func (x *legacyExecutor) Bind(_ context.Context, e harness.Effect) (harness.Job, error) {
	return harness.Job{}, x.note("bind " + e.ID)
}

func (x *legacyExecutor) Start(_ context.Context, id string) (harness.Job, error) {
	return harness.Job{}, x.note("start " + id)
}

func (x *legacyExecutor) Inspect(_ context.Context, id string) (harness.Job, bool, error) {
	return harness.Job{}, false, x.note("inspect " + id)
}

func (x *legacyExecutor) Cancel(_ context.Context, id string) error { return x.note("cancel " + id) }

// resumeLegacy drives the latest run of the journal one step per reopened
// store, as the CLI configures its loop, until it halts, and checks the
// properties every resume must keep.
func resumeLegacy(t *testing.T, c legacyCopy, before legacyDB, x legacyExpectation) legacyResult {
	t.Helper()
	runs := before.runs(t)
	r := legacyResult{copy: c, before: before}
	if err := json.Unmarshal([]byte(runs[len(runs)-1].text(t, "run")), &r.run); err != nil {
		t.Fatal(err)
	}
	var cfg legacyConfig
	if err := legacyStrict(r.run.Config, &cfg); err != nil {
		t.Fatalf("run config: %v", err)
	}
	rows := before.transitions(t, r.run.ID)
	r.paused = legacyState(t, rows[len(rows)-1].text(t, "to_state"))
	model := &corpusModel{tool: cfg.DecisionFormat == "tool", replies: x.replies(r.run.ID)}
	server := httptest.NewServer(model)
	defer server.Close()
	m, err := anthropic.New(anthropic.Config{BaseURL: server.URL, APIKey: "legacy-consumer-key", Model: cfg.Model, MaxTokens: cfg.MaxTokens,
		StructuredOutput: cfg.Structured, DecisionFormat: cfg.DecisionFormat, Fallbacks: cfg.Fallbacks})
	if err != nil {
		t.Fatal(err)
	}
	history, artifacts := filepath.Join(c.roots, "history"), filepath.Join(c.roots, "artifacts")
	for _, dir := range []string{history, artifacts} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	reader := &harness.Reader{History: history, Artifacts: artifacts}
	if cfg.HistoryImports != "" {
		reader.Imports = filepath.Join(c.roots, "imports")
	}
	limits := harness.DefaultPromptLimits()
	limits.Total = cfg.ContextBytes
	executor := &legacyExecutor{}
	lc := harness.LoopConfig{Environment: legacyEnvironment(cfg, r.run.Budget), ArtifactDir: "/canti/artifacts", History: history,
		RunsPath: "/canti/runs", Limits: limits, Poll: time.Millisecond, Backoff: -1, Reader: reader,
		Now:         func() time.Time { return r.run.Created.Add(time.Minute) },
		WindowSteps: cfg.WindowSteps, WindowBytes: cfg.WindowBytes, HandoverBytes: cfg.HandoverBytes}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var snap harness.Snapshot
	for i := 0; ; i++ {
		if i == 128 {
			t.Fatalf("run %s did not halt within 128 steps: %+v", r.run.ID, snap.State)
		}
		store, err := sqlite.Open(ctx, c.db)
		if err != nil {
			t.Fatal(err)
		}
		loop, err := harness.NewLoop(store, executor, m, lc)
		if err == nil {
			snap, err = loop.Step(ctx, r.run.ID)
		}
		if err = errors.Join(err, store.Close()); err != nil {
			t.Fatalf("step %d of run %s: %v", i+1, r.run.ID, err)
		}
		if snap.State.Intent() == core.IntentHalt {
			break
		}
	}
	if snap.State.Phase != core.PhaseReturned || snap.State.Decision.Payload != x.answer {
		t.Fatalf("run %s halted in %s with %q, want returned with %q", r.run.ID, snap.State.Phase, snap.State.Decision.Payload, x.answer)
	}
	store, err := sqlite.Open(ctx, c.db)
	if err != nil {
		t.Fatal(err)
	}
	all, err := store.Transitions(ctx, r.run.ID, 0, 1<<16)
	if err = errors.Join(err, store.Close()); err != nil {
		t.Fatal(err)
	}
	r.suffix = all[len(rows):]
	var kinds []string
	for _, tr := range r.suffix {
		kinds = append(kinds, tr.Event.Kind())
	}
	if !slices.Equal(kinds, x.kinds) {
		t.Fatalf("resume appended %v, want %v", kinds, x.kinds)
	}
	for _, tr := range r.suffix {
		if next, err := tr.From.Next(tr.Event); err != nil || next != tr.To {
			t.Fatalf("seq %d does not replay under the current semantics: %v", tr.Seq, err)
		}
	}
	r.sent = model.sent()
	model.mu.Lock()
	unused := len(model.replies)
	model.mu.Unlock()
	checkLegacySends(t, r, unused)
	checkLegacyEffects(t, all)
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if len(executor.calls) != 0 {
		t.Errorf("the executor was called: %v", executor.calls)
	}
	return r
}

// checkLegacySends requires that the model received exactly the request
// frozen for each appended attempt, once, and every scripted reply was used.
func checkLegacySends(t *testing.T, r legacyResult, unused int) {
	t.Helper()
	frozen := map[[2]int][]byte{}
	for _, row := range r.before.transitions(t, r.run.ID) {
		if row.text(t, "kind") == "request_frozen" {
			frozen[[2]int{int(row.num(t, "step")), int(row.num(t, "rev"))}] = legacyFactsOf(t, row).Request.Body
		}
	}
	var want [][]byte
	for _, tr := range r.suffix {
		key := [2]int{tr.To.Step, tr.To.RequestRev}
		if tr.Facts.Request != nil {
			frozen[key] = tr.Facts.Request.Body
		}
		if _, ok := tr.Event.(core.AttemptStarted); ok {
			want = append(want, frozen[key])
		}
	}
	if len(r.sent) != len(want) || unused != 0 {
		t.Fatalf("the model received %d requests for %d attempts, leaving %d replies", len(r.sent), len(want), unused)
	}
	for i := range want {
		if !bytes.Equal(r.sent[i], want[i]) {
			t.Errorf("request %d is not the bytes frozen for its attempt", i+1)
		}
	}
}

// checkLegacyEffects requires that no effect was bound, started, or
// observed twice across the historical and appended transitions.
func checkLegacyEffects(t *testing.T, all []harness.Transition) {
	t.Helper()
	seen := map[string]int{}
	for _, tr := range all {
		switch e := tr.Event.(type) {
		case core.JobBound:
			seen["bound "+e.EffectID]++
		case core.JobStarted:
			seen["started "+e.EffectID]++
		case core.Observed:
			seen["observed "+e.EffectID]++
		case core.Resolved:
			seen["observed "+e.EffectID]++
		}
	}
	for what, n := range seen {
		if n > 1 {
			t.Errorf("%s %d times", what, n)
		}
	}
}

func legacyBlocks(t *testing.T, body []byte) (string, []string) {
	t.Helper()
	var b struct {
		System   string `json:"system"`
		Messages []struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &b); err != nil || len(b.Messages) != 1 {
		t.Fatalf("request body is not one Messages request: %v", err)
	}
	var blocks []string
	for _, c := range b.Messages[0].Content {
		blocks = append(blocks, c.Text)
	}
	return b.System, blocks
}

// legacySamePrefix requires that the next request repeats the system prompt
// and the first n blocks of the producer's request byte for byte.
func legacySamePrefix(t *testing.T, producer, next []byte, n int) {
	t.Helper()
	system, blocks := legacyBlocks(t, producer)
	nextSystem, nextBlocks := legacyBlocks(t, next)
	if system != nextSystem {
		t.Error("the next request changed the producer's system prompt")
	}
	if len(blocks) < n || len(nextBlocks) < n {
		t.Fatalf("requests of %d and %d blocks have no %d-block prefix", len(blocks), len(nextBlocks), n)
	}
	for i := range n {
		if blocks[i] != nextBlocks[i] {
			t.Errorf("block %d of the next request differs from the producer's", i+1)
		}
	}
}
