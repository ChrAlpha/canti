package harness_test

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

type contextCostModel struct {
	*anthropic.Model
	commands []string
}

func (m *contextCostModel) Complete(_ context.Context, _ harness.Request) (harness.Response, error) {
	if len(m.commands) == 0 {
		return harness.Response{}, fmt.Errorf("no scripted decision")
	}
	body, err := json.Marshal(core.Decision{Kind: core.Exec, Payload: m.commands[0]})
	m.commands = m.commands[1:]
	return harness.Response{Body: body, Complete: true}, err
}

func (m *contextCostModel) Decision(r harness.Response) []byte { return r.Body }

type contextCostFixture struct {
	store      *sqlite.Store
	db         *sql.DB
	loop       *harness.Loop
	probe      *probe
	runner     harness.Runner
	snapshot   harness.Snapshot
	request    *harness.Request
	factsBytes int
}

func newContextCostFixture(b *testing.B, scenario string) contextCostFixture {
	b.Helper()
	ctx := context.Background()
	root := b.TempDir()
	store, err := sqlite.Open(ctx, filepath.Join(root, "canti.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })
	state, err := core.NewState(runID, core.Limits{MaxSteps: 128, MaxAttempts: 3})
	if err != nil {
		b.Fatal(err)
	}
	if err := store.CreateRun(ctx, harness.Run{ID: runID, Goal: "Inspect the original evidence.", Created: created}, state); err != nil {
		b.Fatal(err)
	}
	model, err := anthropic.New(anthropic.Config{APIKey: "benchmark-fixture", Model: "fixture-model"})
	if err != nil {
		b.Fatal(err)
	}
	imported := filepath.Join(root, "archive")
	if err := os.Mkdir(imported, 0700); err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte("evidence\n"), 0600); err != nil {
		b.Fatal(err)
	}
	commands := []string{"true"}
	switch scenario {
	case "near_capacity":
		commands = make([]string, 100)
		for i := range commands {
			commands[i] = "printf '" + strings.Repeat("payload ", 700) + "'"
		}
	case "retrieval_dense":
		commands = make([]string, 32)
		for i := range commands {
			commands[i] = `canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"verify evidence","limit":8}`
		}
	case "rejected_1MiB":
		query, err := json.Marshal(archive.RetrievalQuery{Operation: "read", Path: "/canti/imports/evidence", Reason: strings.Repeat("x", 1<<20)})
		if err != nil {
			b.Fatal(err)
		}
		commands = []string{"canti:retrieve " + string(query), "true", "true", "true"}
	}
	m := &contextCostModel{Model: model, commands: commands}
	c := &clock{now: created}
	x := &fakeExec{clock: c, jobs: map[string]*fakeJob{}, fenced: map[string]bool{}}
	cfg := harness.LoopConfig{Environment: "fixture shell",
		History: filepath.Join(root, "history"), Reader: &archive.Reader{Imports: imported}, Now: c.Now,
		Poll: time.Microsecond, Backoff: -1, Limits: harness.DefaultPromptLimits()}
	cfg.Limits.Total = 256 << 10
	loop, err := harness.NewLoop(store, x, m, cfg)
	if err != nil {
		b.Fatal(err)
	}
	p := &probe{Store: store}
	prepare, err := harness.NewLoop(p, x, m, cfg)
	if err != nil {
		b.Fatal(err)
	}
	var snap harness.Snapshot
	ready := false
	for range 1000 {
		snap, err = store.Latest(ctx, runID)
		if err != nil {
			b.Fatal(err)
		}
		if snap.State.Phase == core.PhaseIdle {
			if _, err := prepare.Step(ctx, runID); err != nil {
				b.Fatal(err)
			}
			if p.frozen == nil {
				b.Fatal("prepare did not freeze a request")
			}
			if scenario == "near_capacity" && p.frozen.Window.Requested || len(m.commands) == 0 {
				ready = true
				break
			}
		}
		if snap.State.Intent() == core.IntentHalt {
			b.Fatal("fixture halted before benchmark boundary")
		}
		if _, err := loop.Step(ctx, runID); err != nil {
			b.Fatal(err)
		}
	}
	if !ready || snap.State.Phase != core.PhaseIdle || p.frozen == nil {
		b.Fatal("fixture did not reach prepare boundary")
	}
	if scenario == "near_capacity" && !p.frozen.Window.Requested {
		b.Fatal("fixture did not reach the handover capacity threshold")
	}
	runner, err := harness.NewRunner(store)
	if err != nil {
		b.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "canti.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	f := contextCostFixture{store: store, db: db, loop: prepare, probe: p, runner: runner, snapshot: snap, request: p.frozen}
	if _, err := runner.Commit(ctx, snap, core.RequestFrozen{}, harness.Facts{Request: f.request}); err != nil {
		b.Fatal(err)
	}
	if err := db.QueryRow("SELECT length(CAST(facts AS BLOB)) FROM transitions WHERE run_id = ? AND seq = ?", runID, snap.Seq+1).Scan(&f.factsBytes); err != nil {
		b.Fatal(err)
	}
	f.resetRequest(b)
	b.Logf("scenario=%s step=%d handover_requested=%t body_bytes=%d frozen_facts_bytes=%d body_sha256=%x", scenario, snap.State.Step, f.request.Window.Requested, len(f.request.Body), f.factsBytes, sha256.Sum256(f.request.Body))
	return f
}

func (f contextCostFixture) resetRequest(b *testing.B) {
	b.Helper()
	if _, err := f.db.Exec("DELETE FROM transitions WHERE run_id = ? AND seq = ? AND kind = 'request_frozen'", runID, f.snapshot.Seq+1); err != nil {
		b.Fatal(err)
	}
}

func (f contextCostFixture) reportBytes(b *testing.B) {
	b.ReportMetric(float64(len(f.request.Body)), "body_bytes")
	b.ReportMetric(float64(f.factsBytes), "frozen_facts_bytes")
}

func BenchmarkContextCost(b *testing.B) {
	for _, scenario := range []string{"short", "near_capacity", "retrieval_dense", "rejected_1MiB"} {
		b.Run(scenario, func(b *testing.B) {
			f := newContextCostFixture(b, scenario)
			ctx := context.Background()
			b.Run("prepare_freeze", func(b *testing.B) {
				f.resetRequest(b)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if _, err := f.loop.Step(ctx, runID); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if sha256.Sum256(f.probe.frozen.Body) != sha256.Sum256(f.request.Body) {
					b.Fatal("repeated prepare changed the frozen body")
				}
				f.reportBytes(b)
			})
			b.Run("commit", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					f.resetRequest(b)
					b.StartTimer()
					if _, err := f.runner.Commit(ctx, f.snapshot, core.RequestFrozen{}, harness.Facts{Request: f.request}); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				f.reportBytes(b)
			})
			b.Run("reload", func(b *testing.B) {
				f.resetRequest(b)
				if _, err := f.runner.Commit(ctx, f.snapshot, core.RequestFrozen{}, harness.Facts{Request: f.request}); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				var request harness.Request
				var err error
				for b.Loop() {
					if request, err = f.store.Request(ctx, runID, f.snapshot.State.Step, f.snapshot.State.RequestRev+1); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if sha256.Sum256(request.Body) != sha256.Sum256(f.request.Body) {
					b.Fatal("reload changed the frozen body")
				}
				f.reportBytes(b)
			})
		})
	}
}

var (
	workingSetJournal = flag.String("working-set-journal", "", "BenchmarkWorkingSetOfAJournal measures a run of this canti journal, opened read-only")
	workingSetRun     = flag.String("working-set-run", "", "the run that -working-set-journal measures; empty selects the latest run")
)

// workingSetCounts are the counts and bytes of the context windows of a run
// that add up across windows. A read is a retrieval step and a range one
// source range it retrieved; work is a completed step that the progress
// predicate accepts. These are diagnostic categories: they say nothing about
// whether a step is safe to retry or makes real progress.
type workingSetCounts struct {
	Requests int `json:"requests"`
	// CapturesMissing counts requests frozen without a context capture,
	// whose rendered bytes the counts below cannot include.
	CapturesMissing int `json:"captures_missing"`
	Steps           int `json:"steps"`
	Reads           int `json:"reads"`
	Work            int `json:"work"`
	Handovers       int `json:"handover_steps"`
	// FailedHandovers counts plain handover steps, and FailedCarries those
	// that proposed to carry evidence, that recorded no accepted handover,
	// the run-ending second failure included.
	FailedHandovers int `json:"failed_handovers"`
	FailedCarries   int `json:"failed_carries"`
	// EvidenceBytes counts retrieved bytes; ReconstructedBytes those, and
	// RepeatedRanges the ranges holding them, that an earlier window had
	// already retrieved; RereadBytes those that the same window had.
	EvidenceBytes      int `json:"evidence_bytes"`
	ReconstructedBytes int `json:"reconstructed_bytes"`
	RepeatedRanges     int `json:"repeated_ranges"`
	RereadBytes        int `json:"reread_bytes"`
	// ResidentEvidenceBytes sums, over requests, the rendered bytes of the
	// retrieval steps each one shows; CarryBytes, those of the evidence a
	// handover carried into its window.
	ResidentEvidenceBytes int `json:"resident_evidence_bytes"`
	CarryBytes            int `json:"carry_bytes"`
	InputBytes            int `json:"input_bytes"`
	// CommonPrefixBytes sums the bytes each frozen body shares as a prefix
	// with the body frozen before it. It is an offline upper bound on what
	// a prefix cache could reuse, not a measured saving: providers reuse at
	// block or breakpoint granularity, with minimum lengths and expiry.
	CommonPrefixBytes int `json:"common_prefix_bytes"`
}

func (c *workingSetCounts) add(o workingSetCounts) {
	c.Requests += o.Requests
	c.CapturesMissing += o.CapturesMissing
	c.Steps += o.Steps
	c.Reads += o.Reads
	c.Work += o.Work
	c.Handovers += o.Handovers
	c.FailedHandovers += o.FailedHandovers
	c.FailedCarries += o.FailedCarries
	c.EvidenceBytes += o.EvidenceBytes
	c.ReconstructedBytes += o.ReconstructedBytes
	c.RepeatedRanges += o.RepeatedRanges
	c.RereadBytes += o.RereadBytes
	c.ResidentEvidenceBytes += o.ResidentEvidenceBytes
	c.CarryBytes += o.CarryBytes
	c.InputBytes += o.InputBytes
	c.CommonPrefixBytes += o.CommonPrefixBytes
}

// workingSetWindow measures one context window of a run from its journal.
type workingSetWindow struct {
	FirstStep int `json:"first_step"`
	workingSetCounts
	// ReadsBeforeWork and RequestsBeforeWork count the reads and the requests
	// of the window up to its first work; -1 when it has none.
	ReadsBeforeWork    int `json:"reads_before_work"`
	RequestsBeforeWork int `json:"requests_before_work"`
	PeakPromptBytes    int `json:"peak_prompt_bytes"`
	// At the first request of the window that requires a handover, the
	// rendered bytes of its steps and of its retrieval steps.
	HandoverStepBytes     int `json:"handover_step_bytes"`
	HandoverEvidenceBytes int `json:"handover_evidence_bytes"`
}

// workingSetUsage is what the responses recorded. The cache tokens are read
// from raw bodies for diagnosis only; a policy or an acceptance criterion
// that needs them needs usage that the adapters report.
type workingSetUsage struct {
	Responses     int   `json:"responses"`
	Input         int64 `json:"input_tokens"`
	Output        int64 `json:"output_tokens"`
	CacheRead     int64 `json:"cache_read_tokens"`
	CacheCreation int64 `json:"cache_creation_tokens"`
}

type workingSetReport struct {
	Capacity int                `json:"first_work_capacity"`
	Windows  []workingSetWindow `json:"windows"`
	Total    workingSetCounts   `json:"total"`
	Usage    workingSetUsage    `json:"usage"`
}

type workingSetRange struct {
	window     int
	start, end int64
}

// workingSetMeasure accumulates a report from the transitions of one run in
// commit order.
type workingSetMeasure struct {
	report   workingSetReport
	progress func(harness.StepRecord) bool
	stepOf   map[int]int // step -> index of the window whose request decided it
	reads    map[int]bool
	ranges   map[string][]workingSetRange
	previous []byte
}

// measureWorkingSet reads every transition of runID and measures each of its
// context windows; progress decides which completed steps are work. It fails
// rather than undercount: on a request without a window, a completed step no
// request decided, a step completed by an event it does not know, and
// retrieved evidence that does not match its source reference.
func measureWorkingSet(ctx context.Context, j *sqlite.Store, runID string, progress func(harness.StepRecord) bool) (workingSetReport, error) {
	m := workingSetMeasure{progress: progress, stepOf: map[int]int{}, reads: map[int]bool{}, ranges: map[string][]workingSetRange{}}
	var after int64
	for {
		page, err := j.Transitions(ctx, runID, after, 256)
		if err != nil {
			return m.report, err
		}
		for _, t := range page {
			if err := m.add(t); err != nil {
				return m.report, err
			}
			after = t.Seq
		}
		if len(page) < 256 {
			break
		}
	}
	for _, w := range m.report.Windows {
		m.report.Total.add(w.workingSetCounts)
	}
	return m.report, nil
}

func (m *workingSetMeasure) add(t harness.Transition) error {
	kind := t.Event.Kind()
	switch {
	case kind == "request_frozen":
		return m.request(t)
	case kind == "response_saved" || kind == "response_incomplete":
		m.usage(t.Facts.Response)
	case kind == "performed" && t.Facts.Outcome == nil:
		return fmt.Errorf("transition %d performs step %d without an outcome", t.Seq, t.From.Step)
	case kind == "performed":
		return m.step(harness.StepRecord{Seq: t.Seq, Step: t.From.Step, Command: t.From.Decision.Payload, Purpose: t.From.Decision.Purpose,
			Outcome: t.Facts.Outcome, Window: t.Facts.Window})
	case (kind == "observed" || kind == "resolved") && t.Facts.Observation == nil:
		return fmt.Errorf("transition %d completes step %d without an observation", t.Seq, t.From.Step)
	case kind == "observed" || kind == "resolved":
		return m.step(harness.StepRecord{Seq: t.Seq, Step: t.From.Step, Command: t.From.Decision.Payload, Purpose: t.From.Decision.Purpose,
			Observation: *t.Facts.Observation, Resolved: kind == "resolved"})
	case t.Facts.Observation != nil || t.Facts.Outcome != nil || t.To.Step != t.From.Step:
		return fmt.Errorf("transition %d completes step %d by %s, which this measurement does not know", t.Seq, t.From.Step, kind)
	}
	return nil
}

// workingSetResult returns the read and the handover that step s recorded,
// in its outcome or, before actions were recorded, in its observation, and
// whether it accepted that handover.
func workingSetResult(s harness.StepRecord) (*archive.Retrieval, *harness.Handover, bool) {
	if o := s.Outcome; o != nil {
		return o.Retrieval, o.Handover, o.Status == harness.OutcomeDone && o.Handover != nil
	}
	o := s.Observation
	exited := o.Execution == core.ExecutionExited && o.ExitCode != nil && *o.ExitCode == 0
	return o.Retrieval, o.Handover, exited && o.Handover != nil
}

func (m *workingSetMeasure) request(t harness.Transition) error {
	r := t.Facts.Request
	if r == nil || r.Window == nil {
		return fmt.Errorf("request at transition %d has no context window", t.Seq)
	}
	ws := &m.report.Windows
	if n := len(*ws); n == 0 || (*ws)[n-1].FirstStep != r.Window.FirstStep {
		*ws = append(*ws, workingSetWindow{FirstStep: r.Window.FirstStep, ReadsBeforeWork: -1, RequestsBeforeWork: -1})
	}
	i := len(*ws) - 1
	w := &(*ws)[i]
	m.stepOf[t.To.Step] = i
	w.Requests++
	w.InputBytes += len(r.Body)
	prefix := 0
	for prefix < min(len(r.Body), len(m.previous)) && r.Body[prefix] == m.previous[prefix] {
		prefix++
	}
	w.CommonPrefixBytes += prefix
	m.previous = r.Body
	c := r.Context
	if c == nil {
		w.CapturesMissing++
		return nil
	}
	w.PeakPromptBytes = max(w.PeakPromptBytes, c.Bytes.Total)
	if c.Handover.Status == "evaluated" && m.report.Capacity == 0 {
		m.report.Capacity = c.Handover.Capacity
	}
	steps, evidence := 0, 0
	for _, s := range c.Bytes.Steps {
		steps += s.Bytes
		if m.reads[s.Step] {
			evidence += s.Bytes
		}
	}
	w.ResidentEvidenceBytes += evidence
	w.CarryBytes += c.Bytes.Carry
	if r.Window.Requested && w.HandoverStepBytes == 0 {
		w.HandoverStepBytes, w.HandoverEvidenceBytes = steps, evidence
	}
	return nil
}

func (m *workingSetMeasure) step(s harness.StepRecord) error {
	i, ok := m.stepOf[s.Step]
	if !ok {
		return fmt.Errorf("step %d completed at transition %d without a request that decided it", s.Step, s.Seq)
	}
	w := &m.report.Windows[i]
	w.Steps++
	retrieval, handover, accepted := workingSetResult(s)
	switch {
	case retrieval != nil:
		w.Reads++
		m.reads[s.Step] = true
		return m.sources(i, s.Step, retrieval)
	case handover != nil || workingSetHandover(s.Command):
		w.Handovers++
		switch {
		case accepted:
		case strings.HasPrefix(strings.TrimLeft(s.Command, harness.ActionSpace), strings.TrimSpace(harness.HandoverCarryPrefix)):
			w.FailedCarries++
		default:
			w.FailedHandovers++
		}
	case m.progress(s):
		w.Work++
		if w.Work == 1 {
			w.ReadsBeforeWork = w.Reads
			w.RequestsBeforeWork = w.Requests
		}
	}
	return nil
}

// workingSetHandover reports whether command proposes a handover, so that
// one that recorded no handover still counts as a failed proposal.
func workingSetHandover(command string) bool {
	word, _, _ := strings.Cut(strings.TrimLeft(command, harness.ActionSpace), " ")
	return strings.HasPrefix(word, strings.TrimSpace(harness.HandoverPrefix))
}

// sources charges the evidence of the retrieval of window i at step: a range
// that an earlier window read is reconstructed, one this window read is
// reread. The committed evidence of each source must match its reference.
func (m *workingSetMeasure) sources(i, step int, r *archive.Retrieval) error {
	if len(r.Evidence) != len(r.Sources) {
		return fmt.Errorf("retrieval at step %d holds %d evidence blocks for %d sources", step, len(r.Evidence), len(r.Sources))
	}
	w := &m.report.Windows[i]
	for k, s := range r.Sources {
		if len(r.Evidence[k]) != s.Length || fmt.Sprintf("%x", sha256.Sum256([]byte(r.Evidence[k]))) != s.SHA256 {
			return fmt.Errorf("retrieval at step %d: evidence %d does not match %s [%d,+%d) sha256 %s", step, k, s.Path, s.Offset, s.Length, s.SHA256)
		}
		start, end := s.Offset, s.Offset+int64(s.Length)
		w.EvidenceBytes += s.Length
		earlier, same := workingSetOverlap(m.ranges[s.Path], start, end, func(g workingSetRange) bool { return g.window < i }),
			workingSetOverlap(m.ranges[s.Path], start, end, func(g workingSetRange) bool { return g.window == i })
		w.ReconstructedBytes += earlier
		w.RereadBytes += same
		if earlier > 0 {
			w.RepeatedRanges++
		}
		m.ranges[s.Path] = append(m.ranges[s.Path], workingSetRange{window: i, start: start, end: end})
	}
	return nil
}

// workingSetOverlap counts the bytes of [start, end) that the selected
// ranges cover, each byte once.
func workingSetOverlap(ranges []workingSetRange, start, end int64, selected func(workingSetRange) bool) int {
	var cut [][2]int64
	for _, g := range ranges {
		if selected(g) && g.start < end && start < g.end {
			cut = append(cut, [2]int64{max(start, g.start), min(end, g.end)})
		}
	}
	slices.SortFunc(cut, func(a, b [2]int64) int { return cmp.Compare(a[0], b[0]) })
	covered, reach := int64(0), start
	for _, c := range cut {
		if c[1] > reach {
			covered += c[1] - max(c[0], reach)
			reach = c[1]
		}
	}
	return int(covered)
}

// usage adds the usage a response recorded and the cache tokens its raw body
// reports in the usage object of the Messages or Responses API.
func (m *workingSetMeasure) usage(r *harness.Response) {
	if r == nil {
		return
	}
	u := &m.report.Usage
	u.Responses++
	u.Input += r.Usage.InputTokens
	u.Output += r.Usage.OutputTokens
	var body struct {
		Usage struct {
			CacheRead     int64 `json:"cache_read_input_tokens"`
			CacheCreation int64 `json:"cache_creation_input_tokens"`
			Details       struct {
				Cached int64 `json:"cached_tokens"`
			} `json:"input_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal(r.Body, &body) == nil {
		u.CacheRead += body.Usage.CacheRead + body.Usage.Details.Cached
		u.CacheCreation += body.Usage.CacheCreation
	}
}

func workingSetProgress(s harness.StepRecord) bool {
	return strings.HasPrefix(s.Command, workingSetUnit)
}

// workingSetScenarios separate sparse history needs from one joint working
// set. A 128 KiB window has about 67 KiB of work capacity: the joint sets
// of fits, heavy, and tight take about a quarter, five eighths, and seven
// eighths of it as rendered, and oversized and near-full more than all of it
// as retrieval steps, near-full with four units of work to do; the 256 KiB
// window of the CLI's default holds the tight set with room to spare.
// Every scenario of the policy without a carry also runs as an arm whose
// agent carries what its next unit needs, under the shipped admission and
// under a margin of one maximal step; shifting changes its joint set
// halfway, and the mis-carry arm leaves one needed range behind.
var workingSetScenarios = withCarryArms([]workingSetScenario{
	{name: "sparse", ranges: 12, units: 24, perRange: 2, pad: 1024, total: 128 << 10, maxSteps: 200},
	{name: "joint-fits", ranges: 2, units: 24, pad: 1024, total: 128 << 10, maxSteps: 200},
	{name: "joint-heavy", ranges: 5, units: 24, pad: 1024, total: 128 << 10, maxSteps: 200},
	{name: "joint-tight", ranges: 7, units: 12, pad: 1024, total: 128 << 10, maxSteps: 200},
	{name: "joint-tight-256k", ranges: 7, units: 12, pad: 1024, total: 256 << 10, maxSteps: 200},
	{name: "joint-oversized", ranges: 8, units: 1, pad: 1024, total: 128 << 10, maxSteps: 30, oversized: true},
	{name: "joint-near-full", ranges: 8, units: 4, pad: 1024, total: 128 << 10, maxSteps: 60, oversized: true},
	{name: "joint-shifting", ranges: 10, units: 12, phases: 2, pad: 1024, total: 128 << 10, maxSteps: 200},
})

func withCarryArms(base []workingSetScenario) []workingSetScenario {
	out := slices.Clone(base)
	for _, sc := range base {
		carry, margin := sc, sc
		carry.name, carry.carry = sc.name+"/carry", true
		margin.name, margin.carry, margin.marginStep = sc.name+"/carry-margin", true, true
		out = append(out, carry, margin)
	}
	miss := base[3]
	miss.name, miss.carry, miss.miss = "joint-tight/mis-carry", true, true
	return append(out, miss)
}

// BenchmarkWorkingSet is the bounded offline evaluation of how the context
// policy serves sparse history needs and one joint working set, without a
// carry and with one under each admission arm. For each scenario it drives
// the scripted agent over SQLite, measures the journal, and reports the
// measurements as metrics and in its log. It fails only on what any policy
// must keep; how far a run departs from evidence that lives as long as its
// window is logged and counted, so that the arms can be compared. Its units of work are no-op commands:
// completing them shows the mechanism, not task success. Run it with
// -run '^$' -bench 'WorkingSet$' -benchtime 1x.
func BenchmarkWorkingSet(b *testing.B) {
	for _, sc := range workingSetScenarios {
		b.Run(sc.name, func(b *testing.B) {
			var r workingSetReport
			var snap harness.Snapshot
			for b.Loop() {
				var store *sqlite.Store
				store, snap = runWorkingSet(b, sc, false)
				var err error
				if r, err = measureWorkingSet(context.Background(), store, runID, workingSetProgress); err != nil {
					b.Fatal(err)
				}
			}
			checkWorkingSetInvariants(b, sc, snap, r)
			checkCarryArm(b, sc, r)
			out, _ := json.Marshal(r)
			b.Logf("phase=%s reason=%s report=%s", snap.State.Phase, snap.State.Reason, out)
			reportWorkingSet(b, r)
			if !sc.carry {
				deviations := windowLifetimeDeviations(sc, snap, r)
				b.Logf("window-lifetime deviations: %q", deviations)
				b.ReportMetric(float64(len(deviations)), "lifetime_deviations")
			}
		})
	}
}

// checkWorkingSetInvariants fails where a run breaks what any context policy
// must keep for the scripted agent: it halts within its step limit, every
// plain handover it proposes is accepted, only an arm that carries proposes
// a carry that may be refused, and a scenario whose working set fits a
// window returns with every unit done. The measurement has already checked
// each retrieval's evidence against its reference, and the agent works only
// when the request shows every needed range whole.
func checkWorkingSetInvariants(tb testing.TB, sc workingSetScenario, snap harness.Snapshot, r workingSetReport) {
	tb.Helper()
	if r.Total.Steps > sc.maxSteps || snap.State.Intent() != core.IntentHalt {
		tb.Errorf("%d steps ending %s; want a halt within %d", r.Total.Steps, snap.State.Phase, sc.maxSteps)
	}
	if r.Total.FailedHandovers != 0 || r.Total.FailedCarries != 0 && !sc.carry {
		tb.Errorf("%d plain handovers and %d carries failed", r.Total.FailedHandovers, r.Total.FailedCarries)
	}
	if !sc.oversized && (snap.State.Phase != core.PhaseReturned || r.Total.Work != sc.units) {
		tb.Errorf("%s with %d of %d units of work", snap.State.Phase, r.Total.Work, sc.units)
	}
	if r.Usage.Input != 0 || r.Usage.CacheRead != 0 {
		tb.Errorf("an offline agent recorded usage %+v", r.Usage)
	}
}

// checkCarryArm fails where an arm that carries one fixed joint set under
// the shipped admission does not keep it: every later window begins its
// work without reading, or, when the agent leaves one range behind, after
// reading exactly that range once.
func checkCarryArm(tb testing.TB, sc workingSetScenario, r workingSetReport) {
	tb.Helper()
	if !sc.carry || sc.marginStep || sc.perRange > 0 || sc.phases > 0 || sc.oversized {
		return
	}
	reads := 0
	if sc.miss {
		reads = 1
	}
	for _, w := range r.Windows[min(1, len(r.Windows)):] {
		if w.Work > 0 && (w.ReadsBeforeWork != reads || w.ReconstructedBytes != reads*archive.MaxBytes) {
			tb.Errorf("carrying window at step %d worked after %d reads reconstructing %d bytes, want %d reads", w.FirstStep, w.ReadsBeforeWork, w.ReconstructedBytes, reads)
		}
	}
}

// windowLifetimeDeviations lists where a run of sc without a carry departs
// from evidence that lives exactly as long as its window, an oracle that
// applies only to arms that do not carry. Under it, a later window that
// needs a joint working set reads all of it, or its phase's share, again
// before its first work; sparse needs reconstruct at most the range
// of the task that a handover interrupted; and a set larger than the work
// capacity never allows work, so the run uses every step it is allowed.
func windowLifetimeDeviations(sc workingSetScenario, snap harness.Snapshot, r workingSetReport) []string {
	var out []string
	if sc.oversized {
		if snap.State.Reason != core.StopStepsExhausted || r.Total.Steps != sc.maxSteps || r.Total.Work != 0 {
			out = append(out, fmt.Sprintf("oversized: %s after %d steps with %d units of work, want steps_exhausted after %d with none", snap.State.Reason, r.Total.Steps, r.Total.Work, sc.maxSteps))
		}
		for _, w := range r.Windows {
			if w.Handovers > 0 && w.Reads != sc.ranges {
				out = append(out, fmt.Sprintf("oversized window at step %d handed over after %d reads, want the whole set of %d", w.FirstStep, w.Reads, sc.ranges))
			}
		}
	}
	for _, w := range r.Windows[min(1, len(r.Windows)):] {
		if w.RereadBytes != 0 {
			out = append(out, fmt.Sprintf("window at step %d reread %d bytes it still showed", w.FirstStep, w.RereadBytes))
		}
		if sc.perRange > 0 {
			if w.ReconstructedBytes > archive.MaxBytes || w.RepeatedRanges > 1 {
				out = append(out, fmt.Sprintf("sparse window at step %d reconstructed %d bytes in %d ranges, want at most the interrupted range", w.FirstStep, w.ReconstructedBytes, w.RepeatedRanges))
			}
			continue
		}
		share := len(sc.needs(0))
		if w.Work > 0 && (w.ReadsBeforeWork != share || sc.phases == 0 && w.ReconstructedBytes != share*archive.MaxBytes) {
			out = append(out, fmt.Sprintf("joint window at step %d worked after %d reads reconstructing %d bytes, want the set of %d", w.FirstStep, w.ReadsBeforeWork, w.ReconstructedBytes, share))
		}
	}
	return out
}

func reportWorkingSet(b *testing.B, r workingSetReport) {
	b.ReportMetric(float64(len(r.Windows)), "windows")
	b.ReportMetric(float64(r.Total.Requests), "requests")
	b.ReportMetric(float64(r.Total.Reads), "reads")
	b.ReportMetric(float64(r.Total.Work), "work")
	b.ReportMetric(float64(r.Total.ReconstructedBytes), "reconstructed_bytes")
	b.ReportMetric(float64(r.Total.ResidentEvidenceBytes), "resident_evidence_bytes")
	b.ReportMetric(float64(r.Total.CarryBytes), "carry_bytes")
	b.ReportMetric(float64(r.Total.InputBytes), "input_bytes")
	b.ReportMetric(float64(r.Total.CommonPrefixBytes), "common_prefix_bytes")
	b.ReportMetric(float64(r.Total.FailedCarries), "failed_carries")
	if r.Total.Work > 0 {
		b.ReportMetric(float64(r.Total.Requests)/float64(r.Total.Work), "requests/unit")
	}
	b.ReportMetric(float64(r.Usage.CacheRead), "cache_read_tokens")
}

// BenchmarkWorkingSetRestart requires, for every scenario, that a run whose
// loop is rebuilt over a newly opened journal before every step commits the
// same transitions and facts as a steady one: the complete matrix behind the
// default suite's restart checks, opt-in because it takes minutes.
func BenchmarkWorkingSetRestart(b *testing.B) {
	for _, sc := range workingSetScenarios {
		b.Run(sc.name, func(b *testing.B) {
			for b.Loop() {
				steady, _ := runWorkingSet(b, sc, false)
				restarted, _ := runWorkingSet(b, sc, true)
				if want, got := workingSetTrace(b, steady), workingSetTrace(b, restarted); !slices.Equal(want, got) {
					b.Fatalf("restarted before every step, the run committed %d transitions unlike the %d of the steady run", len(got), len(want))
				}
			}
		})
	}
}

// BenchmarkWorkingSetOfAJournal measures the run that -working-set-journal
// and -working-set-run name, opening the journal read-only. It counts as work
// every executor step that exited 0 and is not a marked shell discovery,
// which is weaker evidence of progress than a scenario's units; a marked
// discovery is still an external effect, left out of work only because it
// searches. A journal whose requests record no context window, whose
// retrievals hold no frozen evidence, or whose steps complete by an event
// this measurement does not know is refused rather than undercounted.
func BenchmarkWorkingSetOfAJournal(b *testing.B) {
	if *workingSetJournal == "" {
		b.Skip("-working-set-journal names no journal")
	}
	ctx := context.Background()
	store, err := sqlite.OpenReadOnly(ctx, *workingSetJournal)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	id := *workingSetRun
	if id == "" {
		run, err := store.LastRun(ctx)
		if err != nil {
			b.Fatal(err)
		}
		id = run.ID
	}
	var r workingSetReport
	for b.Loop() {
		if r, err = measureWorkingSet(ctx, store, id, func(s harness.StepRecord) bool {
			o := s.Observation
			return s.Outcome == nil && o.Retrieval == nil && o.Handover == nil && o.Execution == core.ExecutionExited && o.ExitCode != nil && *o.ExitCode == 0 &&
				!strings.HasPrefix(s.Command, harness.DiscoveryMarker)
		}); err != nil {
			b.Fatal(err)
		}
	}
	out, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("run=%s report=%s", id, out)
	reportWorkingSet(b, r)
}
