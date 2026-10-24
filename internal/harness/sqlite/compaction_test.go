package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func compactPlan(t *testing.T, s *Store, snap harness.Snapshot) harness.CompactPlan {
	t.Helper()
	run, err := s.Run(context.Background(), snap.State.RunID)
	if err != nil {
		t.Fatal(err)
	}
	entries, _, err := s.ContextEntries(context.Background(), run.ContextOwner(), 0, 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	p, err := harness.NewCompactPlan(harness.CompactPlan{RunID: run.ID, RunSeq: snap.Seq, Owner: run.ContextOwner(), Through: 1, SourceHash: harness.ContextSourceHash(entries), Request: harness.Request{Body: []byte("frozen summary request")}, MaxAttempts: 3, MaxSummaryBytes: 100, ReplacedBytes: 1000, BeforeBytes: 1000, TokenReserve: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.PlanCompaction(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCompactionValidatesFrozenParentAndPersistsCacheEvidence(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s := open(t, path)
	d := newDriver(t, s, createRun(t, s, "run", limits))
	entries, _, err := s.ContextEntries(ctx, "run:run", 0, 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	parent := harness.Request{Body: []byte("committed business prefix"), Context: &harness.ContextCapture{
		SourceEnd: 1, Entries: []harness.ContextEntryRef{{Index: 1, RunID: "run", Kind: "input", BlockEnd: 1}},
	}}
	d.do(core.RequestFrozen{}, harness.Facts{Request: &parent})
	d.send()
	body := execBody("inspect")
	d.save(body, harness.Usage{})
	d.load(body)
	d.call(harness.Runner.Bind)
	d.call(harness.Runner.Start)
	d.observe(core.ExecutionExited, 0)
	p := harness.CompactPlan{RunID: "run", RunSeq: d.snap.Seq, Owner: "run:run", Through: 1,
		SourceHash: harness.ContextSourceHash(entries), Request: harness.Request{Body: []byte("derived summary")},
		MaxAttempts: 3, MaxSummaryBytes: 100, BeforeBytes: 1000, ReplacedBytes: 1000, TokenReserve: 1000,
		Prefix: &harness.FrozenPrefix{RequestHash: harness.RequestHash(parent), Blocks: 1}}
	for _, prefix := range []harness.FrozenPrefix{{RequestHash: strings.Repeat("0", 64), Blocks: 1}, {RequestHash: harness.RequestHash(parent), Blocks: 2}} {
		invalid := p
		invalid.Prefix = &prefix
		invalid, err = harness.NewCompactPlan(invalid)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.PlanCompaction(ctx, invalid); err == nil {
			t.Fatal("accepted changed parent or wrong prefix extent")
		}
	}
	if _, err := s.db.Exec(`UPDATE transitions SET facts=json_set(facts,'$.Request.Context.Entries[0].RunID','foreign') WHERE kind='request_frozen'`); err != nil {
		t.Fatal(err)
	}
	p, err = harness.NewCompactPlan(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PlanCompaction(ctx, p); err == nil {
		t.Fatal("accepted parent provenance belonging to another run")
	}
	if _, err := s.db.Exec(`UPDATE transitions SET facts=json_set(facts,'$.Request.Context.Entries[0].RunID','run') WHERE kind='request_frozen'`); err != nil {
		t.Fatal(err)
	}
	if err := s.PlanCompaction(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := s.StartCompactionAttempt(ctx, p.ID, 1); err != nil {
		t.Fatal(err)
	}
	cache := harness.CacheUsage{ReadTokens: 20, WriteTokens: 2}
	result := harness.CompactAttempt{Number: 1, Done: true, Response: &harness.Response{Body: []byte("summary"), Complete: true, UsageKnown: true, Usage: harness.Usage{InputTokens: 30, OutputTokens: 5, Cache: &cache}}}
	if err := s.SaveCompactionAttempt(ctx, p.ID, result); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = open(t, path)
	got, err := s.PendingCompaction(ctx, "run", p.RunSeq)
	if err != nil || got.Plan.Prefix == nil || *got.Plan.Prefix != *p.Prefix || got.Attempts[0].Response.Usage.Cache == nil || *got.Attempts[0].Response.Usage.Cache != cache {
		t.Fatalf("lost frozen-prefix or cache evidence: %+v %v", got, err)
	}
}

func saveSummary(t *testing.T, s *Store, p harness.CompactPlan) {
	t.Helper()
	ctx := context.Background()
	if err := s.StartCompactionAttempt(ctx, p.ID, 1); err != nil {
		t.Fatal(err)
	}
	r := harness.CompactAttempt{Number: 1, Done: true, Response: &harness.Response{Complete: true, UsageKnown: true, Body: []byte("summary"), Usage: harness.Usage{InputTokens: 30, OutputTokens: 5}}}
	if err := s.SaveCompactionAttempt(ctx, p.ID, r); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveCompactionAttempt(ctx, p.ID, r); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishCompaction(ctx, p.ID, "Continue with the original constraints."); err != nil {
		t.Fatal(err)
	}
}

func TestCompactionCandidateSurvivesReopenAndAdoptsWithRequest(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s := open(t, path)
	snap := createRun(t, s, "run", limits)
	p := compactPlan(t, s, snap)
	saveSummary(t, s, p)
	if h, err := s.ActiveCheckpoint(ctx, p.Owner); err != nil || h.ID != "" {
		t.Fatalf("candidate became active: %+v %v", h, err)
	}
	s.Close()
	s = open(t, path)
	c, err := s.PendingCompaction(ctx, "run", 0)
	if err != nil || c.Summary == "" || c.Accepted {
		t.Fatalf("candidate: %+v %v", c, err)
	}
	d := newDriver(t, s, snap)
	d.do(core.RequestFrozen{}, harness.Facts{Request: &harness.Request{Body: []byte("next frozen work request"), CheckpointID: p.ID, Window: &harness.Window{FirstStep: 1, Through: 1, Summary: c.Summary}}})
	h, err := s.ActiveCheckpoint(ctx, p.Owner)
	if err != nil || h.ID != p.ID || h.Through != 1 {
		t.Fatalf("head: %+v %v", h, err)
	}
	if err := s.Commit(ctx, d.commits[0]); err != nil {
		t.Fatal(err)
	}
	totals := totalsOf(t, s, "run")
	if totals.Usage != (harness.Usage{InputTokens: 30, OutputTokens: 5}) || totals.ReservedTokens != 0 || totals.Steps != 0 || totals.SummaryUsage != totals.Usage {
		t.Fatalf("totals: %+v", totals)
	}
}

func TestCompactionCancellationFencesCandidateAdoption(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	snap := createRun(t, s, "run", limits)
	p := compactPlan(t, s, snap)
	saveSummary(t, s, p)
	d := newDriver(t, s, snap)
	d.do(core.StopRequested{Reason: core.StopCancelled}, harness.Facts{})
	if err := s.StartCompactionAttempt(ctx, p.ID, 2); !errors.Is(err, harness.ErrStaleSnapshot) {
		t.Fatalf("late attempt: %v", err)
	}
	if h, err := s.ActiveCheckpoint(ctx, p.Owner); err != nil || h.ID != "" {
		t.Fatalf("head after stop: %+v %v", h, err)
	}
}

func TestCompactionLostAttemptKeepsUnknownCostAndFrozenBytes(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s := open(t, path)
	p := compactPlan(t, s, createRun(t, s, "run", limits))
	if err := s.StartCompactionAttempt(ctx, p.ID, 1); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = open(t, path)
	if err := s.StartCompactionAttempt(ctx, p.ID, 2); err != nil {
		t.Fatal(err)
	}
	c, err := s.PendingCompaction(ctx, "run", 0)
	if err != nil || !c.Attempts[0].UnknownUsage || !c.Attempts[0].Done || string(c.Plan.Request.Body) != "frozen summary request" {
		t.Fatalf("recovered: %+v %v", c, err)
	}
	result := harness.CompactAttempt{Number: 2, Done: true, Response: &harness.Response{Complete: true, UsageKnown: true, Body: []byte("summary"), Usage: harness.Usage{InputTokens: 30, OutputTokens: 5}}}
	if err := s.SaveCompactionAttempt(ctx, p.ID, result); err != nil {
		t.Fatal(err)
	}
	if got := totalsOf(t, s, "run"); got.ReservedTokens != 1000 || got.Usage.InputTokens != 30 {
		t.Fatalf("unknown usage lost: %+v", got)
	}
}

func TestContextSourcesIncludeTerminalStepsAndRemainIsolated(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "one", limits))
	d.step("printf committed", harness.Usage{}, 0)
	d.decide([]byte(`{"kind":"return","payload":"done"}`), harness.Usage{})
	createRun(t, s, "two", limits)
	entries, more, err := s.ContextEntries(ctx, "run:one", 0, 10, 1<<20)
	if err != nil || more || len(entries) != 3 || entries[0].Kind != "input" || entries[1].Step.Command != "printf committed" || entries[2].Phase != core.PhaseReturned {
		t.Fatalf("sources: %+v %t %v", entries, more, err)
	}
	page, more, err := s.ContextEntries(ctx, "run:one", 1, 1, 1<<20)
	if err != nil || !more || len(page) != 1 || page[0].Index != 2 {
		t.Fatalf("page: %+v %t %v", page, more, err)
	}
}

func TestCompactionRequestFailureRollsBackAdoption(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	snap := createRun(t, s, "run", limits)
	p := compactPlan(t, s, snap)
	saveSummary(t, s, p)
	c, _ := s.PendingCompaction(ctx, "run", 0)
	if _, err := s.db.Exec(`CREATE TRIGGER reject_request BEFORE INSERT ON transitions BEGIN SELECT RAISE(ABORT,'injected request failure'); END`); err != nil {
		t.Fatal(err)
	}
	runner, err := harness.NewRunner(s, executor{})
	if err != nil {
		t.Fatal(err)
	}
	req := harness.Request{Body: []byte("work"), CheckpointID: p.ID, Window: &harness.Window{FirstStep: 1, Through: 1, Summary: c.Summary}}
	if _, err := runner.Commit(ctx, snap, core.RequestFrozen{}, harness.Facts{Request: &req}); err == nil {
		t.Fatal("injected failure succeeded")
	}
	if head, err := s.ActiveCheckpoint(ctx, p.Owner); err != nil || head.ID != "" {
		t.Fatalf("head escaped rollback %+v %v", head, err)
	}
	c, _ = s.PendingCompaction(ctx, "run", 0)
	if c.Accepted {
		t.Fatal("candidate adoption escaped rollback")
	}
	if _, err := s.db.Exec(`DROP TRIGGER reject_request`); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Commit(ctx, snap, core.RequestFrozen{}, harness.Facts{Request: &req}); err != nil {
		t.Fatal(err)
	}
}

func TestCompactionRejectsChangedSourceHashAndMismatchedRequest(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	snap := createRun(t, s, "run", limits)
	p := compactPlan(t, s, snap)
	invalid := p
	invalid.SourceHash = "wrong"
	invalid, _ = harness.NewCompactPlan(invalid)
	if err := s.PlanCompaction(ctx, invalid); err == nil {
		t.Fatal("accepted wrong source hash")
	}
	saveSummary(t, s, p)
	runner, _ := harness.NewRunner(s, executor{})
	if _, err := runner.Commit(ctx, snap, core.RequestFrozen{}, harness.Facts{Request: &harness.Request{Body: []byte("work"), CheckpointID: p.ID, Window: &harness.Window{FirstStep: 1, Through: 1, Summary: "different"}}}); err == nil {
		t.Fatal("adopted a mismatched request")
	}
	if head, _ := s.ActiveCheckpoint(ctx, p.Owner); head.ID != "" {
		t.Fatal("mismatch moved coverage")
	}
}

func TestCompactionDoesNotReleaseReservationForUnreportedUsage(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	p := compactPlan(t, s, createRun(t, s, "run", limits))
	if err := s.StartCompactionAttempt(ctx, p.ID, 1); err != nil {
		t.Fatal(err)
	}
	attempt := harness.CompactAttempt{Number: 1, Done: true, Response: &harness.Response{Complete: true, Body: []byte("summary")}}
	if err := s.SaveCompactionAttempt(ctx, p.ID, attempt); err == nil {
		t.Fatal("missing usage treated as zero")
	}
	attempt.UnknownUsage = true
	if err := s.SaveCompactionAttempt(ctx, p.ID, attempt); err != nil {
		t.Fatal(err)
	}
	if got := totalsOf(t, s, "run"); got.ReservedTokens != p.TokenReserve || got.Usage.InputTokens != 0 {
		t.Fatalf("totals %+v", got)
	}
}

func TestCompactionRejectsCorruptPlanAndUnbackedSuccess(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	p := compactPlan(t, s, createRun(t, s, "run", limits))
	if err := s.StartCompactionAttempt(ctx, p.ID, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveCompactionAttempt(ctx, p.ID, harness.CompactAttempt{Number: 1, Done: true, UnknownUsage: true}); err == nil {
		t.Fatal("accepted success without response")
	}
	if got := totalsOf(t, s, "run"); got.ReservedTokens != p.TokenReserve {
		t.Fatal("invalid response changed reservation")
	}
	if _, err := s.db.Exec(`UPDATE compactions SET data=json_set(data,'$.Plan.SourceHash','corrupt') WHERE id=?`, p.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PendingCompaction(ctx, "run", 0); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("accepted corrupt recovery plan: %v", err)
	}
}
