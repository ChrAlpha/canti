package harness_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
	"github.com/ChrAlpha/canti/internal/harness"
)

// finishWithArtifact finishes a started job at its first inspection and
// writes its output where the archive reads artifacts.
func finishWithArtifact(x *fakeExec, artifacts string) func(string) (harness.Job, bool, error) {
	return func(effectID string) (harness.Job, bool, error) {
		j := x.jobs[effectID]
		if j == nil {
			return harness.Job{}, false, nil
		}
		if j.Status == harness.JobRunning {
			x.finish(j, harness.JobSucceeded)
			if err := os.WriteFile(filepath.Join(artifacts, j.Output.Path), []byte(j.Preview), 0o600); err != nil {
				return harness.Job{}, false, err
			}
		}
		return j.Job, true, nil
	}
}

func TestDirectorySearchCursorAndEvidenceReachFrozenRequest(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply(`canti:retrieve {"operation":"search","path":"/canti/imports","query":"needle","limit":6,"reason":"locate original evidence"}`), returnReply("done"))
	root := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("needle"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	f.cfg.Reader = &archive.Reader{Imports: root}
	if _, err := f.loop().Run(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	steps, err := f.store.Recent(context.Background(), runID, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	r := steps[0].Observation.Retrieval
	if r == nil || !r.More || r.NextCursor == "" {
		t.Fatalf("missing cursor %#v", r)
	}
	quoted, _ := json.Marshal(r.NextCursor)
	body := string(f.model.sent[1])
	if !strings.Contains(body, `"next_cursor":`+string(quoted)) || !strings.Contains(body, "needle") || !strings.Contains(body, "Source /canti/imports/a.txt") {
		t.Fatalf("cursor/evidence absent in actual request: %s", body)
	}
}

func TestRetrievalSearchContinuationSurvivesPromptPreviewLimit(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{},
		execReply(`canti:retrieve {"operation":"search","path":"/canti/imports/evidence","reason":"find earlier facts","query":"x","limit":8192}`),
		returnReply("done"))
	imported := t.TempDir()
	if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte(strings.Repeat("x", 10000)), 0600); err != nil {
		t.Fatal(err)
	}
	f.cfg.Reader = &archive.Reader{Imports: imported}
	if _, err := f.loop().Run(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	if len(f.model.sent) != 2 {
		t.Fatalf("sent %d requests", len(f.model.sent))
	}
	request := string(f.model.sent[1])
	if !strings.Contains(request, `"next_offset":8192`) || !strings.Contains(request, `"more":true`) {
		t.Fatal("search continuation metadata was truncated before reaching the model")
	}
}

func TestRetrievedFencedSourceReachesFrozenRequestWithoutLostBytes(t *testing.T) {
	imported := t.TempDir()
	const evidence = "Original evidence:\n```go\nvalue := 42\n```\nFINAL_SOURCE_BYTE_Z"
	if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte(evidence), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, limits, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"read exact original evidence"}`),
		returnReply("done"))
	f.cfg.Reader = &archive.Reader{Imports: imported}
	if _, err := f.loop().Run(f.bounded(), runID); err != nil {
		t.Fatal(err)
	}
	req, err := f.store.Request(f.bounded(), runID, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(req.Body), evidence) {
		t.Fatal("already-bounded original evidence lost source bytes while adding presentation fences")
	}
}

func TestSuccessfulRetrievalDoesNotReportMissingIncompleteOutput(t *testing.T) {
	imported := t.TempDir()
	const evidence = "complete original source evidence"
	if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte(evidence), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, limits, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"verify earlier evidence"}`),
		returnReply("done"))
	f.cfg.Reader = &archive.Reader{Imports: imported}
	if _, err := f.loop().Run(f.bounded(), runID); err != nil {
		t.Fatal(err)
	}
	if len(f.model.sent) != 2 {
		t.Fatalf("sent %d requests", len(f.model.sent))
	}
	request := string(f.model.sent[1])
	if !strings.Contains(request, evidence) || !strings.Contains(request, `"more":false`) {
		t.Fatal("successful retrieval result did not reach the model")
	}
	if strings.Contains(request, "Output: 0 bytes, incomplete, unavailable") {
		t.Fatal("successful retrieval was presented as missing incomplete command output")
	}
}

type missingLatestWindowRequest struct {
	harness.Journal
}

func TestDirectorySearchGeneratedCursorCanAlwaysBeResumed(t *testing.T) {
	for _, tc := range []struct{ label, name string }{
		{"html", strings.Repeat("<", 255)},
		{"control", strings.Repeat("\x01", 255)},
		{"unicode", strings.Repeat("中", 85)},
	} {
		t.Run(tc.label, func(t *testing.T) {
			testDirectorySearchGeneratedCursor(t, tc.name)
		})
	}
}

func testDirectorySearchGeneratedCursor(t *testing.T, name string) {
	imported := t.TempDir()
	if err := os.WriteFile(filepath.Join(imported, name), []byte("needle needle"), 0600); err != nil {
		t.Fatal(err)
	}
	r := archive.Reader{Imports: imported}
	q := archive.RetrievalQuery{Operation: "search", Path: "/canti/imports", Query: "needle", Reason: "locate evidence", Limit: 6}
	first, err := r.Retrieve(context.Background(), q)
	if err != nil || first.Error != "" || !first.More || first.NextCursor == "" {
		t.Fatalf("first page: %+v %v", first, err)
	}
	q.Cursor = first.NextCursor
	second, err := r.Retrieve(context.Background(), q)
	if err != nil || second.Error != "" || second.More || len(second.Sources) != 1 {
		t.Fatalf("generated %d-byte cursor cannot resume: error=%q more=%v sources=%d err=%v", len(q.Cursor), second.Error, second.More, len(second.Sources), err)
	}
}

func (j missingLatestWindowRequest) LatestRequest(context.Context, string) (harness.Request, error) {
	return harness.Request{}, harness.ErrNotFound
}

func TestWindowRecoveryRejectsMissingDurableRequestAfterProgress(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{},
		execReply("echo old-window-marker"),
		execReply("echo next operation"))
	f.cfg.CompactSteps = 1
	ctx := f.bounded()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Step == 3 {
			break
		}
	}
	before := len(f.model.prompts)
	s, err := f.loopOn(missingLatestWindowRequest{f.open()}).Step(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if s.State.Phase != core.PhaseStopped || s.State.Reason != core.StopUnrecoverable || len(f.model.prompts) != before {
		t.Fatalf("missing durable window silently rebuilt context: state=%+v new_prompts=%d", s.State, len(f.model.prompts)-before)
	}
}

func TestWindowProtocolRepairRejectsMissingInitialFrozenRequest(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, reply{body: "invalid decision", usage: usage})
	f.cfg.CompactSteps = 1
	ctx := f.bounded()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Intent() == core.IntentPrepareRequest && s.State.RequestRev > 0 {
			break
		}
	}
	before := len(f.model.prompts)
	s, err := f.loopOn(missingLatestWindowRequest{f.open()}).Step(ctx, runID)
	if err != nil || s.State.Phase != core.PhaseStopped || s.State.Reason != core.StopUnrecoverable || len(f.model.prompts) != before {
		t.Fatalf("protocol repair recreated missing initial request: state=%+v new_prompts=%d err=%v", s.State, len(f.model.prompts)-before, err)
	}
}

func TestIndependentCompactionKeepsTailAndDoesNotConsumeWorkSteps(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 5, MaxAttempts: 3}, harness.Budget{}, execReply("echo first "+strings.Repeat("x", 512)), execReply("echo second "+strings.Repeat("y", 512)), execReply("echo third"), returnReply("done"))
	f.cfg.CompactSteps = 2
	f.cfg.RetainBytes = 1
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
	if len(f.model.summarySent) != 2 || len(f.model.prompts) != 4 {
		t.Fatalf("summary calls %d, work calls %d", len(f.model.summarySent), len(f.model.prompts))
	}
	first := capturedRequest(t, f, 3, 1)
	if first.CheckpointID == "" || first.Window.FirstStep != 2 || first.Window.Through != 2 || !strings.Contains(string(first.Body), "### Step 2\n") || strings.Contains(string(first.Body), "### Step 1\n") {
		t.Fatalf("first checkpoint: %+v", first.Window)
	}
	second := capturedRequest(t, f, 4, 1)
	if second.CheckpointID == first.CheckpointID || second.Window.FirstStep != 3 || second.Window.Through != 3 {
		t.Fatalf("second checkpoint: %+v", second.Window)
	}
	if !strings.Contains(string(f.model.summarySent[1]), "echo second") || strings.Contains(string(f.model.summarySent[1]), "echo third") {
		t.Fatal("next summary skipped the previously retained step or consumed the protected step")
	}
	totals, err := f.store.Totals(f.bounded(), runID)
	if err != nil || totals.Steps != 3 || totals.SummaryUsage.InputTokens != 2*usage.InputTokens {
		t.Fatalf("totals: %+v %v", totals, err)
	}
	for _, job := range f.exec.jobs {
		if job.runs != 1 {
			t.Fatalf("effect repeated: %+v", job)
		}
	}
	records, err := f.store.Compactions(f.bounded(), runID, 0, 10)
	if err != nil || len(records) != 2 {
		t.Fatalf("operations: %v", err)
	}
	for i, record := range records {
		parent := capturedRequest(t, f, i+2, 1)
		plan := record.Operation.Plan
		if plan.Prefix == nil || plan.Prefix.RequestHash != harness.RequestHash(parent) {
			t.Fatal("compaction did not derive from its committed request")
		}
		var boundary int
		for _, e := range parent.Context.Entries {
			if e.Index == plan.Through {
				boundary = e.BlockEnd
			}
		}
		if boundary != plan.Prefix.Blocks {
			t.Fatal("prefix includes sources outside the replaced range")
		}
	}
}

func TestIndependentCompactionRetriesFrozenSummaryAndPreservesWorkIdentity(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("echo one "+strings.Repeat("x", 512)), execReply("echo two "+strings.Repeat("y", 512)), returnReply("done"))
	f.cfg.CompactSteps = 2
	f.cfg.RetainBytes = 1
	f.model.summaries = []reply{{body: "partial", err: errors.New("transport interrupted"), usage: usage}, {body: "Keep the original goal and constraints; inspect original evidence.", usage: usage}}
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
	if len(f.model.summarySent) != 2 || !bytes.Equal(f.model.summarySent[0], f.model.summarySent[1]) {
		t.Fatal("summary retry changed frozen bytes")
	}
	if len(f.exec.jobs) != 2 {
		t.Fatal("compaction consumed or replayed an effect")
	}
}

func TestIndependentCompactionFailureRetainsOriginalContext(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("echo one "+strings.Repeat("x", 512)), execReply("echo two "+strings.Repeat("y", 512)), returnReply("must not be requested"))
	f.cfg.CompactSteps = 2
	f.cfg.RetainBytes = 1
	f.model.summaries = []reply{{body: ""}, {body: ""}, {body: ""}}
	wantState(t, f.run(f.fresh()), core.PhaseStopped, core.StopUnrecoverable)
	head, err := f.store.ActiveCheckpoint(f.bounded(), "run:"+runID)
	if err != nil || head.ID != "" || len(f.model.prompts) != 2 {
		t.Fatalf("failed compaction mutated context: %+v %v", head, err)
	}
}

func TestIndependentCompactionSavedResponseResumesWithoutResend(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("first "+strings.Repeat("x", 512)), execReply("second "+strings.Repeat("y", 512)), returnReply("done"))
	f.cfg.CompactSteps = 2
	f.cfg.RetainBytes = 1
	ctx := f.bounded()
	var snap harness.Snapshot
	for n := 0; n < 100; n++ {
		var err error
		snap, err = f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		c, err := f.store.PendingCompaction(ctx, runID, snap.Seq)
		if err == nil && len(c.Attempts) == 1 && c.Attempts[0].Done {
			break
		}
	}
	if len(f.model.summarySent) != 1 {
		t.Fatal("did not stop after saved summary response")
	}
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
	if len(f.model.summarySent) != 1 {
		t.Fatal("saved summary was sent again after restart")
	}
}

func TestIndependentCompactionReservesBudgetBeforeSending(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{Tokens: 500}, execReply("first "+strings.Repeat("x", 512)), execReply("second "+strings.Repeat("y", 512)), returnReply("not sent"))
	f.cfg.CompactSteps = 2
	f.cfg.RetainBytes = 1
	wantState(t, f.run(f.fresh()), core.PhaseStopped, core.StopBudgetExhausted)
	if len(f.model.summarySent) != 0 || len(f.model.prompts) != 2 {
		t.Fatal("summary dispatched without budget reservation")
	}
	if head, err := f.store.ActiveCheckpoint(f.bounded(), "run:"+runID); err != nil || head.ID != "" {
		t.Fatal("budget failure adopted context")
	}
}

func TestIndependentCompactionCancellationRetainsAttemptAndSource(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("first "+strings.Repeat("x", 512)), execReply("second "+strings.Repeat("y", 512)), returnReply("not sent"))
	f.cfg.CompactSteps = 2
	f.cfg.RetainBytes = 1
	ctx, cancel := context.WithCancel(f.bounded())
	defer cancel()
	f.model.summaries = []reply{{body: "late result", late: true, hook: cancel}}
	snap, err := f.loop().Run(ctx, runID)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %+v %v", snap, err)
	}
	c, err := f.store.PendingCompaction(f.bounded(), runID, snap.Seq)
	if err != nil || len(c.Attempts) != 1 || c.Attempts[0].Done {
		t.Fatalf("late response committed: %+v %v", c, err)
	}
	if _, err := f.fresh().Stop(f.bounded(), runID, core.StopCancelled); err != nil {
		t.Fatal(err)
	}
	if head, err := f.store.ActiveCheckpoint(f.bounded(), "run:"+runID); err != nil || head.ID != "" {
		t.Fatal("cancellation adopted context")
	}
	totals, _ := f.store.Totals(f.bounded(), runID)
	if totals.ReservedTokens != c.Plan.TokenReserve {
		t.Fatal("cancelled usage assumed zero")
	}
}

func TestIndependentCompactionConsumesBacklogInDurablePasses(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, returnReply("unrelated standalone"))
	f.run(f.loop())
	ctx := f.bounded()
	runner, err := harness.NewRunner(f.store, f.exec)
	if err != nil {
		t.Fatal(err)
	}
	// Seed committed session facts without preparing historical working requests;
	// the next run starts with a smaller context allowance than this raw backlog.
	for i := 1; i <= 40; i++ {
		id := fmt.Sprintf("prior-%02d", i)
		state, _ := core.NewState(id, limits)
		run := harness.Run{ID: id, SessionID: "long", InputID: id, Goal: fmt.Sprintf("Original obligation %d", i), Constraints: "Keep all continuing obligations.", Created: created}
		if _, err := f.store.Submit(ctx, run, state); err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(core.Decision{Kind: core.Return, Payload: strings.Repeat(fmt.Sprintf("Evidence for run %02d. ", i), 400)})
		snap := harness.Snapshot{State: state}
		for _, event := range []core.Event{core.RequestFrozen{}, core.AttemptStarted{Step: 1, Attempt: 1}, core.ResponseSaved{Step: 1, Attempt: 1}, core.ResponseLoaded{Step: 1, Attempt: 1, Body: body}} {
			var facts harness.Facts
			switch event.(type) {
			case core.RequestFrozen:
				facts.Request = &harness.Request{Body: []byte("seed"), Window: &harness.Window{FirstStep: 1}}
			case core.ResponseSaved:
				facts.Response = &harness.Response{Body: body, Complete: true}
			}
			snap, err = runner.Commit(ctx, snap, event, facts)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	id := "continuation"
	state, _ := core.NewState(id, limits)
	run := harness.Run{ID: id, SessionID: "long", InputID: id, Goal: "Continue.", Constraints: "Current correction: read only.", Created: created}
	if _, err := f.store.Submit(ctx, run, state); err != nil {
		t.Fatal(err)
	}
	f.model.replies = []reply{returnReply("continued")}
	for n := 0; n < 100; n++ {
		snap, err := f.fresh().Step(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		head, err := f.store.ActiveCheckpoint(ctx, "session:long")
		if err != nil {
			t.Fatal(err)
		}
		if snap.State.RequestRev == 0 && head.ID != "" {
			t.Fatal("intermediate candidate became active")
		}
		if snap.State.Phase == core.PhaseReturned {
			break
		}
		if n == 99 {
			t.Fatal("backlog did not converge")
		}
	}
	records, err := f.store.Compactions(ctx, id, 0, 100)
	if err != nil || len(records) < 2 {
		t.Fatalf("passes %d %v", len(records), err)
	}
	after := int64(0)
	parent := ""
	for i, record := range records {
		c := record.Operation
		if c.Plan.After != after || c.Plan.Parent != parent || c.Plan.Base != "" || c.Accepted != (i == len(records)-1) {
			t.Fatalf("broken staged lineage: %+v", c.Plan)
		}
		after, parent = c.Plan.Through, c.Plan.ID
	}
	req, err := f.store.LatestRequest(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"Current correction: read only.", "Continue.", "Evidence for run 40."} {
		if !strings.Contains(string(req.Body), text) {
			t.Fatalf("lost %q", text)
		}
	}
	if len(f.exec.jobs) != 0 {
		t.Fatal("maintenance created effects")
	}
}
