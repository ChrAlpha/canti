package harness_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
	"github.com/ChrAlpha/canti/runtime/internal/project"
)

func TestALongerActionNameIsNotAHandover(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
		execReply("true"), execReply("canti:handovers Next: verify."), execReply("canti:handoverNext: verify."))
	s, err := f.loop().Run(f.bounded(), runID)
	if err != nil || s.State.Reason != core.StopUnrecoverable {
		t.Fatalf("run=%+v err=%v", s.State, err)
	}
	steps, err := f.store.Recent(f.bounded(), runID, 0, 2)
	if err != nil || len(steps) != 2 {
		t.Fatalf("steps=%+v err=%v", steps, err)
	}
	for _, step := range steps {
		if o := step.Outcome; o == nil || o.Status != harness.OutcomeFailed {
			t.Fatalf("step %d outcome %+v; want refused work", step.Step, o)
		}
	}
}

func TestDiscoverySkipsAHandoverAfterAnyWhitespace(t *testing.T) {
	for _, separator := range harness.ActionSpace {
		t.Run(fmt.Sprintf("%q", separator), func(t *testing.T) { discoverPastHandover(t, string(separator)) })
	}
}

func TestDiscoverySkipsABareHandoverName(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
		execReply("echo canti:handover"), execReply("canti:handover"), execReply("canti:handover Continue."),
		execReply(`canti:retrieve {"operation":"discover","path":"/canti/runs/run-1/steps","query":"canti:handover","reason":"find the step"}`),
		returnReply("done"))
	artifacts := t.TempDir()
	f.cfg.Reader = &archive.Reader{History: f.cfg.History, Artifacts: artifacts}
	f.exec.inspect = finishWithArtifact(f.exec, artifacts)
	if s, err := f.loop().Run(f.bounded(), runID); err != nil || s.State.Phase != core.PhaseReturned {
		t.Fatalf("run=%+v err=%v", s.State, err)
	}
	steps, err := f.store.Recent(f.bounded(), runID, 0, 1)
	if err != nil || len(steps) != 1 || steps[0].Outcome == nil || steps[0].Outcome.Retrieval == nil {
		t.Fatalf("steps=%+v err=%v", steps, err)
	}
	var found []string
	for _, source := range steps[0].Outcome.Retrieval.Sources {
		found = append(found, source.Path)
	}
	if !slices.ContainsFunc(found, func(p string) bool { return strings.Contains(p, "/00000001/") }) ||
		slices.ContainsFunc(found, func(p string) bool { return strings.Contains(p, "/00000002/") || strings.Contains(p, "/00000003/") }) {
		t.Fatalf("discovery found %v; want step 1 and neither handover step", found)
	}
}

// finishWithArtifact finishes a started job at its first inspection and
// writes its output where the archive reads artifacts.
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
	r := steps[0].Outcome.Retrieval
	if r == nil || !r.More || r.NextCursor == "" {
		t.Fatalf("missing cursor %#v", r)
	}
	quoted, _ := json.Marshal(r.NextCursor)
	body := string(f.model.sent[1])
	if !strings.Contains(body, `"next_cursor":`+string(quoted)) || !strings.Contains(body, "needle") || !strings.Contains(body, "Path /canti/imports/a.txt") {
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

func latestSnapshot(t *testing.T, f *fixture) harness.Snapshot {
	t.Helper()
	snap, err := f.store.Latest(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestConfirmedOverflowResetsSameStepAndNeverExecutesPartialDecision(t *testing.T) {
	imported := t.TempDir()
	if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte("TRANSIENT_BEFORE_RESET"), 0o600); err != nil {
		t.Fatal(err)
	}
	signal := &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded", RequestID: "req_overflow"}
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 4}, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"inspect"}`),
		reply{body: `{"kind":"exec","payload":"MUST_NEVER_EXECUTE"}`, err: signal, usage: harness.Usage{InputTokens: 7, InputReported: true}},
		reply{body: `{"wrong":"shape"}`}, returnReply("done"))
	f.cfg.Reader = &archive.Reader{Imports: imported}
	l := f.loop()
	f.steps(l, 6)
	rejected := f.last()
	if rejected.Event.Kind() != "request_rejected" || rejected.To.Step != 2 || rejected.To.Attempt != 2 || rejected.To.RequestRev != 1 || rejected.Facts.ContextState == nil || rejected.Facts.ContextState.AnchorSeq != rejected.Seq {
		t.Fatalf("rejection=%+v", rejected)
	}
	old := append([]byte(nil), f.frozen(2, 1)...)
	f.run(f.fresh())
	if !bytes.Equal(old, f.frozen(2, 1)) {
		t.Fatal("reset rewrote frozen input")
	}
	for _, rev := range []int{2, 3} {
		raw := f.frozen(2, rev)
		if bytes.Contains(raw, []byte("TRANSIENT_BEFORE_RESET")) || !bytes.Contains(raw, []byte("## Earlier steps")) || !bytes.Contains(raw, []byte("- Step 1:")) {
			t.Fatalf("reset revision %d lost its index or replayed the body: %s", rev, raw)
		}
	}
	if len(f.exec.calls) != 0 {
		t.Fatalf("partial decision executed: %v", f.exec.calls)
	}
	var prepare []string
	for _, tr := range f.transitions() {
		if tr.Facts.Request != nil && tr.To.Step == 2 {
			prepare = append(prepare, tr.Facts.Request.Context.Preparation)
		}
	}
	if !slices.Equal(prepare, []string{"first_request", "request_rejected", "protocol_repair"}) {
		t.Fatalf("preparation reasons=%v", prepare)
	}
	totals, err := f.store.Totals(context.Background(), runID)
	if err != nil || totals.Coverage.Attempts != 4 || totals.Coverage.InputReports != 3 || totals.Usage.InputTokens != 207 {
		t.Fatalf("usage=%+v error=%v", totals, err)
	}
}

func TestRetrievalStaysResidentAcrossSameStepRetryAndRepair(t *testing.T) {
	imported := t.TempDir()
	if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte("LATEST_RESULT_LIFETIME"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 4}, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"inspect"}`),
		reply{err: errors.New("transport interrupted")}, reply{body: `{"wrong":"shape"}`}, execReply("true"), returnReply("done"))
	f.cfg.Reader = &archive.Reader{Imports: imported}
	f.run(f.loop())
	for _, rev := range []int{1, 2} {
		if !bytes.Contains(f.frozen(2, rev), []byte("LATEST_RESULT_LIFETIME")) {
			t.Fatalf("revision %d consumed result early", rev)
		}
	}
	if !bytes.Equal(f.model.sent[1], f.model.sent[2]) {
		t.Fatal("transport retry rebuilt input")
	}
	if !bytes.Contains(f.frozen(3, 1), []byte("LATEST_RESULT_LIFETIME")) {
		t.Fatal("the completed retrieval left the resident epoch")
	}
	// The repair keeps the stable epoch byte-identical.
	a, b := f.model.prompts[1], f.model.prompts[2]
	if !reflect.DeepEqual(a.Blocks[:a.Stable], b.Blocks[:b.Stable]) {
		t.Fatal("repair changed the stable prefix")
	}
}

func TestInitialOrientationOverflowPreservesEvidenceAndUsage(t *testing.T) {
	signal := &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 4}, harness.Budget{}, reply{body: `{"error":{"code":"context_length_exceeded"}}`, err: signal, usage: harness.Usage{InputTokens: 9, InputReported: true}})
	snap := f.run(f.loop())
	if snap.State.Step != 1 || snap.State.Reason != core.StopUnrecoverable || len(f.model.sent) != 1 {
		t.Fatalf("irreducible orientation: %+v sends=%d", snap, len(f.model.sent))
	}
	for _, tr := range f.transitions() {
		if tr.Event.Kind() == "request_rejected" || tr.Facts.ContextState != nil {
			t.Fatal("irreducible orientation opened an empty context")
		}
	}
	if last := f.last(); last.Facts.Rejection == nil || last.Facts.Response == nil {
		t.Fatal("terminal rejection lost evidence")
	}
	totals, err := f.store.Totals(context.Background(), runID)
	if err != nil || totals.Usage.InputTokens != 9 || totals.Coverage.Attempts != 1 || totals.Coverage.InputReports != 1 {
		t.Fatalf("terminal usage=%+v %v", totals, err)
	}
}

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

func discoverPastHandover(t *testing.T, separator string) {
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
		execReply("echo needle"), execReply("canti:handover"+separator+"Find the needle again."),
		execReply(`canti:retrieve {"operation":"discover","path":"/canti/runs/run-1/steps","query":"needle","reason":"find the step"}`),
		returnReply("done"))
	artifacts := t.TempDir()
	f.cfg.Reader = &archive.Reader{History: f.cfg.History, Artifacts: artifacts}
	f.exec.inspect = finishWithArtifact(f.exec, artifacts)
	if s, err := f.loop().Run(f.bounded(), runID); err != nil || s.State.Phase != core.PhaseReturned {
		t.Fatalf("run=%+v err=%v", s.State, err)
	}
	steps, err := f.store.Recent(f.bounded(), runID, 0, 1)
	if err != nil || len(steps) != 1 || steps[0].Outcome == nil || steps[0].Outcome.Retrieval == nil {
		t.Fatalf("steps=%+v err=%v", steps, err)
	}
	var found []string
	for _, source := range steps[0].Outcome.Retrieval.Sources {
		found = append(found, source.Path)
	}
	if len(found) == 0 || slices.ContainsFunc(found, func(p string) bool { return strings.Contains(p, "/00000002/") }) {
		t.Fatalf("discovery found %v; want step 1 and not the handover of step 2", found)
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

type nativeSnapshotReadJournal struct {
	*sqlite.Store
	recentReads, receiptReads int
}

func (j *nativeSnapshotReadJournal) Recent(context.Context, string, int64, int) ([]harness.StepRecord, error) {
	j.recentReads++
	return nil, errors.New("native snapshot repair must not scan completion history")
}

func (j *nativeSnapshotReadJournal) Receipts(context.Context, string, int64, int) ([]harness.StepReceipt, error) {
	j.receiptReads++
	return nil, errors.New("native snapshot repair must not scan receipt history")
}

func TestNativeHistoricalSnapshotReadRepairsOnlyRequestedFrozenStep(t *testing.T) {
	for _, damage := range []string{"evidence", "manifest", "both", "same-size-conflict", "uncommitted-source", "cancelled"} {
		t.Run(damage, func(t *testing.T) {
			imported := t.TempDir()
			original := filepath.Join(imported, "source")
			text := "ORIGINAL_HISTORICAL_FROZEN_BYTES"
			if err := os.WriteFile(original, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			replies := []reply{execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/source","reason":"capture"}`), execReply("canti:handover-carry retired")}
			const unrelated = 8
			for range unrelated {
				replies = append(replies, execReply("canti:unrelated"))
			}
			replies = append(replies, reply{body: `{}`, err: &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}})
			sourceID := 0
			if damage == "uncommitted-source" {
				sourceID = 1
			}
			targetPath := fmt.Sprintf("/canti/runs/%s/steps/00000001/evidence/%04d", runID, sourceID)
			replies = append(replies, execReply(fmt.Sprintf(`canti:retrieve {"operation":"read","path":%q,"reason":"recover original snapshot"}`, targetPath)), returnReply("done"))
			f := newFixture(t, core.Limits{MaxSteps: 20, MaxAttempts: 4}, harness.Budget{}, replies...)
			f.cfg.Reader = &archive.Reader{Imports: imported, History: f.cfg.History}
			loop := f.loop()
			// Complete unrelated later work, reset, and accept the native historical
			// read before damage. The repair must occur in the action, not preparation.
			f.steps(loop, 4*(2+unrelated)+2)
			if f.last().Event.Kind() != "request_rejected" {
				t.Fatal("reset boundary not reached")
			}
			f.steps(f.fresh(), 3)
			before := latestSnapshot(t, f)
			if before.State.Phase != core.PhaseEffectSaved {
				t.Fatal("native read not accepted")
			}
			if err := os.Remove(original); err != nil {
				t.Fatal(err)
			}
			step := filepath.Join(f.cfg.History, runID, "steps", "00000001")
			evidence := filepath.Join(step, archive.EvidencePath(0))
			manifest := filepath.Join(step, archive.EvidenceFile)
			switch damage {
			case "evidence", "cancelled":
				if err := os.Remove(evidence); err != nil {
					t.Fatal(err)
				}
			case "manifest":
				if err := os.Remove(manifest); err != nil {
					t.Fatal(err)
				}
			case "both":
				for _, file := range []string{evidence, manifest} {
					if err := os.Remove(file); err != nil {
						t.Fatal(err)
					}
				}
			case "same-size-conflict":
				if err := os.WriteFile(evidence, []byte(strings.Repeat("X", len(text))), 0600); err != nil {
					t.Fatal(err)
				}
			case "uncommitted-source":
				if err := os.WriteFile(filepath.Join(step, archive.EvidencePath(1)), []byte("NOT_COMMITTED"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			journal := &nativeSnapshotReadJournal{Store: f.open()}
			resumed := f.loopOn(journal)
			if damage == "cancelled" {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := resumed.Step(ctx, runID); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled read: %v", err)
				}
				if got := latestSnapshot(t, f); got != before {
					t.Fatal("cancelled read committed")
				}
				if _, err := os.Stat(evidence); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("cancelled read restored files: %v", err)
				}
				return
			}
			after, err := resumed.Step(f.bounded(), runID)
			if err != nil {
				t.Fatalf("native snapshot read: %v", err)
			}
			if journal.recentReads != 0 || journal.receiptReads != 0 {
				t.Fatalf("repair scanned unrelated history: recent=%d receipts=%d", journal.recentReads, journal.receiptReads)
			}
			last := f.last()
			outcome := last.Facts.Outcome
			if damage == "same-size-conflict" || damage == "uncommitted-source" {
				failed := outcome != nil && outcome.Status == harness.OutcomeFailed || after.State.Phase == core.PhaseStopped && after.State.Reason == core.StopUnrecoverable && last.Facts.Failure != ""
				if !failed {
					t.Fatalf("invalid historical evidence accepted: %+v", last.Facts)
				}
				if damage == "same-size-conflict" {
					if got, err := os.ReadFile(evidence); err != nil || string(got) != strings.Repeat("X", len(text)) {
						t.Fatalf("conflict overwritten: %q %v", got, err)
					}
				}
				return
			}
			if outcome == nil || outcome.Status != harness.OutcomeDone || outcome.Retrieval == nil || !slices.Equal(outcome.Retrieval.Evidence, []string{text}) {
				t.Fatalf("old frozen bytes not recovered: %+v", outcome)
			}
			for _, file := range []string{evidence, manifest} {
				if _, err := os.Stat(file); err != nil {
					t.Fatalf("derived snapshot not repaired: %s %v", file, err)
				}
			}
			if got, err := os.ReadFile(evidence); err != nil || string(got) != text {
				t.Fatalf("repaired bytes: %q %v", got, err)
			}
		})
	}
}

func TestInitialOrientationOverflowStopsWithoutEmptyReset(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 4, MaxAttempts: 4}, harness.Budget{}, reply{body: `{}`, err: &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}})
	got := f.run(f.loop())
	if got.State.Phase != core.PhaseStopped || got.State.Reason != core.StopUnrecoverable {
		t.Fatalf("orientation overflow did not stop: %+v", got)
	}
	resets := 0
	for _, tr := range f.transitions() {
		if tr.Event.Kind() == "request_rejected" {
			resets++
		}
	}
	if len(f.model.sent) != 1 || resets != 0 {
		t.Fatalf("irreducible first orientation opened an empty epoch: sends=%d resets=%d", len(f.model.sent), resets)
	}
}

func TestEpochRetainsFrozenBytesAcrossRepairAndResendUntilHandover(t *testing.T) {
	imported := t.TempDir()
	text := "RESIDENT_EVIDENCE_LIFETIME"
	if err := os.WriteFile(filepath.Join(imported, "source"), []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, core.Limits{MaxSteps: 10, MaxAttempts: 5}, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/source","offset":0,"limit":8192,"reason":"capture"}`),
		execReply("canti:unrelated"),
		reply{err: errors.New("transport interrupted")}, reply{body: `{"wrong":"shape"}`},
		returnReply("done"))
	f.cfg.Reader = &archive.Reader{Imports: imported}
	f.run(f.loop())
	for _, position := range [][]int{{2, 1}, {3, 1}, {3, 2}} {
		if !bytes.Contains(f.frozen(position[0], position[1]), []byte(text)) {
			t.Fatalf("resident evidence vanished at step/rev %v", position)
		}
	}
	if !bytes.Equal(f.model.sent[2], f.model.sent[3]) {
		t.Fatal("same-revision resend changed bytes")
	}
	if len(f.model.prompts) != 4 {
		t.Fatalf("prompts = %d, want 4", len(f.model.prompts))
	}
	resident, repair := f.model.prompts[2], f.model.prompts[3]
	prefix := strings.Join(resident.Blocks[:resident.Stable], "")
	if prefix != strings.Join(repair.Blocks[:repair.Stable], "") || !strings.Contains(prefix, text) {
		t.Fatal("the repair changed the stable epoch or lost the resident evidence")
	}
	current, err := f.store.Context(context.Background(), latestSnapshot(t, f))
	if err != nil || current.AnchorSeq != 0 || current.Minimal {
		t.Fatalf("unexpected head: %+v %v", current, err)
	}
}

func TestLoopExhaustedOverflowSettlesUsageWithoutNewAnchor(t *testing.T) {
	imported := t.TempDir()
	if err := os.WriteFile(filepath.Join(imported, "source"), []byte("latest body"), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, core.Limits{MaxSteps: 5, MaxAttempts: 1}, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/source","offset":0,"limit":8192,"reason":"capture"}`),
		reply{body: `{}`, usage: harness.Usage{InputTokens: 7, InputReported: true, OutputReported: true}, err: &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}})
	f.cfg.Reader = &archive.Reader{Imports: imported}
	got := f.run(f.loop())
	if got.State.Step != 2 || got.State.Reason != core.StopAttemptsExhausted {
		t.Fatalf("exhausted overflow: %+v", got)
	}
	last := f.last()
	if last.Event.Kind() != "request_rejected" || last.Facts.ContextState != nil || last.Facts.Response == nil || last.Facts.Response.Usage.InputTokens != 7 {
		t.Fatalf("exhaustion lost settlement or opened context: %+v", last)
	}
	current, err := f.store.Context(context.Background(), got)
	if err != nil || !current.Equal(harness.InitialContext()) {
		t.Fatalf("exhaustion changed anchor: %+v %v", current, err)
	}
}

type freezeOnlyModel struct {
	harness.Model
	sends int
}

func (m *freezeOnlyModel) Complete(context.Context, harness.Request) (harness.Response, error) {
	m.sends++
	return harness.Response{}, errors.New("offline cap probe forbids sends")
}

func TestRequestResourceCapsStopLocallyBeforeProviderSend(t *testing.T) {
	for _, capKind := range []string{"rendered", "serialized"} {
		t.Run(capKind, func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{})
			cfg := f.cfg
			cfg.Limits = harness.DefaultPromptLimits()
			cfg.Environment = strings.Repeat(`"`, 2000)
			frame := harness.Frame{Goal: "Count the Go files.", Constraints: "Do not modify files.", Environment: cfg.Environment, RunID: runID, ContextState: &harness.ContextState{FirstStep: 1}, Step: 1, MaxSteps: limits.MaxSteps, Attempt: 1, MaxAttempts: limits.MaxAttempts}
			prompt, err := frame.Prompt(cfg.Limits)
			if err != nil {
				t.Fatal(err)
			}
			rendered := len(prompt.System) + len(prompt.User())
			model, err := anthropic.New(anthropic.Config{BaseURL: "https://unused.test", APIKey: "offline-fixture", Model: "offline-fixture-model"})
			if err != nil {
				t.Fatal(err)
			}
			frozen, err := model.Freeze(prompt)
			if err != nil {
				t.Fatal(err)
			}
			if len(frozen.Body) <= rendered {
				t.Fatal("probe did not produce serialization overhead")
			}
			cfg.Limits.Total = rendered
			if capKind == "rendered" {
				cfg.Limits.Total = rendered - 1
			}
			guarded := &freezeOnlyModel{Model: model}
			loop, err := harness.NewLoop(f.store, f.exec, guarded, cfg)
			if err != nil {
				t.Fatal(err)
			}
			got := f.run(loop)
			if got.State.Reason != core.StopUnrecoverable || guarded.sends != 0 {
				t.Fatalf("resource cap did not fail locally: %+v sends=%d", got, guarded.sends)
			}
			last := f.last()
			if last.Event.Kind() != "stop_requested" || !strings.Contains(last.Facts.Failure, "resource cap") || last.Facts.Rejection != nil {
				t.Fatalf("local resource cap became provider rejection: %+v", last)
			}
		})
	}
}

type cancelAfterSnapshotLookup struct {
	*nativeSnapshotReadJournal
	cancel  context.CancelFunc
	lookups int
}

func (j *cancelAfterSnapshotLookup) CompletionAtStep(ctx context.Context, run string, step int) (harness.StepRecord, error) {
	record, err := j.Store.CompletionAtStep(ctx, run, step)
	if err == nil {
		j.lookups++
		j.cancel()
	}
	return record, err
}

func TestNativeSnapshotCancellationAfterLookupPreventsPublicationAndProgress(t *testing.T) {
	imported := t.TempDir()
	original := filepath.Join(imported, "source")
	if err := os.WriteFile(original, []byte("ORIGINAL_FROZEN_CANCELLATION_EVIDENCE"), 0600); err != nil {
		t.Fatal(err)
	}
	target := fmt.Sprintf("/canti/runs/%s/steps/00000001/evidence/0000", runID)
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 4}, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/source","offset":0,"limit":8192,"reason":"capture"}`),
		execReply("canti:unrelated"), execReply(fmt.Sprintf(`canti:retrieve {"operation":"read","path":%q,"reason":"recover exact snapshot"}`, target)), returnReply("done"))
	f.cfg.Reader = &archive.Reader{Imports: imported, History: f.cfg.History}
	f.steps(f.loop(), 11)
	before := latestSnapshot(t, f)
	if before.State.Phase != core.PhaseEffectSaved {
		t.Fatal("historical read not accepted")
	}
	if err := os.Remove(original); err != nil {
		t.Fatal(err)
	}
	stepDir := filepath.Join(f.cfg.History, runID, "steps", "00000001")
	for _, name := range []string{archive.EvidencePath(0), archive.EvidenceFile} {
		if err := os.Remove(filepath.Join(stepDir, name)); err != nil {
			t.Fatal(err)
		}
	}
	priorFiles := tree(t, stepDir)
	ctx, cancel := context.WithCancel(f.bounded())
	defer cancel()
	journal := &cancelAfterSnapshotLookup{nativeSnapshotReadJournal: &nativeSnapshotReadJournal{Store: f.open()}, cancel: cancel}
	_, err := f.loopOn(journal).Step(ctx, runID)
	if !errors.Is(err, context.Canceled) || ctx.Err() != context.Canceled || journal.lookups != 1 {
		t.Fatalf("interleaved cancellation: error=%v context=%v lookups=%d", err, ctx.Err(), journal.lookups)
	}
	if got := latestSnapshot(t, f); got != before {
		t.Fatalf("cancelled repair committed progress: %+v", got)
	}
	if after := tree(t, stepDir); !reflect.DeepEqual(priorFiles, after) {
		t.Fatalf("cancelled repair published files: before=%v after=%v", priorFiles, after)
	}
	if journal.recentReads != 0 || journal.receiptReads != 0 {
		t.Fatal("cancelled repair scanned unrelated history")
	}
}

func TestExecutionPositionReportsCommittedUsage(t *testing.T) {
	signal := &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}
	exec := execReply("true")
	returned := returnReply("done")
	for _, tc := range []struct {
		name     string
		replies  []reply
		want     []string
		absent   []string
		rejected bool
	}{
		{
			name: "previous accepted and rejection maximum",
			replies: []reply{
				{body: exec.body, usage: harness.Usage{InputTokens: 100, InputReported: true}},
				{body: "{}", err: signal, usage: harness.Usage{InputTokens: 200, InputReported: true}},
				{body: returned.body, usage: harness.Usage{InputTokens: 0, InputReported: true}},
			},
			want: []string{"Previous request: 100 input tokens (provider-reported).",
				"Previous request: 200 input tokens (provider-reported).",
				"Provider context rejection at step 2; the largest earlier accepted request reported 100 input tokens."},
			rejected: true,
		},
		{
			name: "reported zero is shown",
			replies: []reply{
				{body: exec.body, usage: harness.Usage{InputTokens: 0, InputReported: true}},
				{body: returned.body, usage: harness.Usage{}},
			},
			want: []string{"Previous request: 0 input tokens (provider-reported)."},
		},
		{
			name: "an unreported previous attempt omits the line",
			replies: []reply{
				{body: exec.body, usage: harness.Usage{InputReported: false}},
				{body: returned.body, usage: harness.Usage{}},
			},
			absent: []string{"Previous request:"},
		},
		{
			name: "an unknown final attempt does not substitute older usage",
			replies: []reply{
				{body: exec.body, usage: harness.Usage{InputTokens: 300, InputReported: true}, err: errors.New("transport interrupted")},
				{body: exec.body, usage: harness.Usage{InputReported: false}},
				{body: returned.body, usage: harness.Usage{}},
			},
			absent: []string{"Previous request: 300"},
		},
		{
			name: "the maximum is taken strictly before the rejection",
			replies: []reply{
				{body: exec.body, usage: harness.Usage{InputTokens: 100, InputReported: true}},
				{body: execReply("true").body, usage: harness.Usage{InputTokens: 500, InputReported: true}},
				{body: "{}", err: signal, usage: harness.Usage{InputTokens: 900, InputReported: true}},
				{body: returned.body, usage: harness.Usage{}},
			},
			want:     []string{"Provider context rejection at step 3; the largest earlier accepted request reported 500 input tokens."},
			rejected: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, core.Limits{MaxSteps: 6, MaxAttempts: 3}, harness.Budget{}, tc.replies...)
			f.run(f.loop())
			var all []string
			for _, tr := range f.transitions() {
				if tr.Facts.Request != nil {
					all = append(all, string(tr.Facts.Request.Body))
				}
			}
			joined := strings.Join(all, "\n")
			for _, want := range tc.want {
				if !strings.Contains(joined, want) {
					t.Fatalf("requests lack %q:\n%s", want, joined)
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(joined, absent) {
					t.Fatalf("requests hold %q:\n%s", absent, joined)
				}
			}
			if tc.rejected {
				var rejection *harness.ContextRejection
				for _, tr := range f.transitions() {
					if tr.Facts.Rejection != nil {
						rejection = tr.Facts.Rejection
					}
				}
				if rejection == nil || !rejection.Signal.Confirmed() || rejection.MaxAcceptedInput == 0 && tc.name != "previous accepted and rejection maximum" {
					t.Fatalf("rejection record = %+v", rejection)
				}
			}
		})
	}
}

func TestMinimalStaysThroughARejectionAfterAStep(t *testing.T) {
	signal := &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}
	imported := t.TempDir()
	if err := os.WriteFile(filepath.Join(imported, "source"), []byte("0123456789SMALL_RANGE"), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, core.Limits{MaxSteps: 6, MaxAttempts: 4}, harness.Budget{},
		handoverReply("a phase note"), returnReply("done"),
		reply{body: "{}", err: signal},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/source","reason":"read","limit":12}`),
		reply{body: "{}", err: signal},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/source","reason":"read","offset":10,"limit":5}`),
		returnReply("done"))
	f.cfg.Reader = &archive.Reader{Imports: imported}
	f.run(f.loop())
	const second = "run-2"
	state, err := core.NewState(second, core.Limits{MaxSteps: 6, MaxAttempts: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.CreateRun(context.Background(), harness.Run{ID: second, Goal: "Continue.", Constraints: "Do not modify files.", Created: created}, state); err != nil {
		t.Fatal(err)
	}
	if _, err := f.loop().Run(f.bounded(), second); err != nil {
		t.Fatal(err)
	}
	// The request that the second rejection rejected still had the first
	// retrieval resident.
	if rejected, err := f.store.Request(f.bounded(), second, 2, 1); err != nil || rejected.ContextState == nil || !rejected.ContextState.Minimal || rejected.Context == nil || len(rejected.Context.Bytes.Steps) != 1 {
		t.Fatalf("the request before the second rejection = %+v, %v", rejected.ContextState, err)
	}
	// After the second rejection the head stays minimal, and the epoch keeps
	// the resident step without re-expanding optional content.
	minimal, err := f.store.Request(f.bounded(), second, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if minimal.ContextState == nil || !minimal.ContextState.Minimal || minimal.Context == nil || len(minimal.Context.Bytes.Steps) != 0 {
		t.Fatalf("the head after the second rejection = %+v", minimal.ContextState)
	}
	if minimal.Context.Bytes.Index != 0 || minimal.Context.Bytes.Previous != 0 || minimal.Context.Bytes.Observations != 0 || minimal.Context.Bytes.Notice == 0 {
		t.Fatalf("the request after the second rejection rendered an optional body: %+v", minimal.Context.Bytes)
	}
	// The small read makes progress in the minimal epoch; the next request
	// keeps both resident steps and still omits every optional body.
	next, err := f.store.Request(f.bounded(), second, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if next.ContextState == nil || !next.ContextState.Minimal || next.Context == nil || len(next.Context.Bytes.Steps) != 1 {
		t.Fatalf("the request after the small read = %+v", next.ContextState)
	}
	if next.Context.Bytes.Index != 0 || next.Context.Bytes.Previous != 0 || next.Context.Bytes.Observations != 0 || next.Context.Bytes.Notice == 0 {
		t.Fatalf("a minimal request rendered an optional body: %+v", next.Context.Bytes)
	}
	if !strings.Contains(string(next.Body), "SMALL") || !strings.Contains(string(next.Body), "### Step 2") {
		t.Fatalf("the small read did not reach the next request: %s", next.Body)
	}
	if snap, err := f.store.Latest(f.bounded(), second); err != nil || snap.State.Phase != core.PhaseReturned {
		t.Fatalf("the run did not continue to completion: %+v, %v", snap.State, err)
	}
}

func TestMinimalRejectionIsTerminal(t *testing.T) {
	signal := &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}
	f := newFixture(t, core.Limits{MaxSteps: 6, MaxAttempts: 4}, harness.Budget{},
		handoverReply("a phase note"), returnReply("done"),
		reply{body: "{}", err: signal}, reply{body: "{}", err: signal})
	f.run(f.loop())
	const second = "run-2"
	state, err := core.NewState(second, core.Limits{MaxSteps: 6, MaxAttempts: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.CreateRun(context.Background(), harness.Run{ID: second, Goal: "Continue.", Created: created}, state); err != nil {
		t.Fatal(err)
	}
	snap, err := f.loop().Run(f.bounded(), second)
	if err != nil || snap.State.Phase != core.PhaseStopped || snap.State.Reason != core.StopUnrecoverable {
		t.Fatalf("minimal rejection = %+v, %v", snap.State, err)
	}
	all, err := f.store.Transitions(context.Background(), second, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	last := all[len(all)-1]
	if last.Event.Kind() != "stop_requested" || !strings.Contains(last.Facts.Failure, "minimal context") || strings.Contains(last.Facts.Failure, "authoritative orientation in fresh") {
		t.Fatalf("terminal wording = %+v", last)
	}
}

func TestRejectionClassifiesTheFrozenRequestNotNewObservations(t *testing.T) {
	signal := &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}
	f := newFixture(t, core.Limits{MaxSteps: 4, MaxAttempts: 3}, harness.Budget{}, reply{body: "{}", err: signal})
	f.model.replies[0].hook = func() {
		if _, err := f.store.Admit(context.Background(), harness.InputSubmission{Authority: harness.AuthorityObservation,
			Source: "ci", Subject: "build", Text: "admitted after the freeze", Now: created}); err != nil {
			t.Fatal(err)
		}
	}
	snap := f.run(f.loop())
	if snap.State.Phase != core.PhaseStopped || snap.State.Reason != core.StopUnrecoverable {
		t.Fatalf("late observation changed the classification: %+v", snap.State)
	}
	last := f.last()
	if last.Event.Kind() != "stop_requested" || !strings.Contains(last.Facts.Failure, "authoritative orientation") ||
		last.Facts.Rejection == nil || last.Facts.Rejection.OptionalContent {
		t.Fatalf("terminal rejection = %+v", last)
	}
	for _, tr := range f.transitions() {
		if tr.Event.Kind() == "request_rejected" {
			t.Fatal("a late observation opened a head")
		}
	}
}

func TestUsageLineFollowsAnInvalidDecision(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 4, MaxAttempts: 3}, harness.Budget{},
		reply{body: "not a decision", usage: harness.Usage{InputTokens: 100, InputReported: true}},
		returnReply("done"))
	f.run(f.loop())
	if body := string(f.frozen(1, 2)); !strings.Contains(body, "Previous request: 100 input tokens (provider-reported).") || !strings.Contains(body, "## Protocol repair") {
		t.Fatalf("a repair lost the invalid decision's usage: %s", body)
	}
}

func TestUsageLineFollowsASupersededDecision(t *testing.T) {
	decision := core.Decision{Kind: core.Exec, Payload: "true"}
	body, err := json.Marshal(decision)
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, core.Limits{MaxSteps: 4, MaxAttempts: 3}, harness.Budget{},
		reply{body: string(body), usage: harness.Usage{InputTokens: 100, InputReported: true}},
		returnReply("done"))
	f.model.replies[0].hook = func() {
		if _, err := f.store.Admit(context.Background(), harness.InputSubmission{InputID: "steer-usage", Authority: harness.AuthorityPrincipal,
			Delivery: harness.DeliverySteer, Text: "use sync", Now: created}); err != nil {
			t.Fatal(err)
		}
	}
	f.run(f.loop())
	if body := string(f.frozen(2, 1)); !strings.Contains(body, "Previous request: 100 input tokens (provider-reported).") || !strings.Contains(body, "use sync") {
		t.Fatalf("a superseded decision's usage or instruction was lost: %s", body)
	}
}

func TestIncompleteResponseDoesNotCountTowardTheRejectionMaximum(t *testing.T) {
	signal := &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}
	exec := execReply("true")
	f := newFixture(t, core.Limits{MaxSteps: 5, MaxAttempts: 4}, harness.Budget{},
		reply{body: "partial", err: errors.New("transport interrupted"), usage: harness.Usage{InputTokens: 5000, InputReported: true}},
		reply{body: exec.body, usage: harness.Usage{InputTokens: 100, InputReported: true}},
		reply{body: "{}", err: signal, usage: harness.Usage{InputTokens: 300, InputReported: true}},
		returnReply("done"))
	f.run(f.loop())
	var rejection *harness.ContextRejection
	for _, tr := range f.transitions() {
		if tr.Facts.Rejection != nil {
			rejection = tr.Facts.Rejection
		}
	}
	if rejection == nil || rejection.MaxAcceptedInput != 100 || !rejection.MaxAcceptedReported {
		t.Fatalf("the incomplete response counted toward the maximum: %+v", rejection)
	}
	if body := string(f.frozen(2, 2)); !strings.Contains(body, "the largest earlier accepted request reported 100 input tokens") || strings.Contains(body, "5000 input tokens") {
		t.Fatalf("the rejection usage line counted the incomplete response: %s", body)
	}
}

// TestIndexLeadsBackToEvidenceAfterANoteLessHandover requires that a
// note-less handover lists its earlier step, that the scripted decision can
// retrieve that step's record through the index path, and that the retrieved
// result stays for the epoch.
func TestIndexLeadsBackToEvidenceAfterANoteLessHandover(t *testing.T) {
	const evidence = "KEY_EVIDENCE_FROM_STEP_ONE"
	imported := t.TempDir()
	if err := os.WriteFile(filepath.Join(imported, "source"), []byte(evidence), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, core.Limits{MaxSteps: 6, MaxAttempts: 3}, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/source","offset":0,"limit":8192,"reason":"capture"}`),
		execReply("canti:handover"),
		execReply(`canti:retrieve {"operation":"read","path":"/canti/runs/run-1/steps/00000001/evidence/0000","reason":"recall"}`),
		returnReply("done"))
	f.cfg.Reader = &archive.Reader{History: f.cfg.History, Imports: imported}
	f.run(f.loop())
	post := string(f.frozen(3, 1))
	for _, want := range []string{"## Earlier steps", "retrieval read /canti/imports/source offset=0 limit=8192", "- Step 2: done;", "Step records: /canti/runs/run-1/steps/"} {
		if !strings.Contains(post, want) {
			t.Fatalf("the request after the handover lacks %q:\n%s", want, post)
		}
	}
	if strings.Contains(post, "## Handover") || strings.Contains(post, "KEY_EVIDENCE_FROM_STEP_ONE") {
		t.Fatalf("the request after the handover carried the note or the old evidence:\n%s", post)
	}
	last := string(f.frozen(4, 1))
	if !strings.Contains(last, "### Step 3") || !strings.Contains(last, evidence) {
		t.Fatalf("the retrieved result did not stay resident for the epoch:\n%s", last)
	}
}

// reported returns r reporting input tokens.
func reported(r reply, input int64) reply {
	r.usage = harness.Usage{InputTokens: input, InputReported: true, OutputTokens: 10, OutputReported: true}
	return r
}

// budgetRequests returns the frozen requests of f in commit order with the
// transitions that froze them.
func budgetRequests(f *fixture) []harness.Transition {
	var out []harness.Transition
	for _, tr := range f.transitions() {
		if tr.Facts.Request != nil {
			out = append(out, tr)
		}
	}
	return out
}

func TestBudgetAnchorKeepsTheLatestResultAndTheRecentSteps(t *testing.T) {
	imported := t.TempDir()
	if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte("LATEST_VALUE_7f3a"), 0o600); err != nil {
		t.Fatal(err)
	}
	const budget = 1000
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{ContextTokens: budget},
		reported(execReply("true"), 10), reported(execReply("true"), 10), reported(execReply("true"), 10),
		reported(execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"inspect"}`), budget),
		reported(execReply("true"), 300), reported(returnReply("done"), 300))
	f.cfg.Reader = &archive.Reader{Imports: imported}
	f.run(f.loop())

	var anchor, next harness.Transition
	for _, tr := range budgetRequests(f) {
		switch {
		case tr.Facts.ContextState != nil:
			if anchor.Seq != 0 {
				t.Fatalf("a second budget anchor at step %d", tr.From.Step)
			}
			anchor = tr
		case anchor.Seq != 0 && next.Seq == 0:
			next = tr
		}
	}
	if anchor.Seq == 0 || anchor.From.Step != 5 {
		t.Fatalf("no budget anchor at step 5: %+v", anchor)
	}
	head, b := *anchor.Facts.ContextState, anchor.Facts.Request.Context.Budget
	if head.AnchorSeq != anchor.Seq || !anchor.Facts.Request.ContextState.Equal(head) || b == nil || b.Replaced == nil || b.Replaced.FirstStep != 1 ||
		b.Tokens != budget || b.Source != harness.BudgetConfigured || b.Input != budget || b.Projected <= budget {
		t.Fatalf("anchor head=%+v budget=%+v", head, b)
	}
	if head.FirstStep != harness.TailStep(b.Steps, budget) || head.FirstStep <= 1 || head.FirstStep > 4 {
		t.Fatalf("tail starts at %d over %+v", head.FirstStep, b.Steps)
	}
	body := string(anchor.Facts.Request.Body)
	if !strings.Contains(body, "LATEST_VALUE_7f3a") || !strings.Contains(body, "### Step 4") || strings.Contains(body, "### Step 1\\n") ||
		!strings.Contains(body, "- Step 1:") || strings.Contains(body, "Latest completed step") || !strings.Contains(body, "Context budget: 1000 input tokens (configured).") {
		t.Fatalf("the anchoring request lost the latest result, kept an old step, or lacks the index or budget: %s", body)
	}
	if nb := next.Facts.Request.Context.Budget; next.Seq == 0 || next.Facts.ContextState != nil || nb == nil || nb.InputSeq != anchor.Seq || nb.Input != 300 {
		t.Fatalf("the request after the anchor is not budgeted from the anchoring request: %+v", nb)
	}
	if got := f.transitions()[len(f.transitions())-1]; got.To.Phase != core.PhaseReturned {
		t.Fatalf("run ended in %s", got.To.Phase)
	}
}

func TestBudgetAnchorOpensOnlyWhenUsageIsComparableAndAStepCanLeave(t *testing.T) {
	for _, tc := range []struct {
		name    string
		replies []reply
		record  bool
	}{
		{
			name:    "no reported usage",
			replies: []reply{{body: execReply("true").body}, {body: execReply("true").body}, {body: execReply("true").body}, returnReply("done")},
		},
		{
			name:    "one resident step",
			replies: []reply{reported(execReply("true"), 5000), reported(returnReply("done"), 10)},
			record:  true,
		},
		{
			name: "a final attempt without usage",
			replies: []reply{reported(execReply("true"), 10), reported(execReply("true"), 10), reported(execReply("true"), 10),
				{body: execReply("true").body, usage: harness.Usage{InputTokens: 5000, InputReported: true}, err: errors.New("transport interrupted")},
				{body: execReply("true").body}, reported(returnReply("done"), 10)},
			record: true,
		},
		{
			name: "protocol repair",
			replies: []reply{reported(execReply("true"), 10), reported(execReply("true"), 10), reported(execReply("true"), 10),
				reported(reply{body: `{"wrong":"shape"}`}, 5000), reported(returnReply("done"), 10)},
			record: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, core.Limits{MaxSteps: 6, MaxAttempts: 3}, harness.Budget{ContextTokens: 1000}, tc.replies...)
			f.run(f.loop())
			recorded := false
			for _, tr := range budgetRequests(f) {
				if tr.Facts.ContextState != nil {
					t.Fatalf("budget anchor at step %d: %+v", tr.From.Step, tr.Facts.Request.Context.Budget)
				}
				recorded = recorded || tr.Facts.Request.Context.Budget != nil
			}
			if recorded != tc.record {
				t.Fatalf("budget recorded = %v, want %v", recorded, tc.record)
			}
			if tc.name == "a final attempt without usage" {
				for _, tr := range budgetRequests(f) {
					if tr.From.Step == 5 && tr.Facts.Request.Context.Budget != nil {
						t.Fatalf("step 5 was budgeted from an attempt before the last: %+v", tr.Facts.Request.Context.Budget)
					}
				}
			}
			if tc.name != "protocol repair" {
				return
			}
			for _, tr := range budgetRequests(f) {
				if tr.From.Step != 4 {
					continue
				}
				if b := tr.Facts.Request.Context.Budget; (b != nil) != (tr.To.RequestRev == 1) {
					t.Fatalf("revision %d of step 4 records %+v; only the first request is budget-checked", tr.To.RequestRev, b)
				}
			}
		})
	}
}

func TestLearnedBudgetAnchorsBeforeTheWindowAfterARejection(t *testing.T) {
	signal := &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
		reported(execReply("true"), 500), reported(execReply("true"), 900),
		reply{body: "{}", err: signal},
		reported(execReply("true"), 300), reported(execReply("true"), 300), reported(execReply("true"), 880),
		reported(execReply("true"), 300), reported(returnReply("done"), 300))
	f.run(f.loop())
	var anchors []*harness.ContextBudget
	for _, tr := range budgetRequests(f) {
		if tr.Facts.ContextState != nil {
			anchors = append(anchors, tr.Facts.Request.Context.Budget)
		}
	}
	if len(anchors) != 1 || anchors[0].Source != harness.BudgetRejection || anchors[0].Tokens != 900 || anchors[0].Input != 880 {
		t.Fatalf("learned-budget anchors = %+v", anchors)
	}
	if !strings.Contains(string(f.frozen(6, 1)), "Context budget: 900 input tokens (learned from the rejection at step 3).") {
		t.Fatalf("the learned budget is not shown: %s", f.frozen(6, 1))
	}
}

func TestASteerBoundAfterARejectionKeepsTheRetryARejectionRetry(t *testing.T) {
	signal := &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}
	rejected := reply{body: "{}", err: signal}
	f := newFixture(t, core.Limits{MaxSteps: 6, MaxAttempts: 3}, harness.Budget{},
		reported(execReply("true"), 100), rejected, reported(execReply("true"), 100), returnReply("done"))
	f.model.replies[1].hook = func() {
		if _, err := f.store.Admit(context.Background(), harness.InputSubmission{InputID: "steer-after-overflow", Authority: harness.AuthorityPrincipal,
			Delivery: harness.DeliverySteer, Text: "prefer the sync API", Now: created}); err != nil {
			t.Fatal(err)
		}
	}
	f.steps(f.loop(), 7)
	f.run(f.fresh())
	var prepare []string
	for _, tr := range budgetRequests(f) {
		if tr.To.Step == 2 {
			prepare = append(prepare, tr.Facts.Request.Context.Preparation)
		}
	}
	if len(prepare) < 2 || prepare[1] != "request_rejected" {
		t.Fatalf("preparation reasons at step 2 = %v", prepare)
	}
	if body := string(f.frozen(2, 2)); strings.Contains(body, "## Protocol repair") || !strings.Contains(body, "prefer the sync API") {
		t.Fatalf("the retry after the rejection is a repair or lost the steer: %s", body)
	}
}

// anchoredRun drives a run whose fifth request opens a budget anchor and
// returns the fixture with the anchor's sequence and head.
func anchoredRun(t *testing.T) (*fixture, int64, harness.ContextState) {
	t.Helper()
	const budget = 1000
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{ContextTokens: budget},
		reported(execReply("true"), 10), reported(execReply("true"), 10), reported(execReply("true"), 10),
		reported(execReply("printf four"), budget), reported(execReply("true"), 300), reported(returnReply("done"), 300))
	f.run(f.loop())
	for _, tr := range budgetRequests(f) {
		if tr.Facts.ContextState != nil {
			return f, tr.Seq, *tr.Facts.ContextState
		}
	}
	t.Fatal("the run opened no budget anchor")
	return nil, 0, harness.ContextState{}
}

func TestRecoveryReadsABudgetAnchorAndRefusesADamagedOne(t *testing.T) {
	ctx := context.Background()
	f, _, head := anchoredRun(t)
	s := f.open()
	snap, err := s.Latest(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Context(ctx, snap); err != nil || !got.Equal(head) {
		t.Fatalf("recovered head = %+v, %v; want %+v", got, err, head)
	}
	for _, tc := range []struct{ name, facts string }{
		{"tokens", `json_set(facts,'$.Request.Context.Budget.Tokens',999)`},
		{"input", `json_set(facts,'$.Request.Context.Budget.Input',1001,'$.Request.Context.Budget.Projected',json_extract(facts,'$.Request.Context.Budget.Projected')+1)`},
		{"input request", `json_set(facts,'$.Request.Context.Budget.InputSeq',1)`},
		{"replaced step bytes", `json_set(facts,'$.Request.Context.Budget.Steps[0].Bytes',json_extract(facts,'$.Request.Context.Budget.Steps[0].Bytes')+1)`},
		{"replaced head", `json_set(facts,'$.Request.Context.Budget.Replaced.FirstStep',2)`},
		{"source", `json_set(facts,'$.Request.Context.Budget.Source','rejection')`},
		{"no head", `json_remove(facts,'$.ContextState')`},
		{"capture without retained steps", `json_remove(facts,'$.Request.Context.Bytes.Steps')`},
		{"capture of another head", `json_set(facts,'$.Request.Context.Anchor.FirstStep',1)`},
		{"no record", `json_remove(facts,'$.Request.Context.Budget')`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, seq, _ := anchoredRun(t)
			db, err := sql.Open("sqlite", f.path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(`UPDATE transitions SET facts=`+tc.facts+` WHERE run_id=? AND seq=?`, runID, seq); err != nil {
				t.Fatal(err)
			}
			s := f.open()
			snap, err := s.Latest(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := s.Context(ctx, snap); err == nil {
				t.Fatalf("damaged budget anchor recovered as %+v", got)
			}
		})
	}
}

// windowModel is a provider whose window holds window input tokens, counting
// one token per request byte, and that reports that count for every request it
// answers. It reads filler until the request shows the secret, retrieves the
// secret once, and returns the secret only while a request shows it.
type windowModel struct {
	mu       sync.Mutex
	window   int
	fillers  int
	calls    int
	rejected int
	secret   string
}

func (m *windowModel) Freeze(p harness.Prompt) (harness.Request, error) {
	return harness.Request{Body: []byte(p.System + "\n\n" + p.User()), Model: "fake-1", Protocol: "fake/v1", Adapter: "fake"}, nil
}

func (m *windowModel) Complete(_ context.Context, req harness.Request) (harness.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(req.Body) > m.window {
		m.rejected++
		return harness.Response{Body: []byte("{}"), Model: "fake-1", Protocol: "fake/v1", Adapter: "fake"}, &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}
	}
	m.calls++
	read := func(name string) core.Decision {
		return core.Decision{Kind: core.Exec, Purpose: fmt.Sprintf("read %s %d", name, m.calls),
			Payload: fmt.Sprintf(`canti:retrieve {"operation":"read","path":"/canti/imports/%s","reason":"read %d"}`, name, m.calls)}
	}
	d := core.Decision{Kind: core.Return, Payload: "lost the secret"}
	switch {
	case bytes.Contains(req.Body, []byte(m.secret)):
		d = core.Decision{Kind: core.Return, Payload: m.secret}
	case m.calls <= m.fillers:
		d = read("filler")
	case m.calls == m.fillers+1:
		d = read("secret")
	}
	body, _ := json.Marshal(d)
	return harness.Response{Body: body, Complete: true, Model: "fake-1", Protocol: "fake/v1", Adapter: "fake",
		Usage: harness.Usage{InputTokens: int64(len(req.Body)), InputReported: true, OutputTokens: 1, OutputReported: true}}, nil
}

func (m *windowModel) Decision(r harness.Response) []byte { return r.Body }

func TestAWindowedRunDeliversTheAnswerOnlyItsLatestReadHolds(t *testing.T) {
	const window = 20000
	for _, tc := range []struct {
		name      string
		budget    int64
		rejection int
	}{{"configured budget", window, 0}, {"learned budget", 0, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			imported := t.TempDir()
			if err := os.WriteFile(filepath.Join(imported, "filler"), bytes.Repeat([]byte("filler line of an earlier result\n"), 45), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(imported, "secret"), []byte("SECRET_VALUE_9c41"), 0o600); err != nil {
				t.Fatal(err)
			}
			f := newFixture(t, core.Limits{MaxSteps: 40, MaxAttempts: 3}, harness.Budget{ContextTokens: tc.budget})
			f.cfg.Reader = &archive.Reader{Imports: imported}
			m := &windowModel{window: window, fillers: 16, secret: "SECRET_VALUE_9c41"}
			l, err := harness.NewLoop(f.store, f.exec, m, f.cfg)
			if err != nil {
				t.Fatal(err)
			}
			final := f.run(l)
			anchors := 0
			for _, tr := range budgetRequests(f) {
				if tr.Facts.ContextState != nil {
					anchors++
				}
			}
			if final.State.Phase != core.PhaseReturned || final.State.Decision.Payload != m.secret || m.rejected != tc.rejection || anchors == 0 {
				t.Fatalf("returned %q in %s after %d rejections and %d budget anchors", final.State.Decision.Payload, final.State.Phase, m.rejected, anchors)
			}
		})
	}
}

var errIntercepted = errors.New("commit intercepted")

// interceptedCommit keeps, instead of committing, the first frozen request of
// step.
type interceptedCommit struct {
	*sqlite.Store
	step int
	kept *harness.Transition
}

func (j *interceptedCommit) Commit(ctx context.Context, t harness.Transition) error {
	if j.kept == nil && t.Facts.Request != nil && t.From.Step == j.step {
		j.kept = &t
		return errIntercepted
	}
	return j.Store.Commit(ctx, t)
}

func TestCommitRefusesARequestThatSkipsItsBudgetAnchor(t *testing.T) {
	const budget = 1000
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{ContextTokens: budget},
		reported(execReply("true"), 10), reported(execReply("true"), 10), reported(execReply("true"), 10),
		reported(execReply("printf four"), budget), reported(returnReply("done"), 10))
	j := &interceptedCommit{Store: f.open(), step: 5}
	l := f.loopOn(j)
	for j.kept == nil {
		snap, err := l.Step(f.bounded(), runID)
		if err != nil && !errors.Is(err, errIntercepted) {
			t.Fatal(err)
		}
		if snap.State.Intent() == core.IntentHalt {
			t.Fatalf("the run halted before its fifth request: %+v", snap.State)
		}
	}
	anchor := *j.kept
	b := anchor.Facts.Request.Context.Budget
	if anchor.Facts.ContextState == nil || b == nil || b.Replaced == nil {
		t.Fatalf("the fixture's fifth request opens no budget anchor: %+v", anchor.Facts)
	}
	// unanchored is the anchoring request committed as if no anchor opened.
	unanchored := func() harness.Transition {
		tr := anchor
		request, capture, record := *tr.Facts.Request, *tr.Facts.Request.Context, *b
		replaced := *b.Replaced
		record.Steps, record.Replaced = nil, nil
		capture.Anchor, capture.Bytes.Steps, capture.Bytes.Total, capture.Budget = replaced, b.Steps, b.Bytes, &record
		// The initial head shows no index; the tail absorbs the rest so the
		// sections still sum to the total.
		c := &capture.Bytes
		c.Index, c.Progress = 0, 0
		rest := c.System + c.Goal + c.Constraints + c.Project + c.Skills + c.HeadInstructions + c.Environment + c.Anchor + c.Previous + c.Notice +
			c.EpochHeader + c.EpochInstructions + c.Observations + c.Latest + c.Repair
		for _, step := range c.Steps {
			rest += step.Bytes
		}
		c.Progress = c.Total - rest
		request.ContextState, request.Context = &replaced, &capture
		tr.Facts.Request, tr.Facts.ContextState = &request, nil
		return tr
	}
	for _, tc := range []struct {
		name, reason string
		damage       func(*harness.Transition)
	}{
		{"steps that could leave", "could leave", func(*harness.Transition) {}},
		{"no capture", "records its capture", func(tr *harness.Transition) { tr.Facts.Request.Context = nil }},
		{"a first request labelled a repair", `preparation "protocol_repair"`, func(tr *harness.Transition) {
			tr.Facts.Request.Context.Preparation, tr.Facts.Request.Context.Budget = "protocol_repair", nil
		}},
		{"no resident steps captured", "does not render the resident epoch", func(tr *harness.Transition) {
			c := tr.Facts.Request.Context
			for _, step := range c.Bytes.Steps {
				c.Bytes.Progress += step.Bytes
			}
			c.Bytes.Steps = nil
		}},
		{"an understated total", "sum to", func(tr *harness.Transition) {
			c := tr.Facts.Request.Context
			c.Bytes.Total, c.Budget.Bytes, c.Budget.Projected = c.Budget.InputBytes, c.Budget.InputBytes, c.Budget.Input
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := unanchored()
			tc.damage(&tr)
			if err := f.store.Commit(context.Background(), tr); err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("a request that skipped its budget anchor: %v, want %q", err, tc.reason)
			}
		})
	}
	if err := f.store.Commit(context.Background(), anchor); err != nil {
		t.Fatalf("the genuine budget anchor was refused: %v", err)
	}
}

func TestPreflightCountsTheContextBudgetLine(t *testing.T) {
	run := harness.Run{ID: runID, Goal: "Count the Go files.", Constraints: "Do not modify files.", Created: created}
	budgeted := run
	budgeted.Budget.ContextTokens = 6100
	check := func(r harness.Run, total int) error {
		cfg := harness.LoopConfig{Environment: "Alpine Linux container", Limits: harness.DefaultPromptLimits()}
		cfg.Limits.Total = total
		return harness.CheckOrientation(cfg, r, project.Snapshot{}, core.Limits{MaxSteps: 4, MaxAttempts: 3})
	}
	need := func(r harness.Run) int {
		var n int
		if err := check(r, 1); err == nil {
			t.Fatal("a one-byte cap admitted orientation")
		} else if _, scan := fmt.Sscanf(err.Error(), "authoritative orientation needs at least %d bytes", &n); scan != nil {
			t.Fatalf("unexpected preflight error: %v", err)
		}
		return n
	}
	plain, withBudget := need(run), need(budgeted)
	if withBudget <= plain {
		t.Fatalf("orientation with a budget needs %d bytes, without %d", withBudget, plain)
	}
	if err := check(run, plain); err != nil {
		t.Fatalf("orientation that fits refused: %v", err)
	}
	if err := check(budgeted, plain); err == nil {
		t.Fatal("preflight admitted a budgeted run whose first request exceeds the cap")
	}
}
