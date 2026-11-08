package harness_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
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

func TestFocusReplacesFrozenSelectionAndInvalidSelectionPreservesIt(t *testing.T) {
	imported := t.TempDir()
	for name, text := range map[string]string{"a": "ORIGINAL_A", "b": "ORIGINAL_B"} {
		if err := os.WriteFile(filepath.Join(imported, name), []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f := newFixture(t, core.Limits{MaxSteps: 10, MaxAttempts: 3}, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/a","reason":"a"}`),
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/b","reason":"b"}`),
		execReply(`canti:focus {"sources":[{"seq":5,"source":0}]}`),
		execReply(`canti:focus {"sources":[{"seq":10,"source":0}]}`),
		execReply(`canti:focus {"sources":[{"seq":10,"source":0},{"seq":10,"source":0}]}`),
		execReply(`canti:focus {"sources":[]}`), returnReply("done"))
	f.cfg.Reader = &archive.Reader{Imports: imported}
	f.steps(f.loop(), 12)
	first, err := f.store.Context(context.Background(), focusSnapshot(t, f))
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Focus) != 1 || first.Focus[0].SourceSeq != 5 {
		t.Fatalf("first selection=%+v", first)
	}
	if err := os.RemoveAll(imported); err != nil {
		t.Fatal(err)
	}
	f.steps(f.fresh(), 4)
	second, err := f.store.Context(context.Background(), focusSnapshot(t, f))
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Focus) != 1 || second.Focus[0].SourceSeq != 10 {
		t.Fatalf("replacement=%+v", second)
	}
	f.steps(f.fresh(), 4)
	invalid, err := f.store.Context(context.Background(), focusSnapshot(t, f))
	if err != nil || !invalid.Equal(second) {
		t.Fatalf("invalid changed selection=%+v %v", invalid, err)
	}
	if got := f.last().Facts.Outcome; got == nil || got.Status != harness.OutcomeFailed {
		t.Fatalf("duplicate selection result=%+v", got)
	}
	f.run(f.fresh())
	cleared, err := f.store.Context(context.Background(), focusSnapshot(t, f))
	if err != nil || len(cleared.Focus) != 0 || cleared.AnchorSeq != 0 {
		t.Fatalf("clear=%+v %v", cleared, err)
	}
	if len(f.exec.calls) != 0 {
		t.Fatalf("focus reached executor: %v", f.exec.calls)
	}
}

func focusSnapshot(t *testing.T, f *fixture) harness.Snapshot {
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
		if bytes.Contains(raw, []byte("TRANSIENT_BEFORE_RESET")) || !bytes.Contains(raw, []byte("Its result predates this anchor")) {
			t.Fatalf("reset revision %d lost orientation or replayed body: %s", rev, raw)
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

func TestLatestRetrievalSurvivesSameStepRetryAndRepairThenExpires(t *testing.T) {
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
	if bytes.Contains(f.frozen(3, 1), []byte("LATEST_RESULT_LIFETIME")) {
		t.Fatal("completed step retained transient retrieval body")
	}
	// Receipts remain stable while the dynamic result is replaced.
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
			replies := []reply{execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/source","reason":"capture"}`), execReply(`canti:focus {"sources":[{"seq":5,"source":0}]}`)}
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
			before := focusSnapshot(t, f)
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
				if got := focusSnapshot(t, f); got != before {
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

func TestFocusRetainsFrozenBytesAcrossRepairAndResendUntilReset(t *testing.T) {
	imported := t.TempDir()
	text := "FOCUS_EVIDENCE_LIFETIME"
	if err := os.WriteFile(filepath.Join(imported, "source"), []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 5}, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/source","reason":"capture"}`),
		execReply(`canti:focus {"sources":[{"seq":5,"source":0}]}`), execReply("canti:unrelated"),
		reply{err: errors.New("transport interrupted")}, reply{body: `{"wrong":"shape"}`},
		reply{body: `{}`, err: &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}}, returnReply("done"))
	f.cfg.Reader = &archive.Reader{Imports: imported}
	f.run(f.loop())
	for _, position := range [][2]int{{3, 1}, {4, 1}, {4, 2}} {
		if !bytes.Contains(f.frozen(position[0], position[1]), []byte(text)) {
			t.Fatalf("focus vanished at step/rev %v", position)
		}
	}
	if bytes.Contains(f.frozen(4, 3), []byte(text)) {
		t.Fatal("reset retained old focus body")
	}
	if !bytes.Equal(f.model.sent[3], f.model.sent[4]) {
		t.Fatal("same-step resend changed bytes")
	}
	p3, p4, repair := f.model.prompts[2], f.model.prompts[3], f.model.prompts[4]
	prefix3, prefix4 := strings.Join(p3.Blocks[:p3.Stable], ""), strings.Join(p4.Blocks[:p4.Stable], "")
	if !strings.HasPrefix(prefix4, prefix3) || prefix4 != strings.Join(repair.Blocks[:repair.Stable], "") {
		t.Fatal("receipts did not append or repair changed stable prefix")
	}
	if strings.Contains(prefix4, text) {
		t.Fatal("replaceable focus entered stable prefix")
	}
	current, err := f.store.Context(context.Background(), focusSnapshot(t, f))
	if err != nil || current.AnchorSeq == 0 || len(current.Focus) != 0 {
		t.Fatalf("reset did not release focus: %+v %v", current, err)
	}
}

func TestLoopExhaustedOverflowSettlesUsageWithoutNewAnchor(t *testing.T) {
	imported := t.TempDir()
	if err := os.WriteFile(filepath.Join(imported, "source"), []byte("latest body"), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, core.Limits{MaxSteps: 5, MaxAttempts: 1}, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/source","reason":"capture"}`),
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
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/source","reason":"capture"}`),
		execReply("canti:unrelated"), execReply(fmt.Sprintf(`canti:retrieve {"operation":"read","path":%q,"reason":"recover exact snapshot"}`, target)), returnReply("done"))
	f.cfg.Reader = &archive.Reader{Imports: imported, History: f.cfg.History}
	f.steps(f.loop(), 11)
	before := focusSnapshot(t, f)
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
	if got := focusSnapshot(t, f); got != before {
		t.Fatalf("cancelled repair committed progress: %+v", got)
	}
	if after := tree(t, stepDir); !reflect.DeepEqual(priorFiles, after) {
		t.Fatalf("cancelled repair published files: before=%v after=%v", priorFiles, after)
	}
	if journal.recentReads != 0 || journal.receiptReads != 0 {
		t.Fatal("cancelled repair scanned unrelated history")
	}
}
