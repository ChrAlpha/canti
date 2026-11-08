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
)

func TestWindowHandoverRecoversAndDoesNotCarryOldEvidence(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"check source"}`),
		execReply(harness.HandoverPrefix+"Continue at the evidence reference; verify before use."),
		reply{err: errors.New("transport interrupted")}, returnReply("done"))
	imported := t.TempDir()
	if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte("ORIGINAL_EVIDENCE_SENTINEL"), 0600); err != nil {
		t.Fatal(err)
	}
	f.cfg.Reader = &archive.Reader{Imports: imported}
	f.cfg.WindowSteps = 1
	ctx := context.Background()
	for {
		snap, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.State.Intent() == core.IntentHalt {
			break
		}
	}
	if len(f.model.prompts) != 3 || len(f.model.sent) != 4 {
		t.Fatalf("prompts=%d sends=%d", len(f.model.prompts), len(f.model.sent))
	}
	if !strings.Contains(f.model.prompts[1].User(), "ORIGINAL_EVIDENCE_SENTINEL") {
		t.Fatal("retrieved evidence did not enter actual request")
	}
	last := f.model.prompts[2].User()
	if strings.Contains(last, "ORIGINAL_EVIDENCE_SENTINEL") || strings.Contains(last, "### Step 1") || strings.Contains(last, "## Notes") {
		t.Fatalf("old context leaked: %s", last)
	}
	if !strings.Contains(last, "Continue at the evidence reference") {
		t.Fatal("handover missing")
	}
	if !bytes.Equal(f.model.sent[2], f.model.sent[3]) {
		t.Fatal("retry changed frozen window bytes")
	}
	req, err := f.store.LatestRequest(ctx, runID)
	if err != nil || req.Window == nil || req.Window.FirstStep != 3 || req.Window.SourceStep != 2 {
		t.Fatalf("window: %#v %v", req.Window, err)
	}
	if _, err := os.Stat(filepath.Join(f.cfg.History, runID, "steps", "00000002", "preview")); err != nil {
		t.Fatal("last old-page step not published", err)
	}
}

func TestHandoverTakesTheTextAfterItsPrefixVerbatim(t *testing.T) {
	text := "  Objective: deploy \"gateway\" for <service> {stage 3}.\nNext: run C:\\tools\\check; 继续核对。\n" +
		"Evidence: /canti/artifacts/effect_" + strings.Repeat("a", 64) + "/output.\n\n"
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
		execReply("true"), execReply(harness.HandoverPrefix+text), returnReply("done"))
	f.cfg.WindowSteps = 1
	ctx := f.bounded()
	s, err := f.loop().Run(ctx, runID)
	if err != nil || s.State.Phase != core.PhaseReturned {
		t.Fatalf("run=%+v err=%v", s.State, err)
	}
	req, err := f.store.LatestRequest(ctx, runID)
	if err != nil || req.Window == nil || req.Window.FirstStep != 3 || req.Window.SourceStep != 2 || req.Window.Handover != text {
		t.Fatalf("window=%+v err=%v", req.Window, err)
	}
	steps, err := f.store.Recent(ctx, runID, 0, 1)
	if err != nil || len(steps) != 1 {
		t.Fatalf("steps=%+v err=%v", steps, err)
	}
	if o := steps[0].Outcome; o == nil || o.Status != harness.OutcomeDone || o.Handover == nil || !reflect.DeepEqual(*o.Handover, harness.Handover{RunID: runID, FirstStep: 1, Text: text}) {
		t.Fatalf("recorded outcome %+v", o)
	}
	if w := steps[0].Window; w == nil || !w.Equal(harness.Window{FirstStep: 3, SourceStep: 2, Handover: text}) {
		t.Fatalf("the handover committed the window %+v", w)
	}
	if !strings.Contains(f.model.prompts[2].User(), text) {
		t.Fatal("the next window does not quote the handover text")
	}
}

func TestHandoverFollowsItsNameAfterAnyWhitespace(t *testing.T) {
	for _, separator := range []string{"\n", "\t", "\r", " "} {
		f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
			execReply("true"), execReply("canti:handover"+separator+"Next: verify the build.\n"), returnReply("done"))
		f.cfg.WindowSteps = 1
		s, err := f.loop().Run(f.bounded(), runID)
		if err != nil || s.State.Phase != core.PhaseReturned {
			t.Fatalf("separator %q: run=%+v err=%v", separator, s.State, err)
		}
		req, err := f.store.LatestRequest(f.bounded(), runID)
		if err != nil || req.Window == nil || req.Window.Handover != "Next: verify the build.\n" {
			t.Fatalf("separator %q: window=%+v err=%v", separator, req.Window, err)
		}
	}
}

func TestALongerActionNameIsNotAHandover(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
		execReply("true"), execReply("canti:handovers Next: verify."), execReply("canti:handoverNext: verify."))
	f.cfg.WindowSteps = 1
	s, err := f.loop().Run(f.bounded(), runID)
	if err != nil || s.State.Reason != core.StopUnrecoverable {
		t.Fatalf("run=%+v err=%v", s.State, err)
	}
	steps, err := f.store.Recent(f.bounded(), runID, 0, 2)
	if err != nil || len(steps) != 2 {
		t.Fatalf("steps=%+v err=%v", steps, err)
	}
	for _, step := range steps {
		if o := step.Outcome; o == nil || o.Status != harness.OutcomeRefused || o.Handover != nil {
			t.Fatalf("step %d outcome %+v; want refused work", step.Step, o)
		}
	}
}

func TestUnrequestedHandoverNeverRunsAsACommand(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 4, MaxAttempts: 3}, harness.Budget{},
		execReply("canti:handover\ntouch ran; next: report"), execReply("canti:handover"), returnReply("done"))
	if _, err := f.loop().Run(f.bounded(), runID); err != nil {
		t.Fatal(err)
	}
	if len(f.exec.jobs) != 0 {
		t.Fatalf("a handover reached the executor: %v", f.exec.calls)
	}
	steps, err := f.store.Recent(f.bounded(), runID, 0, 2)
	if err != nil || len(steps) != 2 {
		t.Fatalf("steps=%+v err=%v", steps, err)
	}
	for _, s := range steps {
		if o := s.Outcome; o == nil || o.Status != harness.OutcomeFailed || o.Handover != nil || s.Window != nil ||
			!strings.Contains(o.Detail, "handover was not requested for this step") {
			t.Fatalf("step %d outcome %+v, window %+v", s.Step, o, s.Window)
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
	f.cfg.WindowSteps = 1
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
	f.cfg.WindowSteps = 1
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

func TestWindowRefusesRepeatedInvalidHandover(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{}, execReply("true"),
		execReply("canti:handover"),
		execReply(harness.HandoverPrefix+strings.Repeat("界", 683)))
	f.cfg.WindowSteps = 1
	s, err := f.loop().Run(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if s.State.Reason != core.StopUnrecoverable {
		t.Fatalf("state %#v", s.State)
	}
	req, err := f.store.LatestRequest(context.Background(), runID)
	if err != nil || req.Window.FirstStep != 1 {
		t.Fatalf("discarded old page: %#v %v", req.Window, err)
	}
	steps, err := f.store.Recent(context.Background(), runID, 0, 2)
	if err != nil || len(steps) != 2 {
		t.Fatalf("steps=%+v err=%v", steps, err)
	}
	for i, want := range []string{"handover text must not be empty", "2049 UTF-8 bytes; maximum 2048"} {
		if o := steps[i].Outcome; o == nil || o.Handover != nil || !strings.Contains(o.Detail, want) {
			t.Fatalf("step %d outcome %+v, want %q", steps[i].Step, o, want)
		}
		if w := steps[i].Window; w == nil || w.FirstStep != 1 || !w.Requested || w.Failures != i+1 {
			t.Fatalf("step %d committed the window %+v, want the active window with %d failures", steps[i].Step, w, i+1)
		}
	}
}

func TestZeroTriggersRequestAndAdvanceHandoverAtCapacity(t *testing.T) {
	var replies []reply
	for i := 0; i < 24; i++ {
		replies = append(replies, execReply("echo CAPACITY_PAGE_MARKER "+strings.Repeat("x", 1800)))
	}
	f := newFixture(t, core.Limits{MaxSteps: 30, MaxAttempts: 3}, harness.Budget{}, replies...)
	f.cfg.Limits = harness.DefaultPromptLimits()
	f.cfg.Limits.Total = 96 << 10
	if f.cfg.WindowSteps != 0 || f.cfg.WindowBytes != 0 {
		t.Fatalf("default triggers = %d steps, %d bytes", f.cfg.WindowSteps, f.cfg.WindowBytes)
	}
	ctx := f.bounded()
	var handoverStep int
	for i := 0; i < 250; i++ {
		snap, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.State.Phase != core.PhaseRequestFrozen {
			continue
		}
		req, err := f.store.Request(ctx, runID, snap.State.Step, snap.State.RequestRev)
		if err != nil || req.Window == nil {
			t.Fatalf("frozen request has no window: %+v %v", req, err)
		}
		if req.Window.Requested {
			handoverStep = snap.State.Step
			if !strings.Contains(string(req.Body), "Handover required") || !strings.Contains(string(req.Body), "### Step 1") {
				t.Fatal("capacity trigger dropped current-page history before handover")
			}
			break
		}
	}
	if handoverStep < 2 {
		t.Fatal("automatic capacity trigger did not request a handover")
	}
	f.model.mu.Lock()
	f.model.replies = []reply{
		execReply(harness.HandoverPrefix + "Continue on the next page; inspect the old record if needed."),
		returnReply("done"),
	}
	f.model.mu.Unlock()
	snap, err := f.fresh().Run(ctx, runID)
	if err != nil || snap.State.Phase != core.PhaseReturned {
		t.Fatalf("run = %+v, %v", snap.State, err)
	}
	req, err := f.store.LatestRequest(ctx, runID)
	if err != nil || req.Window == nil || req.Window.FirstStep != handoverStep+1 || req.Window.SourceStep != handoverStep {
		t.Fatalf("advanced window = %+v, %v", req.Window, err)
	}
	if strings.Contains(string(req.Body), "CAPACITY_PAGE_MARKER") || !strings.Contains(string(req.Body), "Continue on the next page") {
		t.Fatal("new page inherited old steps or lost handover")
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

func TestRevisitingRetrievalAfterHandoverDoesNotExpandItsEvidence(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"verify original"}`),
		execReply(harness.HandoverPrefix+"Inspect step 1 access metadata."),
		execReply(`canti:retrieve {"operation":"read","path":"/canti/runs/run-1/steps/00000001/preview","reason":"inspect prior access"}`),
		execReply(harness.HandoverPrefix+"Finish."),
		returnReply("done"))
	imported := t.TempDir()
	const original = "UNIQUE_ORIGINAL_EVIDENCE_NOT_METADATA"
	if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	f.cfg.Reader = &archive.Reader{Imports: imported, History: f.cfg.History}
	f.cfg.WindowSteps = 1
	ctx := f.bounded()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Intent() == core.IntentHalt {
			break
		}
	}
	if len(f.model.sent) != 5 {
		t.Fatalf("sent %d requests", len(f.model.sent))
	}
	if !strings.Contains(string(f.model.sent[1]), original) {
		t.Fatal("initial retrieval evidence missing")
	}
	for _, request := range f.model.sent[2:] {
		if strings.Contains(string(request), original) {
			t.Fatal("original evidence recursively reappeared after handover")
		}
	}
	if !strings.Contains(string(f.model.sent[3]), "verify original") || !strings.Contains(string(f.model.sent[3]), "/canti/imports/evidence") {
		t.Fatal("revisited access did not expose original reason and source")
	}
}

func TestHandoverProtocolRepairPreservesFailureCountAndWindow(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
		execReply("true"),
		execReply(harness.HandoverPrefix),
		reply{body: "not a decision", usage: usage},
		execReply(harness.HandoverPrefix+"Continue from the verified source."),
		returnReply("done"))
	f.cfg.WindowSteps = 1
	ctx := f.bounded()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Intent() == core.IntentHalt {
			if s.State.Phase != core.PhaseReturned {
				t.Fatalf("unexpected halt: %+v", s.State)
			}
			break
		}
	}
	const last = "This step is the last chance to hand over"
	if req, err := f.store.Request(ctx, runID, 2, 1); err != nil || strings.Contains(string(req.Body), last) {
		t.Fatalf("the first handover request says it is the last: %v", err)
	}
	for _, revision := range []int{1, 2} {
		req, err := f.store.Request(ctx, runID, 3, revision)
		if err != nil || req.Window == nil || req.Window.FirstStep != 1 || req.Window.Failures != 1 || !req.Window.Requested {
			t.Fatalf("revision %d window=%+v err=%v", revision, req.Window, err)
		}
		if !strings.Contains(string(req.Body), last) {
			t.Fatalf("revision %d of the retry does not say it is the last chance", revision)
		}
	}
	req, err := f.store.LatestRequest(ctx, runID)
	if err != nil || req.Window == nil || req.Window.FirstStep != 4 || req.Window.SourceStep != 3 || req.Window.Failures != 0 {
		t.Fatalf("new window=%+v err=%v", req.Window, err)
	}
}

func TestHandoverPagePreservesRetryAndRepair(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 6, MaxAttempts: 3}, harness.Budget{},
		execReply("echo first-window-marker"),
		execReply(harness.HandoverPrefix+"Continue after marker; verify source."),
		reply{err: errors.New("transport interruption")},
		reply{body: "invalid decision", usage: usage},
		returnReply("done"))
	f.cfg.WindowSteps = 1
	ctx := f.bounded()
	s, err := f.fresh().Run(ctx, runID)
	if err != nil || s.State.Phase != core.PhaseReturned {
		t.Fatalf("run=%+v err=%v", s.State, err)
	}
	if len(f.model.sent) != 5 || !bytes.Equal(f.model.sent[2], f.model.sent[3]) {
		t.Fatal("transport retry changed frozen request")
	}
	for _, revision := range []int{1, 2} {
		req, err := f.store.Request(ctx, runID, 3, revision)
		if err != nil || req.Window == nil || req.Window.FirstStep != 3 || req.Window.SourceStep != 2 || req.Window.Handover == "" {
			t.Fatalf("revision %d window=%+v err=%v", revision, req.Window, err)
		}
		if strings.Contains(string(req.Body), "first-window-marker") || !strings.Contains(string(req.Body), "Continue after marker") {
			t.Fatalf("revision %d leaks old page or loses handover", revision)
		}
	}
	if !strings.Contains(string(f.model.sent[4]), "Rejected response") {
		t.Fatal("protocol repair missing")
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
		execReply(harness.HandoverPrefix+"Continue with the next operation."))
	f.cfg.WindowSteps = 1
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
	f.cfg.WindowSteps = 1
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

// capturing records the first transition of kind handed to its journal.
type capturing struct {
	harness.Journal
	kind string
	got  *harness.Transition
}

func (j capturing) Commit(ctx context.Context, tr harness.Transition) error {
	if tr.Event.Kind() == j.kind && j.got.Event == nil {
		*j.got = tr
	}
	return j.Journal.Commit(ctx, tr)
}

// TestHandoverAcknowledgementLossCommitsOneIdenticalTransition requires that
// when the performed commit of a handover lands without its acknowledgement
// the next step keeps it, and that when it never landed the handover is
// performed again into the identical transition: its outcome, window, and
// progress, as a local change computed from committed facts.
func TestHandoverAcknowledgementLossCommitsOneIdenticalTransition(t *testing.T) {
	for _, landed := range []bool{true, false} {
		t.Run(map[bool]string{true: "landed", false: "lost"}[landed], func(t *testing.T) {
			f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
				execReply("true"), execReply(harness.HandoverPrefix+"Next: report."), returnReply("done"))
			f.cfg.WindowSteps = 1
			ctx := f.bounded()
			f.stepUntil(ctx, core.PhaseIdle)
			f.stepUntil(ctx, core.PhaseEffectSaved)
			var first, committed harness.Transition
			var failed bool
			if _, err := f.loopOn(capturing{failingCommit{f.store, "performed", landed, &failed}, "performed", &first}).Step(ctx, runID); err == nil {
				t.Fatal("the lost acknowledgement was not surfaced")
			}
			wantState(t, f.run(f.loopOn(capturing{f.open(), "performed", &committed})), core.PhaseReturned, "")
			performed := slices.DeleteFunc(f.transitions(), func(tr harness.Transition) bool { return tr.Event.Kind() != "performed" })
			if len(performed) != 1 || !reflect.DeepEqual(performed[0].Facts, first.Facts) || performed[0].To != first.To {
				t.Fatalf("committed %+v, first attempted %+v", performed, first)
			}
			if !landed && !reflect.DeepEqual(committed.Facts, first.Facts) {
				t.Fatalf("the handover performed again into %+v, not %+v", committed.Facts, first.Facts)
			}
			if w := performed[0].Facts.Window; w == nil || !w.Equal(harness.Window{FirstStep: 3, SourceStep: 2, Handover: "Next: report."}) {
				t.Fatalf("window %+v", w)
			}
		})
	}
}

// tamperedWindow reports the window of the latest completed step as want.
type tamperedWindow struct {
	harness.Journal
	want *harness.Window
}

func (j tamperedWindow) Recent(ctx context.Context, runID string, before int64, limit int) ([]harness.StepRecord, error) {
	steps, err := j.Journal.Recent(ctx, runID, before, limit)
	if err == nil && len(steps) > 0 {
		steps[len(steps)-1].Window = j.want
	}
	return steps, err
}

// TestPreparationRefusesAWindowItsRequestDoesNotExplain requires that a
// preparation that reads a completion whose committed window does not follow
// from the window of its request, or that lacks the window a request
// requiring a handover implies, stops the run unrecoverably without a
// request.
func TestPreparationRefusesAWindowItsRequestDoesNotExplain(t *testing.T) {
	for name, tc := range map[string]struct {
		requested bool
		window    *harness.Window
	}{
		"window without a handover":    {false, &harness.Window{FirstStep: 2, SourceStep: 1, Handover: "forged"}},
		"failure without a request":    {false, &harness.Window{FirstStep: 1, Requested: true, Failures: 1}},
		"requested without its window": {true, nil},
	} {
		t.Run(name, func(t *testing.T) {
			second := execReply("echo next")
			if tc.requested {
				second = execReply("echo refused")
			}
			f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{}, execReply("true"), second, returnReply("done"))
			if tc.requested {
				f.cfg.WindowSteps = 1
			}
			ctx := f.bounded()
			f.stepUntil(ctx, core.PhaseIdle)
			if tc.requested {
				f.stepUntil(ctx, core.PhaseIdle)
			}
			s, err := f.loopOn(tamperedWindow{f.store, tc.window}).Step(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
			wantState(t, s, core.PhaseStopped, core.StopUnrecoverable)
			if last := f.last(); !strings.Contains(last.Facts.Failure, "does not follow from its request") {
				t.Fatalf("stop failure %q", last.Facts.Failure)
			}
		})
	}
}
