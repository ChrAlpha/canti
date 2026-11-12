//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestContextDiagnosticAcrossExecutionRetrievalHandoverAndRestart(t *testing.T) {
	f, image := dockerFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "Earlier answer: verify original evidence."}})
	if r := f.cli(nil, "run", "-input", "first", "-goal", "prior goal", "-image", image); r.code != exitOK {
		t.Fatalf("prior: %+v", r)
	}
	priorID := f.status().Run.ID
	if r := f.cli(nil, "run", "-once", "-input", "second", "-goal", "verify evidence and create done.txt", "-image", image, "-max-steps", "8", "-wall-time", "2m", "-job-timeout", "10s"); r.code != exitActive {
		t.Fatalf("freeze: %+v", r)
	}
	runID := f.status().Run.ID
	answer := func(step, call int, kind core.Kind, payload string) {
		body, err := json.Marshal(core.Decision{Kind: kind, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		f.model.answer(step, call, reply{text: string(body)})
	}
	answer(1, 2, core.Exec, "printf evidence > proof.txt; cat proof.txt")
	f.model.answer(2, 1, reply{status: 503, body: "temporary scripted failure"})
	query, _ := json.Marshal(archive.RetrievalQuery{Operation: "read", Path: "/canti/runs/" + runID + "/steps/00000001/preview", Reason: "verify original evidence"})
	answer(2, 2, core.Exec, "canti:retrieve "+string(query))
	f.model.answer(3, 1, reply{text: "invalid decision"})
	answer(3, 2, core.Exec, harness.FocusPrefix+`{"sources":[{"seq":12,"source":0}]}`)
	f.model.answer(4, 1, reply{status: 400, body: `{"error":{"type":"invalid_request_error","message":"prompt is too long: 200001 tokens > 200000 maximum"}}`})
	answer(4, 2, core.Exec, "test \"$(cat proof.txt)\" = evidence && printf verified > done.txt")
	answer(5, 1, core.Return, "Verified evidence and created done.txt.")
	deadline := time.Now().Add(100 * time.Second)
	completed := false
	for i := 0; i < 60 && time.Now().Before(deadline); i++ {
		r := f.cli(nil, "resume", "-once")
		if r.code == exitOK {
			completed = true
			break
		}
		if r.code != exitActive {
			t.Fatalf("resume: %+v", r)
		}
	}
	if !completed {
		t.Fatal("task did not complete within 60 transitions / 100 seconds")
	}
	if body, err := os.ReadFile(filepath.Join(f.workspace, "done.txt")); err != nil || string(body) != "verified" {
		t.Fatalf("business result: %q %v", body, err)
	}
	before := f.history()
	calls := len(f.model.requests())
	var reports []contextReport
	for _, tr := range before {
		if tr.Facts.Request == nil {
			continue
		}
		args := []string{"-run", runID, "-step", fmt.Sprint(tr.State.Step), "-revision", fmt.Sprint(tr.State.RequestRev), "-limit", "100"}
		r := f.cli(nil, "context", args...)
		var report contextReport
		if err := json.Unmarshal([]byte(r.stdout), &report); r.code != exitOK || err != nil {
			t.Fatalf("inspect: %+v %v", r, err)
		}
		if report.AsOfSeq != before[len(before)-1].Seq || report.Request.Seq != tr.Seq || !report.Request.CaptureAvailable || report.InputID != "second" || report.InputPos == 0 {
			t.Fatalf("identity: %+v", report)
		}
		capture := report.Request.Capture
		if capture.PreviousRun != priorID {
			t.Fatalf("previous-run provenance: %+v", capture.PreviousRun)
		}
		raw := f.cli(nil, "context", append(args, "-raw")...)
		if raw.code != exitOK || raw.stdout != string(tr.Facts.Request.Body) {
			t.Fatal("raw bytes differ from frozen request")
		}
		var envelope struct {
			System   string
			Messages []struct{ Content []struct{ Text string } }
		}
		if err := json.Unmarshal(tr.Facts.Request.Body, &envelope); err != nil {
			t.Fatal(err)
		}
		promptBytes := len(envelope.System)
		for _, block := range envelope.Messages[0].Content {
			promptBytes += len(block.Text)
		}
		if capture.Bytes.Total != promptBytes || promptBytes > capture.Bytes.Limit {
			t.Fatalf("byte accounting %d vs %d", capture.Bytes.Total, promptBytes)
		}
		reports = append(reports, report)
	}
	if len(reports) != 7 {
		t.Fatalf("frozen revisions: %d", len(reports))
	}
	requested := reports[2].Request.Capture
	if len(requested.Sources) != 1 || len(requested.Retrievals) != 1 || requested.Retrievals[0].Step != 2 {
		t.Fatalf("retrieval provenance=%+v", requested)
	}
	if reports[3].Request.Capture.Preparation != "protocol_repair" || len(reports[4].Request.Capture.Focus) != 1 || reports[5].Request.Anchor.FirstStep != 4 || reports[5].Delta.AnchorChanged == nil || !*reports[5].Delta.AnchorChanged || len(reports[5].Request.Capture.Sources) != 0 {
		t.Fatal("focus/repair/reset misrepresented")
	}
	if after := f.history(); !reflect.DeepEqual(before, after) || len(f.model.requests()) != calls {
		t.Fatal("inspection changed execution")
	}
	if err := os.RemoveAll(filepath.Join(f.state, "history")); err != nil {
		t.Fatal(err)
	}
	r := f.cli(nil, "context", "-step", "3", "-revision", "1", "-limit", "100")
	var afterDelete contextReport
	if err := json.Unmarshal([]byte(r.stdout), &afterDelete); r.code != exitOK || err != nil || !reflect.DeepEqual(afterDelete, reports[2]) {
		t.Fatalf("inspection depended on current history files: %+v %v", r, err)
	}
	if _, err := os.Stat(filepath.Join(f.state, "history")); !os.IsNotExist(err) {
		t.Fatal("inspection republished history")
	}
	t.Logf("SCRIPTED_CONTEXT requests=7 contexts=2 injected_provider_rejections=1 transport_retries=1 protocol_repairs=1 business_result=verified; real CLI/SQLite/Docker, process-per-transition resume")
}
