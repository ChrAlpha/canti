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

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
)

func TestContextDiagnosticAcrossExecutionRetrievalCompactionAndRestart(t *testing.T) {
	f, image := dockerFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "Earlier answer: verify original evidence."}})
	if r := f.cli(nil, "run", "-session", "diagnostic", "-input", "first", "-goal", "prior goal", "-image", image); r.code != exitOK {
		t.Fatalf("prior: %+v", r)
	}
	priorID := f.status().Run.ID
	if r := f.cli(nil, "run", "-once", "-session", "diagnostic", "-input", "second", "-goal", "verify evidence and create done.txt", "-image", image, "-compact-steps", "2", "-retain-bytes", "1", "-max-steps", "8", "-wall-time", "2m", "-job-timeout", "10s"); r.code != exitActive {
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
	answer(3, 2, core.Exec, "test \"$(cat proof.txt)\" = evidence && printf verified > done.txt")

	answer(4, 1, core.Return, "Verified evidence and created done.txt.")
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
		if report.AsOfSeq != before[len(before)-1].Seq || report.Request.Seq != tr.Seq || !report.Request.CaptureAvailable || report.SessionID != "diagnostic" || report.SessionSeq != 2 {
			t.Fatalf("identity: %+v", report)
		}
		capture := report.Request.Capture
		if tr.State.Step < 3 && (len(capture.Session) != 1 || capture.Session[0].RunID != priorID) {
			t.Fatalf("session provenance: %+v", capture.Session)
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
	if len(reports) != 5 {
		t.Fatalf("frozen revisions: %d", len(reports))
	}
	requested := reports[2].Request.Capture
	if requested.CheckpointID == "" || len(requested.Sources) != 0 || requested.Retrievals[0].Step != 2 || requested.Retrievals[0].Operation != "read" || requested.Retrievals[0].Query != nil {
		t.Fatalf("compaction/evidence: %+v", requested)
	}
	if reports[3].Request.Capture.Compaction.Status != "request_revision" || reports[4].Request.Window.FirstStep != 3 || reports[4].Delta.WindowChanged == nil || !*reports[4].Delta.WindowChanged || len(reports[4].Request.Capture.Sources) != 0 {
		t.Fatal("repair or next window was misrepresented")
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
	t.Logf("SCRIPTED_CONTEXT requests=5 windows=3 transport_retries=1 protocol_repairs=1 business_result=verified; real CLI/SQLite/Docker, process-per-transition resume")
}
