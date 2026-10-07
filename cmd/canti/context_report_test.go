package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestContextTextUsesBoundedRetrievalOperation(t *testing.T) {
	for _, operation := range []string{"read", "list", "search", "discover", strings.Repeat("\"\\\n界", 1<<17)} {
		for _, legacy := range []bool{false, true} {
			r := harness.ContextRetrieval{Step: 7, Operation: operation, More: true, Incomplete: true}
			if legacy {
				r.Operation = ""
				r.Query = &archive.RetrievalQuery{Operation: operation, Path: "/canti/imports/legacy", Reason: strings.Repeat("REASON", 1<<17)}
			}
			report := contextReport{RunID: "original-run", Request: harness.ContextRequest{Capture: &harness.ContextCapture{Retrievals: []harness.ContextRetrieval{r}}}}
			var out bytes.Buffer
			if err := report.writeText(&out); err != nil {
				t.Fatal(err)
			}
			want := operation
			if len(want) > 8 {
				want = "invalid"
			}
			if out.Len() > 1024 || !strings.Contains(out.String(), "Retrieval at step 7: "+want+"; original query in observation; more true; incomplete true") || strings.Contains(out.String(), "REASON") || strings.Contains(out.String(), "/canti/imports/legacy") {
				t.Fatalf("legacy %t retrieval text = %q", legacy, out.String())
			}
		}
	}
}

func TestContextTextNamesTheHandoverPathOfEarlierDialogue(t *testing.T) {
	report := contextReport{RunID: "current", Request: harness.ContextRequest{Capture: &harness.ContextCapture{Session: []harness.ContextSessionSource{
		{RunID: "r1", SessionSeq: 1, Status: "returned", IncludedGoal: true},
		{RunID: "r2", SessionSeq: 2, Status: "stopped", HandoverStep: 7},
	}}}}
	var out bytes.Buffer
	if err := report.writeText(&out); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"Earlier dialogue: run r1 sequence 1, returned, goal included true\n",
		"Earlier dialogue: run r2 sequence 2, stopped, goal included false, latest handover step 7\n"} {
		if !strings.Contains(out.String(), line) {
			t.Fatalf("report lacks %q:\n%s", line, out.String())
		}
	}
}

func TestContextInspectFrozenRequest(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-goal", "inspect me"); r.code != exitOK {
		t.Fatalf("run: %+v", r)
	}
	before := f.history()
	r := f.cli(nil, "context")
	if r.code != exitOK {
		t.Fatalf("context: %+v", r)
	}
	var report struct {
		AsOf    int64 `json:"as_of_seq"`
		Request struct {
			Step             int  `json:"step"`
			Revision         int  `json:"revision"`
			BodyBytes        int  `json:"body_bytes"`
			CaptureAvailable bool `json:"capture_available"`
		} `json:"request"`
		Events []struct {
			Kind string `json:"kind"`
		} `json:"events"`
	}
	if err := json.Unmarshal([]byte(r.stdout), &report); err != nil {
		t.Fatal(err)
	}
	if report.AsOf != before[len(before)-1].Seq || report.Request.Step != 1 || report.Request.Revision != 1 || !report.Request.CaptureAvailable || len(report.Events) != 3 {
		t.Fatalf("unexpected report: %s", r.stdout)
	}
	raw := f.cli(nil, "context", "-raw")
	if raw.code != exitOK || raw.stdout != string(f.model.requests()[0]) || len(raw.stdout) != report.Request.BodyBytes {
		t.Fatalf("raw request changed: %+v", raw)
	}
	text := f.cli(nil, "context", "-text")
	if text.code != exitOK || !strings.Contains(text.stdout, "prompt bytes") || !strings.Contains(text.stdout, "request bytes") || !strings.Contains(text.stdout, "returned") {
		t.Fatalf("text: %+v", text)
	}
	if after := f.history(); !reflect.DeepEqual(before, after) || len(f.model.requests()) != 1 {
		t.Fatal("diagnostic changed journal or called provider")
	}
}

func TestContextMissingDatabaseDoesNotCreateState(t *testing.T) {
	f := newFixture(t, nil)
	r := f.cli(nil, "context")
	if r.code != exitError {
		t.Fatalf("context without database: %+v", r)
	}
	if _, err := os.Stat(filepath.Join(f.state, "canti.db")); !os.IsNotExist(err) {
		t.Fatalf("missing database created: %v", err)
	}
}

func TestContextRetryRepairAndPagination(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.model.answer(1, 1, reply{status: 503, body: "retry me"})
	f.model.answer(1, 2, reply{text: "invalid decision"})
	if r := f.cli(nil, "run", "-goal", "inspect repair"); r.code != exitOK {
		t.Fatalf("run: %+v", r)
	}
	first := f.cli(nil, "context", "-step", "1", "-revision", "1", "-limit", "2")
	var page contextReport
	if err := json.Unmarshal([]byte(first.stdout), &page); first.code != exitOK || err != nil {
		t.Fatalf("page: %+v, %v", first, err)
	}
	if !page.More || page.NextAfter == 0 || len(page.Events) != 2 || page.Events[1].Kind != "response_incomplete" || page.Events[1].Attempt != 1 || page.Next == nil || page.Next.Revision != 2 {
		t.Fatalf("retry page: %+v", page)
	}
	next := f.cli(nil, "context", "-run", page.RunID, "-step", "1", "-revision", "1", "-after", fmt.Sprint(page.NextAfter), "-limit", "100", "-responses")
	if err := json.Unmarshal([]byte(next.stdout), &page); next.code != exitOK || err != nil {
		t.Fatalf("next: %+v, %v", next, err)
	}
	if page.More || len(page.Events) != 3 || page.Events[0].Attempt != 2 || page.Events[1].Response == nil || len(page.Events[1].Response.Body) == 0 || page.Events[2].Outcome != "protocol_rejected" {
		t.Fatalf("repair page: %+v", page)
	}
	last := f.cli(nil, "context")
	if err := json.Unmarshal([]byte(last.stdout), &page); last.code != exitOK || err != nil {
		t.Fatalf("last: %+v, %v", last, err)
	}
	if page.Request.Revision != 2 || page.Previous == nil || page.Previous.Revision != 1 || page.Delta == nil || page.Delta.PromptBytes == nil || page.Delta.WindowChanged == nil || *page.Delta.WindowChanged || page.Events[0].Attempt != 3 || page.Request.Capture.Handover.Status != "request_revision" {
		t.Fatalf("revision report: %+v", page)
	}
}

func TestContextRejectsInvalidSelectors(t *testing.T) {
	for _, args := range [][]string{{"-step", "-1"}, {"-revision", "1"}, {"-limit", "101"}, {"-after", "-1"}, {"-raw", "-text"}, {"-text", "-responses"}} {
		f := newFixture(t, nil)
		if r := f.cli(nil, "context", args...); r.code != exitUsage {
			t.Fatalf("args %v: %+v", args, r)
		}
		if _, err := os.Stat(f.state); !os.IsNotExist(err) {
			t.Fatalf("invalid selector touched state: %v", err)
		}
	}
}

func TestContextInterruptedAttemptIsNotInvented(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-once", "-goal", "interrupt before sending", "-max-attempts", "1"); r.code != exitActive {
		t.Fatalf("freeze: %+v", r)
	}
	if r := f.cli([]string{"CANTI_TEST_CRASH=after:attempt_started"}, "resume"); r.code == exitOK {
		t.Fatalf("crash: %+v", r)
	}
	r := f.cli(nil, "context")
	var report contextReport
	if err := json.Unmarshal([]byte(r.stdout), &report); r.code != exitOK || err != nil {
		t.Fatalf("interrupted: %+v %v", r, err)
	}
	if len(report.Events) != 1 || report.Events[0].Outcome != "attempt_started" || report.Events[0].Response != nil || len(f.model.requests()) != 0 {
		t.Fatalf("invented response: %+v", report)
	}
	if r := f.cli(nil, "resume"); r.code != exitStopped {
		t.Fatalf("exhausted: %+v", r)
	}
	r = f.cli(nil, "context")
	if err := json.Unmarshal([]byte(r.stdout), &report); r.code != exitOK || err != nil {
		t.Fatalf("exhausted report: %+v %v", r, err)
	}
	last := report.Events[len(report.Events)-1]
	if last.Outcome != "attempt_not_started" || last.Attempt != 1 || last.RequestedAttempt != 2 || last.LostAttempt != 1 || len(f.model.requests()) != 0 {
		t.Fatalf("invented sent attempt: %+v", last)
	}
}

func TestContextContinuationRequiresPinnedRequest(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-goal", "first request"); r.code != exitOK {
		t.Fatalf("run: %+v", r)
	}
	runID := f.status().Run.ID
	for _, args := range [][]string{
		{"-after", "1"},
		{"-after", "1", "-run", runID},
		{"-after", "1", "-step", "1", "-revision", "1"},
		{"-after", "1", "-run", runID, "-step", "1"},
	} {
		if r := f.cli(nil, "context", args...); r.code != exitUsage {
			t.Fatalf("unpinned continuation %v: %+v", args, r)
		}
	}
}

func TestContextTextPreservesStopReason(t *testing.T) {
	f := newFixture(t, nil)
	if r := f.cli(nil, "run", "-once", "-goal", "stop before model call"); r.code != exitActive {
		t.Fatalf("run: %+v", r)
	}
	if r := f.cli(nil, "stop"); r.code != exitStopped {
		t.Fatalf("stop: %+v", r)
	}
	if r := f.cli(nil, "context", "-text"); r.code != exitOK || !strings.Contains(r.stdout, "cancelled") {
		t.Fatalf("missing durable stop reason: %+v", r)
	}
}

// TestContextNamesTheDecoderOfTheRunsOwnRulesOnly requires that context
// gives a rejected response the current decoder's diagnostic only for a run
// of the current context version, whose rules include the decoder's
// diagnostics; for a run of another version the rejection keeps no cause.
func TestContextNamesTheDecoderOfTheRunsOwnRulesOnly(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.model.answer(1, 1, reply{text: `{"kind":"exec","purpose":1,"payload":"true"}`})
	if r := f.cli(nil, "run", "-goal", "inspect repair"); r.code != exitOK {
		t.Fatalf("run: %+v", r)
	}
	id := f.status().Run.ID
	rejection := func() harness.ContextEvent {
		t.Helper()
		r := f.cli(nil, "context", "-step", "1", "-revision", "1")
		var page contextReport
		if err := json.Unmarshal([]byte(r.stdout), &page); r.code != exitOK || err != nil {
			t.Fatalf("context: %+v, %v", r, err)
		}
		for _, e := range page.Events {
			if e.Kind == "response_loaded" {
				return e
			}
		}
		t.Fatalf("no loaded response in %+v", page)
		return harness.ContextEvent{}
	}
	if e := rejection(); e.Outcome != "protocol_rejected" || e.Failure != "core: invalid decision: purpose must be a string" {
		t.Fatalf("current run: %+v", e)
	}
	setContextVersion(t, f.state, id, strconv.Itoa(contextVersion-1))
	if e := rejection(); e.Outcome != "protocol_rejected" || e.Failure != "" {
		t.Fatalf("run of version %d: %+v", contextVersion-1, e)
	}
}
