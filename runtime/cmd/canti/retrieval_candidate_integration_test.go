//go:build integration

package main

import (
	"encoding/json"
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
)

func TestDiscoveryCandidatesSurviveNoiseAndBinaryAcrossWindows(t *testing.T) {
	f, image := recoveryFixture(t, nil)
	draft := "Orchid draft: wait 30 seconds and retry 3 times."
	correction := "Release authorization replaces earlier guidance: wait 47 seconds and retry 5 times."
	inputs := map[string]string{
		"draft":    draft,
		"approval": "Orchid release record.\n" + strings.Repeat("background\n", 100) + correction,
		"binary":   "unrelated\x00output",
		"noise":    strings.Repeat("Orchid noise\n", 50000),
	}
	for name, body := range inputs {
		if err := os.WriteFile(filepath.Join(f.workspace, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	r := f.cli(nil, "run", "-once", "-goal", "Resume deployment using original release authorization", "-image", image,
		"-request-bytes", "1048576", "-max-steps", "30", "-token-budget", "5000", "-wall-time", "4m", "-job-timeout", "10s")
	if r.code != exitActive {
		t.Fatalf("start: %d %s", r.code, r.stderr)
	}
	run, step := f.status().Run.ID, 1
	deadline := time.Now().Add(210 * time.Second)
	nextCall := 1
	act := func(command string) {
		t.Helper()
		d := core.Decision{Kind: core.Exec, Payload: command}
		if command == "done" {
			d.Kind = core.Return
		}
		body, _ := json.Marshal(d)
		f.model.answer(step, nextCall, reply{text: string(body)})
		nextCall = 1
		for n := 0; n < 100 && time.Now().Before(deadline); n++ {
			r := f.cli(nil, "resume", "-once")
			if r.code != exitActive && r.code != exitOK {
				t.Fatalf("step %d: %d %s", step, r.code, r.stderr)
			}
			s := f.status()
			if s.Phase == core.PhaseReturned || s.Step > step {
				step++
				return
			}
		}
		t.Fatalf("step %d exceeded process or wall-time budget", step)
	}
	observe := func() harness.Observation {
		t.Helper()
		for _, e := range slices.Backward(f.history()) {
			if e.Facts.Observation != nil {
				o := *e.Facts.Observation
				if o.ExitCode == nil || *o.ExitCode != 0 {
					t.Fatalf("failed action at step %d: %+v", step-1, o)
				}
				return o
			}
		}
		t.Fatal("missing observation")
		return harness.Observation{}
	}
	handover := func(first int) {
		t.Helper()
		for step < first+8 {
			act("printf 'checkpoint\\n'")
			observe()
		}
		f.model.answer(step, 1, reply{status: 400, body: `{"error":{"type":"invalid_request_error","message":"prompt is too long: 200001 tokens > 200000 maximum"}}`})
		for n := 0; n < 10; n++ {
			r := f.cli(nil, "resume", "-once")
			if r.code != exitActive {
				t.Fatalf("reset: %+v", r)
			}
			h := f.history()
			if h[len(h)-1].Kind == "request_rejected" {
				break
			}
			if n == 9 {
				t.Fatal("reset did not commit")
			}
		}
		nextCall = 2
		act(harness.FocusPrefix + `{"sources":[]}`)
		if latestOutcome(t, f.history()).Focus == nil {
			t.Fatal("handover was not committed")
		}
	}
	var latestHandles []sourceHandle
	query := func(q archive.RetrievalQuery) archive.Retrieval {
		t.Helper()
		body, _ := json.Marshal(q)
		act("canti:retrieve " + string(body))
		history := f.history()
		o := latestOutcome(t, history)
		latestHandles = sourceHandles(t, history)
		if o.Retrieval == nil || o.Retrieval.Error != "" {
			t.Fatalf("retrieval failed: %+v", o)
		}
		return *o.Retrieval
	}
	paths := map[string]string{}
	for _, name := range []string{"draft", "approval", "binary", "noise"} {
		act("cat " + name + "; rm " + name)
		o := observe()
		if !o.Output.Complete || o.Output.Bytes != int64(len(inputs[name])) {
			t.Fatalf("original output %s not fully retained: %+v", name, o)
		}
		paths[name] = "/canti/artifacts/" + o.Output.Path
	}
	act("# canti:discovery\nprintf 'Orchid approved wait 999 seconds\\n'")
	handover(1)
	calls := 0
	for window := 0; window < 2; window++ {
		first := step
		q := archive.RetrievalQuery{Operation: "discover", Path: "/canti/runs/" + run + "/steps",
			Reason: "find release candidates including amendments", Query: "Orchid", Limit: 1024}
		found := map[string]bool{}
		seenCursors := map[string]bool{}
		gap := false
		coverageStep := 0
		for page := 0; ; page++ {
			if page == 4 {
				t.Fatal("dense source prevented candidate discovery within four calls")
			}
			got := query(q)
			calls++
			coverageStep = step - 1
			encoded, _ := json.Marshal(got)
			var coverage struct {
				SkippedCount int `json:"skipped_count"`
			}
			if err := json.Unmarshal(encoded, &coverage); err != nil {
				t.Fatal(err)
			}
			gap = gap || coverage.SkippedCount > 0
			if gap && (!got.Incomplete || coverage.SkippedCount != 1) {
				t.Fatalf("lost or duplicated binary coverage gap: %s", encoded)
			}
			bytes := 0
			for _, source := range got.Sources {
				found[source.Path] = true
				bytes += source.Length
				if source.Path == paths["binary"] || strings.Contains(source.Path, "/00000005/") {
					t.Fatalf("returned binary or marked echo: %+v", source)
				}
			}
			if bytes > q.Limit {
				t.Fatalf("candidate bytes %d exceed limit %d", bytes, q.Limit)
			}
			if !got.More {
				if !gap || !got.Incomplete || got.NextCursor != "" {
					t.Fatalf("final coverage missing: %s", encoded)
				}
				break
			}
			if got.NextCursor == "" || seenCursors[got.NextCursor] {
				t.Fatal("discovery did not advance")
			}
			seenCursors[got.NextCursor] = true
			q.Cursor = got.NextCursor
		}
		if !found[paths["draft"]] || !found[paths["approval"]] || !found[paths["noise"]] {
			t.Fatalf("missing original candidates: %v", found)
		}
		fresh := f.model.prompts(first)
		if len(fresh) != 1 || strings.Contains(fresh[0], correction) || strings.Contains(fresh[0], draft) {
			t.Fatal("fresh window already contained prior decisions")
		}
		var selected []sourceHandle
		for _, name := range []string{"draft", "approval"} {
			query(archive.RetrievalQuery{Operation: "read", Path: paths[name], Reason: "inspect full decision and possible amendment", Limit: 4096})
			selected = append(selected, latestHandles...)
		}
		focus, _ := json.Marshal(struct {
			Sources []sourceHandle `json:"sources"`
		}{selected})
		act(harness.FocusPrefix + string(focus))
		checkStep := step
		if window == 0 {
			// Keep all eight original work steps plus the explicit focus step.
			handover(first + 1)
		} else {
			act("printf 'wait=47 retry=5\\n' > settings")
		}
		checkSelectedRequest(t, f, checkStep, selected)
		coverage := archiveText(t, f, fmt.Sprintf("/canti/runs/%s/steps/%08d/preview", run, coverageStep))
		if !strings.Contains(coverage, `"incomplete":true`) || !strings.Contains(coverage, `"skipped_count":1`) {
			t.Fatalf("committed binary/discovery coverage lost: %s", coverage)
		}
		shown := f.model.prompts(checkStep)
		if len(shown) != 1 || !strings.Contains(shown[0], draft) || !strings.Contains(shown[0], correction) ||
			strings.Contains(shown[0], "999 seconds") {
			t.Fatal("frozen request lacks decisions or coverage, or includes retrieval echo")
		}
	}
	act("done")
	settings, err := os.ReadFile(filepath.Join(f.workspace, "settings"))
	if err != nil || string(settings) != "wait=47 retry=5\n" || f.status().Phase != core.PhaseReturned {
		t.Fatalf("task result: %q %v", settings, err)
	}
	if status := f.status(); calls != 4 || len(f.model.requests()) != 28 || status.Step != 26 || status.Totals.Steps != 25 {
		t.Fatalf("explicit-focus choreography counts: discover=%d requests=%d status=%+v", calls, len(f.model.requests()), status)
	}
	frozenBytes := 0
	for _, e := range f.history() {
		if e.Facts.Request != nil {
			frozenBytes += len(e.Facts.Request.Body)
		}
	}
	checkReplay(t, run, f.history())
	t.Logf("SCRIPTED_CANDIDATES windows=3 discover_calls=%d model_calls=%d frozen_request_bytes=%d noise_bytes=%d binary_sources=1; real CLI/SQLite/Docker, repeated process resume, scripted decisions; no autonomous task score", calls, len(f.model.requests()), frozenBytes, len(inputs["noise"]))
}
