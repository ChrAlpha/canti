//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestRetrievalDiscoveryAcrossFreshPages(t *testing.T) {
	old := "Orchid timeout draft: 30 seconds; awaiting approval."
	noise := strings.Repeat("Orchid timeout font sample. ", 100)
	retry := "Orchid retry budget: approved maximum is 3 attempts."
	corrected := "Orchid timeout correction: " + strings.Repeat("background ", 32) + "approved wait is 45 seconds; supersedes the 30 second draft."
	output := func(s string) core.Decision {
		return core.Decision{Kind: core.Exec, Payload: "printf '%s\\n' '" + s + "'"}
	}
	f, image := recoveryFixture(t, map[int]core.Decision{
		1: output(old), 2: output(noise),
		3: {Kind: core.Exec, Payload: "# canti:discovery\nprintf '%s\\n' 'Orchid timeout stale search excerpt: approved 999 seconds'"},
		4: output(retry), 5: output(corrected),
		6: {Kind: core.Exec, Payload: `canti:retrieve {"operation":"list","path":"/canti/runs","reason":"Orchid discovery metadata"}`},
		7: {Kind: core.Return, Payload: "history ready"},
	})
	if r := f.cli(nil, "run", "-goal", "record deployment decisions", "-image", image); r.code != exitOK {
		t.Fatalf("seed run: %d %s", r.code, r.stderr)
	}
	sourceRun := f.status().Run.ID
	f.model = newScriptedModel(t, f.key, nil)
	goal := "Resume Orchid deployment from run " + sourceRun + "; recover the approved wait and retry budget, accounting for revised decisions."
	r := f.cli(nil, "run", "-once", "-goal", goal, "-image", image,
		"-max-steps", "33", "-token-budget", "4000", "-wall-time", "3m", "-job-timeout", "10s", "-output-limit", "2048", "-output-budget", "8192")
	if r.code != exitActive {
		t.Fatalf("start: %d %s", r.code, r.stderr)
	}
	step := 1
	deadline := time.Now().Add(150 * time.Second)
	act := func(d core.Decision) {
		t.Helper()
		body, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		f.model.answer(step, 1, reply{text: string(body)})
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
			if e.Kind == "observed" && e.Facts.Observation != nil {
				o := *e.Facts.Observation
				if o.ExitCode == nil || *o.ExitCode != 0 {
					t.Fatalf("failed action: %+v", o)
				}
				return o
			}
		}
		t.Fatal("missing observation")
		return harness.Observation{}
	}
	var latestHandles []sourceHandle
	query := func(operation, path string) archive.Retrieval {
		t.Helper()
		q := archive.RetrievalQuery{Operation: operation, Path: path, Reason: "recover original deployment evidence", Limit: 4096}
		if operation == "search" {
			q.Query, q.Limit = "Orchid", 128
		}
		body, _ := json.Marshal(q)
		act(core.Decision{Kind: core.Exec, Payload: "canti:retrieve " + string(body)})
		history := f.history()
		o := latestOutcome(t, history)
		latestHandles = sourceHandles(t, history)
		if o.Retrieval == nil || o.Retrieval.Error != "" || len(o.Retrieval.Sources) == 0 {
			t.Fatalf("missing evidence: %+v", o)
		}
		return *o.Retrieval
	}
	command := fmt.Sprintf(`# canti:discovery
examined=0
found=0
for file in /canti/runs/%s/steps/*/preview; do
  examined=$((examined+1))
  [ "$examined" -le 128 ] || { printf 'incomplete: file limit\n' >&2; exit 2; }
  first=$(head -n 1 "${file%%/preview}/command") || { printf 'incomplete: command unavailable\n' >&2; exit 2; }
  case "$first" in 'canti:'*|'# canti:discovery'*) continue ;; esac
  if grep -F -q 'Orchid' "$file"; then
    found=$((found+1))
    [ "$found" -le 16 ] || { printf 'incomplete: path limit\n' >&2; exit 2; }
    printf '%%s\n' "$file"
  else
    code=$?
    [ "$code" -eq 1 ] || exit "$code"
  fi
done`, sourceRun)
	returnedBytes, shellBytes, searchCalls, readCalls := 0, 0, 0, 0
	for page := 0; page < 3; page++ {
		first := step
		act(core.Decision{Kind: core.Exec, Payload: command})
		var sent struct {
			System string `json:"system"`
		}
		requests := f.model.requests()
		if err := json.Unmarshal(requests[len(requests)-1], &sent); err != nil {
			t.Fatal(err)
		}
		for _, convention := range []string{"# canti:discovery", "return paths only", "report an incomplete scan", "use read to obtain enough context"} {
			if !strings.Contains(sent.System, convention) {
				t.Fatalf("actual system request omitted discovery convention %q", convention)
			}
		}
		fresh := f.model.prompts(first)
		if len(fresh) != 1 || strings.Contains(fresh[0], "approved wait is 45 seconds") || strings.Contains(fresh[0], "approved maximum is 3 attempts") {
			t.Fatal("fresh page already contains the answers")
		}
		o := observe()
		if !o.Output.Complete || o.Output.Bytes > 2048 {
			t.Fatalf("discovery output not completely delivered within budget: %+v", o)
		}
		shellBytes += len(o.Preview)
		paths := strings.Fields(o.Preview)
		want := []string{}
		for _, n := range []int{1, 2, 4, 5} {
			want = append(want, fmt.Sprintf("/canti/runs/%s/steps/%08d/preview", sourceRun, n))
		}
		if !slices.Equal(paths, want) {
			t.Fatalf("discovery paths: %q, want %q (exclude earlier discovery)", paths, want)
		}
		var selected []sourceHandle
		for _, path := range paths {
			found := query("search", path)
			searchCalls++
			for i, source := range found.Sources {
				returnedBytes += source.Length
				if text := found.Evidence[i]; strings.Contains(text, "approved wait is 45 seconds") {
					t.Fatalf("candidate phase bypassed bounded read: %q", text)
				}
			}
			read := query("read", found.Sources[0].Path)
			readCalls++
			selected = append(selected, latestHandles...)
			if len(read.Evidence) != len(read.Sources) {
				t.Fatalf("read returned %d evidence texts for %d sources", len(read.Evidence), len(read.Sources))
			}
			for _, source := range read.Sources {
				returnedBytes += source.Length
			}
		}
		focus, _ := json.Marshal(struct {
			Sources []sourceHandle `json:"sources"`
		}{selected})
		act(core.Decision{Kind: core.Exec, Payload: harness.FocusPrefix + string(focus)})
		if page < 2 {
			act(core.Decision{Kind: core.Exec, Payload: harness.FocusPrefix + `{"sources":[]}`})
		} else {
			act(core.Decision{Kind: core.Return, Payload: "scripted evidence check complete"})
		}
		checkSelectedRequest(t, f, step-1, selected)
		joined := f.model.prompts(step - 1)
		if len(joined) != 1 || !strings.Contains(joined[0], corrected) || !strings.Contains(joined[0], retry) || !strings.Contains(joined[0], old) {
			t.Fatal("full joint evidence and correction were not simultaneously delivered")
		}
		if strings.Contains(joined[0], "approved 999 seconds") {
			t.Fatal("old discovery output contaminated original evidence")
		}
	}
	frozenBytes := 0
	for _, e := range f.history() {
		if e.Facts.Request != nil {
			frozenBytes += len(e.Facts.Request.Body)
		}
	}
	s := f.status()
	if s.Phase != core.PhaseReturned || len(f.model.requests()) != 33 || s.Totals.InputTokens+s.Totals.OutputTokens > 4000 {
		t.Fatalf("run/call/token bound: %+v, calls=%d", s, len(f.model.requests()))
	}
	checkReplay(t, f.status().Run.ID, f.history())
	t.Logf("SCRIPTED_DISCOVERY pages=3 shell_calls=3 search_calls=%d read_calls=%d model_calls=33 focus_selections=3 shell_bytes=%d native_bytes=%d frozen_request_bytes=%d synthetic_tokens=%d; real CLI/SQLite/Docker, scripted query choice and decisions; no live model task score or provider cost", searchCalls, readCalls, shellBytes, returnedBytes, frozenBytes, s.Totals.InputTokens+s.Totals.OutputTokens)
}
