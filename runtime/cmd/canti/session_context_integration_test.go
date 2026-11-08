//go:build integration

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestSessionDialogueSurvivesHandoverRetryAndRestart(t *testing.T) {
	priorAnswer := "Previous answer chose async.\n" + strings.Repeat("Evidence `🙂` remains quoted.\n", 200)
	f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: priorAnswer}})
	if r := f.cli(nil, "run", "-session", "s", "-input", "first", "-goal", "Compare async and sync", "-image", image); r.code != exitOK {
		t.Fatalf("prior run: %+v", r)
	}
	priorID := f.status().Run.ID
	goal := "Correct the earlier answer: use sync. " + strings.Repeat("Keep the current requirement. ", 10)
	constraints := "Preserve the workspace result. Do not restore async."
	start := f.cli(nil, "run", "-once", "-session", "s", "-input", "second", "-goal", goal, "-constraints", constraints,
		"-image", image, "-context-bytes", "98304", "-window-steps", "1", "-max-steps", "7", "-wall-time", "3m", "-job-timeout", "10s")
	if start.code != exitActive {
		t.Fatalf("session start: %+v", start)
	}
	runID := f.status().Run.ID
	if runID == priorID || f.status().Run.SessionSeq != 2 {
		t.Fatal("session input did not create a new ordered run")
	}
	decision := func(step, call int, d core.Decision) {
		body, _ := json.Marshal(d)
		f.model.answer(step, call, reply{text: string(body)})
	}
	decision(1, 2, core.Decision{Kind: core.Exec, Payload: "printf async > mode.txt"})
	handover := func(step, first int) {
		decision(step, 1, core.Decision{Kind: core.Exec, Payload: harness.HandoverPrefix + "Current correction is sync. Verify the workspace and finish the request."})
	}
	handover(2, 1)
	f.model.answer(3, 1, reply{status: 503, body: "temporary transport failure"})
	decision(3, 2, core.Decision{Kind: core.Exec, Purpose: "Switch the mode from async to sync.", Payload: "test \"$(cat mode.txt)\" = async && printf sync > mode.txt"})
	handover(4, 3)
	decision(5, 1, core.Decision{Kind: core.Return, Payload: "Sync correction completed."})

	deadline := time.Now().Add(120 * time.Second)
	for step := 1; step <= 5; step++ {
		advanced := false
		for n := 0; n < 100 && time.Now().Before(deadline); n++ {
			r := f.cli(nil, "resume", "-once")
			if r.code != exitActive && r.code != exitOK {
				t.Fatalf("step %d process %d: %+v", step, n, r)
			}
			s := f.status()
			if s.Step > step || s.Phase == core.PhaseReturned {
				advanced = true
				break
			}
		}
		if !advanced {
			t.Fatalf("step %d failed to advance before deadline", step)
		}
	}
	if s := f.status(); s.Phase != core.PhaseReturned || s.Decision == nil || s.Decision.Payload != "Sync correction completed." {
		t.Fatalf("business return: %+v", s)
	}
	if data, err := os.ReadFile(filepath.Join(f.workspace, "mode.txt")); err != nil || string(data) != "sync" {
		t.Fatalf("correction effect: %q, %v", data, err)
	}
	var retry [][]byte
	for _, body := range f.model.requests() {
		step, prompt, err := promptOf(body)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(prompt, strings.TrimSpace(goal)) {
			continue
		}
		var envelope struct {
			System string `json:"system"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatal(err)
		}
		if len(envelope.System)+len(prompt) > 98304 || !strings.Contains(prompt, constraints) ||
			!strings.Contains(prompt, priorAnswer) || !strings.Contains(prompt, priorID) {
			t.Fatalf("step %d lost the corrected goal, constraints, historical pair, or prompt budget (%d bytes)", step, len(envelope.System)+len(prompt))
		}
		if step == 3 {
			if !strings.Contains(prompt, "This window begins at step 3") || strings.Contains(prompt, "### Step 1\n") {
				t.Fatalf("fresh window copied earlier steps: %s", prompt)
			}
			retry = append(retry, body)
		}
	}
	if len(retry) != 2 || !bytes.Equal(retry[0], retry[1]) {
		t.Fatalf("step 3 transport retry changed frozen request: %d requests, total calls %d, current goal len %d", len(retry), len(f.model.requests()), len(goal))
	}
	meta, err := os.ReadFile(filepath.Join(f.state, "history", runID, "session"))
	if err != nil || !strings.Contains(string(meta), `"previous_run_id":"`+priorID+`"`) {
		t.Fatalf("session navigation: %q, %v", meta, err)
	}
	var executed []entry
	for _, e := range f.history("-run", runID) {
		if !strings.HasPrefix(e.State.Decision.Payload, "canti:handover") {
			executed = append(executed, e)
		}
	}
	checkReleased(t, f, executed)

	steps := "/canti/runs/" + runID + "/steps/"
	if r := f.cli(nil, "run", "-once", "-session", "s", "-input", "third", "-goal", "Continue the session.", "-image", image, "-wall-time", "3m", "-job-timeout", "10s"); r.code != exitActive {
		t.Fatalf("third session run: %+v", r)
	}
	decision(1, 3, core.Decision{Kind: core.Exec, Payload: "cat " + steps + "00000004/command " + steps + "00000003/purpose"})
	decision(2, 2, core.Decision{Kind: core.Return, Payload: "Continued from the handover."})
	for n := 0; f.status().Phase != core.PhaseReturned; n++ {
		if r := f.cli(nil, "resume", "-once"); n == 20 || r.code != exitActive && r.code != exitOK {
			t.Fatalf("third session run, process %d: %+v", n, r)
		}
	}
	var third []string
	for _, body := range f.model.requests() {
		if _, prompt, err := promptOf(body); err == nil && strings.Contains(prompt, "Continue the session.") {
			third = append(third, prompt)
		}
	}
	path := "Latest handover of this run, agent-written and possibly overtaken by later steps: " + steps + "00000004/command\n"
	if len(third) != 2 || !strings.Contains(third[0], "; returned):\n"+path) ||
		!strings.Contains(third[1], "Current correction is sync. Verify the workspace and finish the request.") || !strings.Contains(third[1], "Switch the mode from async to sync.") {
		t.Fatalf("the third run did not find and read the handover and purpose records from its container: %q", third)
	}
	report := f.cli(nil, "context", "-text", "-step", "1", "-revision", "1")
	if report.code != exitOK || !strings.Contains(report.stdout, "Earlier dialogue: run "+runID+" sequence 2, returned, goal included true, latest handover step 4\n") {
		t.Fatalf("the context report of the third run does not name the handover step it points to: %+v", report)
	}
	checkReleased(t, f, append(executed, f.history("-run", f.status().Run.ID)...))
	t.Logf("SCRIPTED_SESSION context_limit=98304 previous_answer_bytes=%d windows=3 step3_transport_attempts=2 continued_from_handover_step=4; real CLI/SQLite/Docker, process-per-transition resume", len(priorAnswer))
}

func TestOversizedPreviousAnswerKeepsSessionNavigation(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: strings.Repeat("X", 70000)}})
	if r := f.cli(nil, "run", "-session", "s", "-input", "first", "-goal", "historical request"); r.code != exitOK {
		t.Fatalf("prior: %+v", r)
	}
	priorID := f.status().Run.ID
	short, _ := json.Marshal(core.Decision{Kind: core.Return, Payload: "short confirmed answer"})
	f.model.answer(1, 2, reply{text: string(short)})
	if r := f.cli(nil, "run", "-session", "s", "-input", "second", "-goal", "new request", "-context-bytes", "98304"); r.code != exitOK {
		t.Fatalf("navigation-only linked run: %+v", r)
	}
	if r := f.cli(nil, "run", "-session", "s", "-input", "third", "-goal", "third request", "-context-bytes", "98304"); r.code != exitOK {
		t.Fatalf("projected linked run: %+v", r)
	}
	if r := f.cli(nil, "run", "-goal", "standalone request", "-context-bytes", "98304"); r.code != exitOK {
		t.Fatalf("standalone: %+v", r)
	}
	prompts := f.model.prompts(1)
	if len(prompts) != 4 || strings.Contains(prompts[1], strings.Repeat("X", 100)) || !strings.Contains(prompts[1], priorID+"/session") ||
		strings.Contains(prompts[1], "short confirmed answer") || !strings.Contains(prompts[2], "short confirmed answer") ||
		strings.Contains(prompts[2], strings.Repeat("X", 100)) || strings.Contains(prompts[3], "short confirmed answer") ||
		strings.Contains(prompts[3], "/session") {
		t.Fatal("oversized navigation, projected dialogue, and standalone inputs were not distinct")
	}
	for _, body := range f.model.requests() {
		_, prompt, err := promptOf(body)
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			System string `json:"system"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatal(err)
		}
		if len(envelope.System)+len(prompt) > 98304 {
			t.Fatal("rendered request exceeded the 96 KiB context limit")
		}
	}
}
