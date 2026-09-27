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

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
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
		h, _ := json.Marshal(harness.Handover{RunID: runID, FirstStep: first, Text: "Current correction is sync. Verify the workspace and finish the request."})
		decision(step, 1, core.Decision{Kind: core.Exec, Payload: "canti:handover " + string(h)})
	}
	handover(2, 1)
	f.model.answer(3, 1, reply{status: 503, body: "temporary transport failure"})
	decision(3, 2, core.Decision{Kind: core.Exec, Payload: "test \"$(cat mode.txt)\" = async && printf sync > mode.txt"})
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
	t.Logf("SCRIPTED_SESSION context_limit=98304 previous_answer_bytes=%d windows=3 step3_transport_attempts=2; real CLI/SQLite/Docker, process-per-transition resume", len(priorAnswer))
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
