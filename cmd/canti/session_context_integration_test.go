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
)

func TestSessionDialogueSurvivesCompactionRetryAndRestart(t *testing.T) {
	priorAnswer := "Previous answer chose async.\n" + strings.Repeat("Evidence `🙂` remains quoted.\n", 200)
	f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: priorAnswer}})
	if r := f.cli(nil, "run", "-session", "s", "-input", "first", "-goal", "Compare async and sync", "-image", image); r.code != exitOK {
		t.Fatalf("prior run: %+v", r)
	}
	priorID := f.status().Run.ID
	goal := "Correct the earlier answer: use sync. " + strings.Repeat("Keep the current requirement. ", 10)
	constraints := "Preserve the workspace result. Do not restore async."
	f.model.summarize = func(body []byte) (string, error) {
		return "The earlier answer chose async. The current correction is sync. Preserve the workspace and verify original evidence under /canti/runs/" + priorID + ". Executed effects must not be repeated.", nil
	}
	start := f.cli(nil, "run", "-once", "-session", "s", "-input", "second", "-goal", goal, "-constraints", constraints,
		"-image", image, "-summary-bytes", "2048", "-context-bytes", "98304", "-compact-steps", "2", "-retain-bytes", "1", "-max-steps", "7", "-wall-time", "3m", "-job-timeout", "10s")
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
	decision(1, 2, core.Decision{Kind: core.Exec, Payload: "printf async > mode.txt; printf '%s\\n' init >> effects.txt"})
	f.model.answer(2, 1, reply{status: 503, body: "temporary transport failure"})
	decision(2, 2, core.Decision{Kind: core.Exec, Payload: `canti:retrieve {"operation":"read","path":"/canti/runs/` + runID + `/steps/00000001/preview","reason":"verify committed observation"}`})
	decision(3, 1, core.Decision{Kind: core.Exec, Purpose: "Switch the mode from async to sync.", Payload: "test \"$(cat mode.txt)\" = async && printf sync > mode.txt && printf '%s\\n' corrected >> effects.txt"})
	decision(4, 1, core.Decision{Kind: core.Return, Payload: "Sync correction completed."})
	deadline := time.Now().Add(120 * time.Second)
	for n := 0; f.status().Phase != core.PhaseReturned; n++ {
		if n >= 100 || time.Now().After(deadline) {
			t.Fatal("session exceeded transition/time bound")
		}
		if r := f.cli(nil, "resume", "-once"); r.code != exitActive && r.code != exitOK {
			t.Fatalf("resume %d: %+v", n, r)
		}
	}
	if data, err := os.ReadFile(filepath.Join(f.workspace, "mode.txt")); err != nil || string(data) != "sync" {
		t.Fatalf("correction effect: %q %v", data, err)
	}
	if data, err := os.ReadFile(filepath.Join(f.workspace, "effects.txt")); err != nil || string(data) != "init\ncorrected\n" {
		t.Fatalf("effects replayed: %q %v", data, err)
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
		var envelope struct{ System string }
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatal(err)
		}
		if len(envelope.System)+len(prompt) > 98304 || !strings.Contains(prompt, constraints) || !strings.Contains(prompt, priorID) {
			t.Fatalf("step %d lost protected input or provenance", step)
		}
		if step < 3 && !strings.Contains(prompt, priorAnswer) {
			t.Fatal("earlier raw return disappeared before compact")
		}
		if step >= 3 && (!strings.Contains(prompt, "## Session summary") || strings.Contains(prompt, "### Step 1\n")) {
			t.Fatalf("step %d did not adopt compact", step)
		}
		if step == 2 {
			retry = append(retry, body)
		}
	}
	if len(retry) != 2 || !bytes.Equal(retry[0], retry[1]) {
		t.Fatal("transport retry changed frozen request")
	}
	summaries := f.model.summaryRequests()
	if len(summaries) < 2 {
		t.Fatal("expected repeated independent compaction")
	}
	// The previous retained retrieval must enter the next summary's source prefix.
	if !bytes.Contains(summaries[1], []byte("committed observation")) {
		t.Fatal("previous retained tail was skipped")
	}
	var thirdGoal = "Continue the session."
	if r := f.cli(nil, "run", "-once", "-session", "s", "-input", "third", "-goal", thirdGoal, "-image", image, "-wall-time", "3m", "-job-timeout", "10s"); r.code != exitActive {
		t.Fatalf("third: %+v", r)
	}
	decision(1, 3, core.Decision{Kind: core.Exec, Payload: "cat /canti/runs/" + runID + "/steps/00000003/purpose"})
	decision(2, 3, core.Decision{Kind: core.Return, Payload: "Continued from the session checkpoint and original records."})
	for n := 0; f.status().Phase != core.PhaseReturned; n++ {
		if n >= 40 {
			t.Fatal("third run transition bound")
		}
		if r := f.cli(nil, "resume", "-once"); r.code != exitActive && r.code != exitOK {
			t.Fatalf("third: %+v", r)
		}
	}
	var third []string
	for _, body := range f.model.requests() {
		if _, prompt, err := promptOf(body); err == nil && strings.Contains(prompt, thirdGoal) {
			third = append(third, prompt)
		}
	}
	if len(third) != 2 || !strings.Contains(third[0], "Sync correction completed.") || !strings.Contains(third[1], "Switch the mode from async to sync.") {
		t.Fatalf("cross-run tail or archive evidence lost: %q", third)
	}
	var shell []entry
	for _, e := range append(f.history("-run", runID), f.history("-run", f.status().Run.ID)...) {
		if !strings.HasPrefix(e.State.Decision.Payload, "canti:retrieve ") {
			shell = append(shell, e)
		}
	}
	checkReleased(t, f, shell)
	t.Logf("SCRIPTED_SESSION summaries=%d business_steps=4 retry_bytes_identical=true; real CLI/SQLite/Docker, process-per-transition resume", len(summaries))
}

func TestOversizedPreviousAnswerKeepsSessionNavigation(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: strings.Repeat("X", 70000)}})
	if r := f.cli(nil, "run", "-session", "s", "-input", "first", "-goal", "historical request"); r.code != exitOK {
		t.Fatalf("prior: %+v", r)
	}
	short, _ := json.Marshal(core.Decision{Kind: core.Return, Payload: "short confirmed answer"})
	f.model.answer(1, 2, reply{text: string(short)})
	if r := f.cli(nil, "run", "-session", "s", "-input", "second", "-goal", "new request", "-summary-bytes", "2048", "-context-bytes", "98304"); r.code != exitOK {
		t.Fatalf("navigation-only linked run: %+v", r)
	}
	if r := f.cli(nil, "run", "-session", "s", "-input", "third", "-goal", "third request", "-summary-bytes", "2048", "-context-bytes", "98304"); r.code != exitOK {
		t.Fatalf("projected linked run: %+v", r)
	}
	if r := f.cli(nil, "run", "-goal", "standalone request", "-summary-bytes", "2048", "-context-bytes", "98304"); r.code != exitOK {
		t.Fatalf("standalone: %+v", r)
	}
	prompts := f.model.prompts(1)
	if len(prompts) != 4 || strings.Contains(prompts[1], strings.Repeat("X", 100)) ||
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
