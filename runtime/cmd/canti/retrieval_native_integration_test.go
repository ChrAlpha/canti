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

func TestNativeDiscoveryOwnRunAcrossRestartsAndWindows(t *testing.T) {
	f, image := recoveryFixture(t, nil)
	evidence := "Orchid approved wait is 47 seconds."
	payload := strings.Repeat("background\n", 56000) + evidence + "\n" + strings.Repeat("background\n", 1000)
	if err := os.WriteFile(filepath.Join(f.workspace, "input"), []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	r := f.cli(nil, "run", "-once", "-goal", "Continue deployment using the approved wait", "-image", image,
		"-window-steps", "3", "-max-steps", "12", "-token-budget", "2000", "-wall-time", "3m", "-job-timeout", "10s")
	if r.code != exitActive {
		t.Fatalf("start: %d %s", r.code, r.stderr)
	}
	run, step := f.status().Run.ID, 1
	deadline := time.Now().Add(150 * time.Second)
	act := func(command string) {
		t.Helper()
		d := core.Decision{Kind: core.Exec, Payload: command}
		if command == "done" {
			d.Kind = core.Return
		}
		body, _ := json.Marshal(d)
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
			if e.Facts.Observation != nil {
				return *e.Facts.Observation
			}
		}
		t.Fatal("missing observation")
		return harness.Observation{}
	}
	handover := func(first int) {
		t.Helper()
		act(harness.HandoverPrefix + "Continue deployment. Recheck original approvals before applying settings.")
	}
	act("cat input; rm input")
	original := observe()
	if !original.Output.Complete || strings.Contains(original.Preview, evidence) {
		t.Fatal("fixture must retain a complete artifact with the approval outside preview")
	}
	act("# canti:discovery\nprintf 'Orchid approved wait is 999 seconds.\\n'")
	act("printf 'unrelated checkpoint\\n'")
	handover(1)
	q, _ := json.Marshal(archive.RetrievalQuery{Operation: "discover", Path: "/canti/runs/" + run + "/steps", Reason: "locate original approval", Query: "Orchid", Limit: 4096})
	for page := 0; page < 2; page++ {
		discoveryStep := step
		act("canti:retrieve " + string(q))
		o := latestOutcome(t, f.history())
		if o.Retrieval == nil || o.Retrieval.Error != "" || o.Retrieval.More {
			t.Fatalf("discovery did not finish: %+v", o)
		}
		artifact, meta := false, false
		for _, source := range o.Retrieval.Sources {
			switch source.Path {
			case "/canti/artifacts/" + original.Output.Path:
				artifact = true
			case fmt.Sprintf("/canti/runs/%s/steps/00000001/meta", run):
				meta = true
			default:
				t.Fatalf("discovery included an echo or unrelated source: %+v", source)
			}
		}
		if !artifact || !meta {
			t.Fatalf("artifact and original meta not delivered: %+v", o.Retrieval.Sources)
		}
		fresh := f.model.prompts(discoveryStep)
		if len(fresh) != 1 || strings.Contains(fresh[0], evidence) {
			t.Fatal("fresh window already carried the original approval")
		}
		if page == 0 {
			act("printf 'verification checkpoint\\n'")
			act("printf 'next checkpoint\\n'")
			handover(discoveryStep)
		} else {
			act("done")
		}
		shown := f.model.prompts(discoveryStep + 1)
		if len(shown) != 1 || !strings.Contains(shown[0], evidence) || strings.Contains(shown[0], "999 seconds") {
			t.Fatal("original artifact evidence missing or retrieval echo included in frozen request")
		}
	}
	if f.status().Phase != core.PhaseReturned || len(f.model.requests()) != 10 {
		t.Fatalf("unexpected final phase/calls: %+v, %d", f.status(), len(f.model.requests()))
	}
	t.Log("SCRIPTED_NATIVE_DISCOVERY windows=3 model_calls=10 discover_calls=2; same growing run, real CLI/SQLite/Docker and restarts; scripted decisions, no autonomous task score")
}
