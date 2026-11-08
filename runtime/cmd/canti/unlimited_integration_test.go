//go:build integration

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestUnlimitedStepsAcrossCrashHandoverAndRestart(t *testing.T) {
	for _, tc := range []struct {
		limit int
		crash string
	}{{23, "response_saved"}, {0, "response_saved"}, {0, "attempt_started"}} {
		t.Run(fmt.Sprintf("max_%d_%s", tc.limit, tc.crash), func(t *testing.T) {
			f, image := recoveryFixture(t, nil)
			r := f.cli(nil, "run", "-once", "-goal", "write once and verify after handovers", "-image", image,
				"-max-steps", strconv.Itoa(tc.limit), "-wall-time", "3m", "-job-timeout", "10s")
			if r.code != exitActive {
				t.Fatalf("freeze: %+v", r)
			}
			runID := f.status().Run.ID
			for step := 1; step <= 23; step++ {
				d := core.Decision{Kind: core.Exec, Payload: `canti:retrieve {"operation":"list","path":"/canti/runs","reason":"check prior progress"}`}
				switch {
				case step == 1:
					d.Payload = "printf 'once\\n' >> proof"
				case step == 22:
					d.Payload = "test \"$(cat proof)\" = once && printf 'verified\\n' >> result"
				case step == 23:
					d = core.Decision{Kind: core.Return, Payload: "done"}
				case step%4 == 0:
					d.Payload = harness.FocusPrefix + `{"sources":[]}`
				}
				body, _ := json.Marshal(d)
				f.model.answer(step, 1, reply{text: string(body)})
				if step%4 == 0 && step < 22 {
					f.model.answer(step, 1, reply{status: 400, body: `{"error":{"type":"invalid_request_error","message":"prompt is too long: 200001 tokens > 200000 maximum"}}`})
					f.model.answer(step, 2, reply{text: string(body)})
				}
				if step == 2 {
					f.model.answer(step, 1, reply{status: 503, body: "temporary failure"})
					f.model.answer(step, 2, reply{text: string(body)})
				}
			}
			crashRun(t, f, "after:"+tc.crash, "resume")
			prefix := f.history()
			deadline := time.Now().Add(150 * time.Second)
			for step := 1; step <= 22; step++ {
				if time.Now().After(deadline) {
					t.Fatal("restart sequence exceeded wall-time budget")
				}
				completion := "after:performed"
				if step == 1 || step == 22 {
					completion = "after:observed"
				}
				crashRun(t, f, completion, "resume")
				s := f.status()
				if s.Step != step+1 || s.Phase != core.PhaseIdle || s.Totals.Steps != step {
					t.Fatalf("restart did not advance step %d: %+v", step, s)
				}
			}
			if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
				t.Fatalf("return: %+v", r)
			}
			h := f.history()
			if !extends(h, prefix) {
				t.Fatal("restart changed committed facts")
			}
			requests, handovers, startedAttempts := 0, 0, 0
			currentContext := harness.InitialContext()
			builtin := map[string]bool{}
			for _, e := range h {
				if e.State.Limits.MaxSteps != tc.limit {
					t.Fatalf("stored step limit changed: %+v", e.State)
				}
				if e.State.Step == 1 && e.Kind == "response_saved" {
					wantAttempt := 1
					if tc.crash == "attempt_started" {
						wantAttempt = 2
					}
					if e.State.Attempt != wantAttempt {
						t.Fatalf("saved response attempt = %d, want %d", e.State.Attempt, wantAttempt)
					}
				}
				if e.State.Step == 1 && e.Kind == "attempt_started" {
					startedAttempts++
					if e.State.Attempt != startedAttempts {
						t.Fatalf("attempt started out of order: %+v", e.State)
					}
				}
				if e.Kind == "request_rejected" && e.Facts.ContextState != nil {
					currentContext = *e.Facts.ContextState
				}
				if req := e.Facts.Request; req != nil {
					requests++
					if req.ContextState == nil || !req.ContextState.Equal(currentContext) {
						t.Fatalf("window at step %d: %+v", e.State.Step, req.ContextState)
					}
					if tc.limit == 0 && !bytes.Contains(req.Body, []byte("(unlimited steps)")) {
						t.Fatal("frozen request lost unlimited progress")
					}
				}
				if o := e.Facts.Outcome; o != nil {
					if o.Focus != nil {
						handovers++
					}
					builtin[evEvent[core.Performed](t, e).EffectID] = true
				}
			}
			wantStarted := 1
			if tc.crash == "attempt_started" {
				wantStarted = 2
			}
			if startedAttempts != wantStarted {
				t.Fatalf("started attempts = %d, want %d", startedAttempts, wantStarted)
			}
			sent := f.model.requests()
			if requests != 28 || handovers != 5 || len(sent) != 29 || !bytes.Equal(sent[0], prefix[0].Facts.Request.Body) || !bytes.Equal(sent[1], sent[2]) {
				t.Fatalf("requests=%d handovers=%d sends=%d; expected 28/5/29 and frozen retry bytes", requests, handovers, len(sent))
			}
			checkFile(t, filepath.Join(f.workspace, "proof"), "once\n")
			checkFile(t, filepath.Join(f.workspace, "result"), "verified\n")
			var shell []entry
			for _, e := range h {
				var identity struct{ EffectID string }
				if err := json.Unmarshal(e.Event, &identity); err != nil {
					t.Fatal(err)
				}
				if !builtin[identity.EffectID] {
					shell = append(shell, e)
				}
			}
			if jobs := checkReleased(t, f, shell); len(jobs) != 2 || len(builtin) != 20 {
				t.Fatalf("executed %d jobs, want 2", len(jobs))
			}
			checkReplay(t, runID, h)
			if r := f.cli(nil, "resume"); r.code != exitOK || len(f.model.requests()) != len(sent) {
				t.Fatalf("terminal resume repeated work: %+v", r)
			}
			t.Logf("max_steps=%d steps=23 handovers=5 process_restarts=23 transport_retries=1 docker_jobs=2 result=verified", tc.limit)
		})
	}
}

func TestUnlimitedStepsRetainBudgetsAndStop(t *testing.T) {
	for _, tc := range []struct {
		name   string
		flags  []string
		reason core.StopReason
	}{
		{"tokens", []string{"-token-budget", "100"}, core.StopBudgetExhausted},
		{"attempts", []string{"-max-attempts", "2"}, core.StopAttemptsExhausted},
		{"wall", []string{"-wall-time", "3s"}, core.StopTimedOut},
		{"cancel", nil, core.StopCancelled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, image := recoveryFixture(t, map[int]core.Decision{
				1: {Kind: core.Exec, Payload: "printf started > started; sleep 60"},
				2: {Kind: core.Return, Payload: "unreachable"},
			})
			if tc.name == "attempts" {
				for attempt := 1; attempt <= 2; attempt++ {
					f.model.answer(1, attempt, reply{status: 503, body: "temporary failure"})
				}
			}
			args := append([]string{"-goal", "exercise stopping", "-image", image, "-max-steps", "0"}, tc.flags...)
			cmd, wait := f.start(nil, "run", args...)
			if tc.name == "cancel" {
				waitFor(t, func() bool { _, err := os.Stat(filepath.Join(f.workspace, "started")); return err == nil })
				if err := cmd.Process.Signal(os.Interrupt); err != nil {
					t.Fatal(err)
				}
			}
			if r := wait(); r.code != exitStopped || !strings.Contains(r.stderr, "stopped: "+string(tc.reason)) {
				t.Fatalf("stop: %+v", r)
			}
			h := f.history()
			last := h[len(h)-1]
			if last.State.Limits.MaxSteps != 0 || last.State.Reason != tc.reason {
				t.Fatalf("stop changed unlimited budget: %+v", last.State)
			}
			checkReleased(t, f, h)
			calls := len(f.model.requests())
			if r := f.cli(nil, "resume"); r.code != exitStopped || len(f.model.requests()) != calls || len(f.history()) != len(h) {
				t.Fatalf("stopped resume changed committed facts: %+v", r)
			}
		})
	}
}
