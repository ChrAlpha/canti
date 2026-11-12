//go:build integration

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
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestLineMultiRunWindowStress(t *testing.T) {
	runs := 4
	if raw := os.Getenv("CANTI_LINE_STRESS_RUNS"); raw != "" {
		var err error
		runs, err = strconv.Atoi(raw)
		if err != nil || runs < 4 || runs > 100 {
			t.Fatal("CANTI_LINE_STRESS_RUNS must be 4..100")
		}
	}
	started := time.Now()
	metrics := struct {
		Runs, Completed, Steps, Anchors, Handovers, Requests, Retries, Crashes int
		Effects, Duplicates, Containers, NavigationBoundaries                  int
		BusinessEntries                                                        int
		Failed                                                                 bool
		MaxPromptBytes                                                         int
		DBBytes, RuntimeMS                                                     int64
	}{Runs: runs}
	f, image := recoveryFixture(t, nil)
	f.model.decideWith(func(step int, prompt string) (core.Decision, error) {
		d, err := lineStressDecision(step, prompt)
		if err != nil {
			t.Errorf("scripted policy: %v", err)
		}
		return d, err
	})
	t.Cleanup(func() {
		metrics.Requests = len(f.model.requests())
		metrics.RuntimeMS = time.Since(started).Milliseconds()
		metrics.Failed = t.Failed()
		metrics.Containers = len(effectContainers(t, f.state))
		effects, _ := os.ReadFile(filepath.Join(f.workspace, "effects.txt"))
		seen := map[string]bool{}
		metrics.Effects, metrics.Duplicates = 0, 0
		for line := range strings.Lines(string(effects)) {
			metrics.Effects++
			if seen[line] {
				metrics.Duplicates++
			}
			seen[line] = true
		}
		ledger, _ := os.ReadFile(filepath.Join(f.workspace, "ledger.txt"))
		metrics.BusinessEntries = strings.Count(string(ledger), "\n")
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if info, err := os.Stat(filepath.Join(f.state, "canti.db"+suffix)); err == nil {
				metrics.DBBytes += info.Size()
			}
		}
		data, _ := json.Marshal(metrics)
		t.Logf("CANTI_LINE_STRESS %s", data)
	})
	var ids []string
	var inputs [][]string
	var ledger strings.Builder
	var executed []entry
	deadline := started.Add(12 * time.Minute)
	for round := 1; round <= runs; round++ {
		if time.Now().After(deadline) {
			t.Fatalf("line exceeded its 12-minute wall budget after %d runs", metrics.Completed)
		}
		goal := fmt.Sprintf("LINE_STRESS_ROUND=%d\nAppend this round's A, B, C once; verify earlier original evidence before continuing.", round)
		args := []string{"-input", fmt.Sprintf("round-%02d", round), "-goal", goal,
			"-constraints", "Keep every earlier ledger entry and verify original evidence.", "-image", image,
			"-request-bytes", "98304", "-max-steps", "32", "-max-attempts", "3",
			"-wall-time", "90s", "-job-timeout", "10s", "-token-budget", "10000", "-output-budget", "1048576"}
		inputs = append(inputs, args)
		if r := f.cli(nil, "run", append(append([]string{}, args...), "-once")...); r.code != exitActive {
			t.Fatalf("round %d admission: %+v", round, r)
		}
		admitted := f.status()
		if admitted.Run.InputID != fmt.Sprintf("round-%02d", round) || admitted.Run.InputPos == 0 || admitted.Step != 1 {
			t.Fatalf("round %d admission identity: %+v", round, admitted)
		}
		for _, id := range ids {
			if id == admitted.Run.ID {
				t.Fatal("new input reused a run identity")
			}
		}
		ids = append(ids, admitted.Run.ID)
		if round > 1 {
			assertStressReplay(t, f, inputs[round-2], exitOK)
		}
		assertStressReplay(t, f, args, exitActive)
		f.model.mu.Lock()
		retryCall := f.model.sent[6] + 1
		reset4, reset8 := f.model.sent[4]+1, f.model.sent[8]+1
		f.model.mu.Unlock()
		f.model.answer(6, retryCall, reply{status: 503, body: "bounded line stress transport failure"})
		f.model.answer(4, reset4, reply{status: 400, body: `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 200001 tokens > 200000 maximum"}}`})
		f.model.answer(8, reset8, reply{status: 400, body: `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 200001 tokens > 200000 maximum"}}`})
		beforeCalls := len(f.model.requests())
		if round == runs-1 {
			if r := f.cli([]string{"CANTI_TEST_CRASH=after:observed"}, "resume"); r.code != -1 {
				t.Fatalf("late-run committed observation crash: %+v", r)
			}
			metrics.Crashes++
			if s := f.status(); s.Run.ID != admitted.Run.ID || s.Step != 2 || s.Phase != core.PhaseIdle {
				t.Fatalf("crash lost committed progress: %+v", s)
			}
			checkFile(t, filepath.Join(f.workspace, "effects.txt"), ledger.String()+fmt.Sprintf("run-%02d:A\n", round))
			assertStressReplay(t, f, args, exitActive)
			assertStressReplay(t, f, inputs[round-2], exitOK)
		}
		if r := f.cli(nil, "resume"); r.code != exitOK {
			t.Fatalf("round %d resume: %+v", round, r)
		}
		final := f.status()
		if final.Phase != core.PhaseReturned || final.Run.ID != admitted.Run.ID || final.Decision == nil ||
			!strings.HasPrefix(final.Decision.Payload, fmt.Sprintf("completed run=%02d origin=/canti/runs/%s/", round, ids[0])) {
			t.Fatalf("round %d did not finish: %+v", round, final)
		}

		for _, stage := range "ABC" {
			fmt.Fprintf(&ledger, "run-%02d:%c\n", round, stage)
		}
		checkFile(t, filepath.Join(f.workspace, "ledger.txt"), ledger.String())
		effects, err := os.ReadFile(filepath.Join(f.workspace, "effects.txt"))
		if err != nil {
			t.Fatal(err)
		}
		metrics.Duplicates = max(0, strings.Count(string(effects), "\n")-round*3)
		if string(effects) != ledger.String() {
			t.Fatalf("side effects were lost or repeated at round %d: %q", round, effects)
		}
		metrics.Effects = round * 3
		assertStressReplay(t, f, args, exitOK)
		requests := f.model.requests()[beforeCalls:]
		var retry [][]byte
		for _, body := range requests {
			step, _, err := promptOf(body)
			if err != nil {
				t.Fatal(err)
			}
			if step == 6 {
				retry = append(retry, body)
			}
		}
		if len(retry) != 2 || !bytes.Equal(retry[0], retry[1]) {
			t.Fatalf("round %d transport retry changed frozen bytes: %d attempts", round, len(retry))
		}
		metrics.Retries++
		history := f.history("-run", admitted.Run.ID)
		anchors := map[int64]bool{}
		handovers, resets, frozen := 0, 0, 0
		for _, e := range history {
			if !strings.HasPrefix(e.State.Decision.Payload, "canti:") {
				executed = append(executed, e)
			}
			if obs := e.Facts.Observation; obs != nil {
				if obs.Execution != core.ExecutionExited || obs.ExitCode == nil || *obs.ExitCode != 0 {
					t.Fatalf("round %d observation failed: %+v", round, obs)
				}
			}
			if o := e.Facts.Outcome; o != nil {
				if o.Status != harness.OutcomeDone {
					t.Fatalf("round %d local action failed: %+v", round, o)
				}
				if o.Status == harness.OutcomeDone && o.Retrieval == nil && o.Detail == "" {
					handovers++
				}
			}
			if e.Facts.Rejection != nil {
				resets++
			}
			if req := e.Facts.Request; req != nil {
				frozen++
				if req.ContextState == nil || req.Context == nil {
					t.Fatal("frozen request lost anchor or capture")
				}
				anchors[req.ContextState.AnchorSeq] = true
				var envelope struct{ System string }
				_, prompt, err := promptOf(req.Body)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(req.Body, &envelope); err != nil {
					t.Fatal(err)
				}
				promptBytes := len(envelope.System) + len(prompt)
				if promptBytes != req.Context.Bytes.Total || promptBytes > 98304 {
					t.Fatalf("round %d prompt bytes %d vs %+v", round, promptBytes, req.Context.Bytes)
				}
				metrics.MaxPromptBytes = max(metrics.MaxPromptBytes, promptBytes)
				prior := req.Context.PreviousRun
				want := ""
				if round > 1 {
					want = ids[round-2]
				}
				if prior != want {
					t.Fatalf("round %d previous-run navigation: %q, want %q", round, prior, want)
				}
				if e.State.Step == 1 && (round == 9 || round == 10) {
					metrics.NavigationBoundaries++
				}
			}
		}
		if handovers != 2 || resets != 2 || len(anchors) != 5 || frozen+1 != len(requests) {
			t.Fatalf("round %d continuity: anchors=%v handovers=%d frozen=%d calls=%d", round, anchors, handovers, frozen, len(requests))
		}
		metrics.Steps += final.Totals.Steps
		metrics.Anchors += len(anchors)
		metrics.Handovers += handovers
		metrics.Containers = len(effectContainers(t, f.state))
		if metrics.Containers != 0 {
			t.Fatalf("round %d left %d containers", round, metrics.Containers)
		}
		metrics.Completed++
		t.Logf("LINE_ROUND round=%d run=%s steps=%d anchors=%d prompt_max=%d elapsed_ms=%d", round, admitted.Run.ID, final.Totals.Steps, len(anchors), metrics.MaxPromptBytes, time.Since(started).Milliseconds())
	}
	checkReleased(t, f, executed)
	if metrics.Completed != runs || metrics.Retries != runs || metrics.Crashes != 1 || metrics.Handovers != runs*2 ||
		runs >= 10 && metrics.NavigationBoundaries != 2 {
		t.Fatalf("incomplete stress coverage: %+v", metrics)
	}
}

func assertStressReplay(t *testing.T, f *fixture, args []string, code int) {
	t.Helper()
	readEffects := func() [2]string {
		var data [2]string
		for i, name := range []string{"ledger.txt", "effects.txt"} {
			body, err := os.ReadFile(filepath.Join(f.workspace, name))
			if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			data[i] = string(body)
		}
		return data
	}
	before, calls := f.status(), len(f.model.requests())
	effects := readEffects()
	if r := f.cli(nil, "run", args...); r.code != code {
		t.Fatalf("input replay: %+v, want %d", r, code)
	}
	if after := f.status(); !reflect.DeepEqual(before, after) || len(f.model.requests()) != calls {
		t.Fatal("input replay changed the active run or made a model request")
	}
	if effects != readEffects() {
		t.Fatal("input replay repeated a workspace side effect")
	}
}
