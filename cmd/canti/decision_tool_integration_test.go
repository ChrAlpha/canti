//go:build integration

package main

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

func TestToolSessionRetainsFrozenIdentityAcrossRetryAndRestart(t *testing.T) {
	f, image := recoveryFixture(t, nil)
	const identity = "canti-anthropic/3-tool"
	const protocol = "anthropic-messages/2023-06-01"
	answer := func(step, call int, body string) {
		f.model.answer(step, call, reply{status: http.StatusOK, body: body})
	}
	f.model.answer(1, 1, reply{status: http.StatusServiceUnavailable, body: "temporary transport failure"})
	invalid := toolReply(core.Decision{Kind: core.Exec, Payload: "touch forbidden"}, core.Decision{Kind: core.Return, Payload: "ignored"})
	answer(1, 2, invalid)
	answer(1, 3, toolReply(core.Decision{Kind: core.Exec, Payload: "printf 'first\\n' >> effects.txt"}))
	answer(2, 1, toolReply(core.Decision{Kind: core.Return, Payload: "first-done"}))
	start := f.cli(nil, "run", "-once", "-session", "tools", "-input", "first", "-goal", "write first",
		"-image", image, "-decision-format", "tool", "-fallbacks=false", "-max-steps", "0", "-max-attempts", "3", "-wall-time", "2m", "-job-timeout", "10s")
	if start.code != exitActive || len(f.model.requests()) != 0 {
		t.Fatalf("freeze before restart: %+v", start)
	}
	firstID := f.status().Run.ID
	if r := f.cli(nil, "resume", "-once"); r.code != exitActive {
		t.Fatalf("transport failure: %+v", r)
	}
	if s := f.status(); s.Phase != core.PhaseRequestFrozen || s.Attempt != 2 || s.RequestRev != 1 {
		t.Fatalf("transport retry state: %+v", s)
	}
	for range 2 {
		if r := f.cli(nil, "resume", "-once"); r.code != exitActive {
			t.Fatalf("save/decode invalid response: %+v", r)
		}
	}
	if s := f.status(); s.Phase != core.PhaseIdle || s.Attempt != 3 || s.RequestRev != 1 || len(boundJobs(t, f.history())) != 0 {
		t.Fatalf("ambiguous tools created an effect or lost retry identity: %+v", s)
	}
	if r := f.cli(nil, "resume"); r.code != exitOK || strings.TrimSpace(r.stdout) != "first-done" {
		t.Fatalf("first return after restart: %+v", r)
	}
	first := f.history("-run", firstID)
	checkFile(t, filepath.Join(f.workspace, "effects.txt"), "first\n")
	if _, err := os.Stat(filepath.Join(f.workspace, "forbidden")); !os.IsNotExist(err) {
		t.Fatalf("rejected command ran: %v", err)
	}
	wrongName := strings.Replace(toolReply(core.Decision{Kind: core.Exec, Payload: "touch forbidden"}), "canti_decision", "other_tool", 1)
	answer(1, 4, wrongName)
	answer(1, 5, toolReply(core.Decision{Kind: core.Exec, Payload: "test \"$(cat effects.txt)\" = first && printf 'second\\n' >> effects.txt"}))
	answer(2, 2, toolReply(core.Decision{Kind: core.Return, Payload: "second-done"}))
	if r := f.cli(nil, "run", "-once", "-session", "tools", "-input", "second", "-goal", "repair and write second",
		"-image", image, "-decision-format", "tool", "-fallbacks=false", "-max-steps", "0", "-max-attempts", "2", "-wall-time", "2m", "-job-timeout", "10s"); r.code != exitActive {
		t.Fatalf("second admission: %+v", r)
	}
	secondID := f.status().Run.ID
	if secondID == firstID || f.status().Run.SessionSeq != 2 {
		t.Fatal("second input did not retain session identity")
	}
	if r := f.cli(nil, "resume"); r.code != exitOK || strings.TrimSpace(r.stdout) != "second-done" {
		t.Fatalf("second return: %+v", r)
	}
	second := f.history("-run", secondID)
	wire := f.model.requests()
	if len(wire) != 7 || !bytes.Equal(wire[0], wire[1]) {
		t.Fatalf("transport retry changed frozen bytes or request count: %d", len(wire))
	}
	_, prompt, err := promptOf(wire[4])
	if err != nil || !strings.Contains(prompt, "first-done") || !strings.Contains(prompt, firstID) {
		t.Fatalf("second run lost prior session answer: %v", err)
	}
	var frozenBodies [][]byte
	rejected, incomplete, savedInvalid := 0, 0, false
	for _, history := range [][]entry{first, second} {
		for _, e := range history {
			if req := e.Facts.Request; req != nil {
				if req.Adapter != identity || req.Protocol != protocol || !bytes.Contains(req.Body, []byte(`"tools"`)) || bytes.Contains(req.Body, []byte(`"output_config"`)) {
					t.Fatalf("frozen identity/schema: %+v", req)
				}
				frozenBodies = append(frozenBodies, req.Body)
			}
			if resp := e.Facts.Response; resp != nil {
				if resp.Adapter != identity || resp.Protocol != protocol {
					t.Fatalf("saved response lost adapter/protocol: %+v", resp)
				}
				savedInvalid = savedInvalid || string(resp.Body) == invalid
			}
			if e.Kind == "response_loaded" && e.To == core.PhaseIdle {
				rejected++
			}
			if e.Kind == "response_incomplete" {
				incomplete++
			}
		}
	}
	if len(frozenBodies) != 6 || rejected != 2 || incomplete != 1 || !savedInvalid {
		t.Fatalf("durable evidence: requests=%d rejected=%d incomplete=%d raw_invalid=%v", len(frozenBodies), rejected, incomplete, savedInvalid)
	}
	for i, index := range []int{0, 2, 3, 4, 5, 6} {
		if !bytes.Equal(frozenBodies[i], wire[index]) {
			t.Fatalf("HTTP body diverged from SQLite request %d", i)
		}
	}
	if r := f.cli(nil, "run", "-session", "tools", "-input", "second", "-goal", "repair and write second", "-decision-format", "text"); r.code != exitOK || len(f.model.requests()) != 7 {
		t.Fatalf("same input resubmitted model work: %+v", r)
	}
	checkFile(t, filepath.Join(f.workspace, "effects.txt"), "first\nsecond\n")
	checkReplay(t, firstID, first)
	checkReplay(t, secondID, second)
	checkRecords(t, f, "write first", first)
	checkRecords(t, f, "repair and write second", second)
	checkReleased(t, f, append(first, second...))
	t.Log("TOOL_SESSION runs=2 effects=2 requests=7 frozen_requests=6 transport_retries=1 protocol_rejections=2; real CLI/SQLite/Docker, cross-process resume")
}

func TestToolProtocolFailureTerminatesWithoutEffects(t *testing.T) {
	f := newFixture(t, nil)
	for call := 1; call <= 3; call++ {
		body := toolReply(core.Decision{Kind: core.Exec, Payload: "touch forbidden"})
		body = strings.Replace(body, "canti_decision", "wrong_name", 1)
		f.model.answer(1, call, reply{status: http.StatusOK, body: body})
	}
	if r := f.cli(nil, "run", "-goal", "reject ambiguous provider decisions", "-decision-format", "tool", "-max-attempts", "3"); r.code != exitStopped {
		t.Fatalf("bounded protocol failure: %+v", r)
	}
	s := f.status()
	if s.Phase != core.PhaseStopped || s.Reason != core.StopProtocolFailure || s.Step != 1 || s.Totals.Steps != 0 ||
		s.Totals.InputTokens != 300 || s.Totals.OutputTokens != 30 || len(f.model.requests()) != 3 || len(boundJobs(t, f.history())) != 0 {
		t.Fatalf("protocol failure did not terminate without effects and retain usage: %+v", s)
	}
}
