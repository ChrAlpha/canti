package harness_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

func boundaryScenarios() []frozenScenario {
	return []frozenScenario{
		{name: "maximum-read-and-transient-tail", limits: core.Limits{MaxSteps: 6, MaxAttempts: 3}, imports: map[string]string{"large": strings.Repeat("`", 8192)}, runs: []frozenRun{{id: corpusRun, goal: "Read a maximum fenced source.", replies: []string{corpusRetrieve("read", "/canti/imports/large", `,"limit":8192`, "Read fenced source."), corpusDecision(core.Exec, `canti:focus {"sources":[]}`), corpusDecision(core.Return, "Original bytes archived.")}, steps: []string{"1 done retrieval 1 sources 0 entries", "2 done focus 0 sources"}}}},
	}
}
func corpusPurpose(kind core.Kind, purpose, payload string) string {
	b, err := json.Marshal(core.Decision{Kind: kind, Purpose: purpose, Payload: payload})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func envelope(id, stop, blocks string) string {
	return `raw:{"id":"msg_` + id + `","type":"message","role":"assistant","model":"claude-opus-5","content":[` + blocks +
		`],"stop_reason":"` + stop + `","usage":{"input_tokens":1000,"output_tokens":20}}`
}

// toolBlock is a tool_use block of id and name fields holding an unsupported
// decision.
func toolBlock(id, name, payload string) string {
	return `{"type":"tool_use","id":"` + id + `",` + name + `,"input":{"kind":"bogus","payload":"envelope ` + payload + `"}}`
}

// capped is a retrieval of imported whose command, with the fence marks
// beyond three that its four-backtick reason charges twice, takes width
// bytes of the 2048-byte command cap.
func capped(imported string, width int) string {
	command := func(reason string) string {
		return `canti:retrieve {"operation":"read","path":"/canti/imports/` + imported + `","reason":"` + reason + `"}`
	}
	fill := width - 2*(5-3) - len(command("````"))
	return corpusDecision(core.Exec, command("````"+strings.Repeat("r", fill)))
}

// evidenceBlocks extracts exact transient observation text from a provider body.
func evidenceBlocks(body []byte) []string {
	var req struct {
		Messages []struct{ Content []struct{ Text string } }
	}
	if json.Unmarshal(body, &req) != nil || len(req.Messages) != 1 {
		return nil
	}
	var out []string
	for _, c := range req.Messages[0].Content {
		if _, evidence, ok := strings.Cut(c.Text, "Retrieved original evidence:\n"); ok {
			out = append(out, evidence)
		}
	}
	return out
}

// These trajectories exercise adapter extraction through the actual Loop and
// SQLite-frozen repair requests without extending the registered version17 corpus.
type frozenRepairCase struct {
	reply, decision, diagnostic string
	shown                       string // rendered replacement for invalid UTF-8
}

func TestMalformedToolEnvelopesReachFrozenProtocolRepair(t *testing.T) {
	var cases []frozenRepairCase
	for _, raw := range []string{
		envelope("e1", "end_turn", toolBlock("toolu_e1", `"name":"canti_decision"`, "e1")),
		envelope("e2", "tool_use", toolBlock("", `"name":"canti_decision"`, "e2")),
		envelope("e3", "tool_use", toolBlock("toolu_e3", `"name":"other_tool"`, "e3")),
		envelope("e4", "tool_use", toolBlock("toolu_e4", `"name":"other_tool","name":"canti_decision"`, "e4")),
		envelope("e5", "tool_use", `{"type":"mystery"},`+toolBlock("toolu_e5", `"name":"canti_decision"`, "e5")),
		envelope("e6", "tool_use", toolBlock("toolu_e6a", `"name":"canti_decision"`, "e6")+","+toolBlock("toolu_e6b", `"name":"canti_decision"`, "e6")),
		envelope("e8", "tool_use", `{"type":"tool_use","id":"toolu_e8a","name":"canti_decision"},`+toolBlock("toolu_e8b", `"name":"canti_decision"`, "e8")),
	} {
		cases = append(cases, frozenRepairCase{reply: raw, decision: strings.TrimPrefix(raw, "raw:"), diagnostic: "expected kind and payload"})
	}
	cases = append(cases, frozenRepairCase{
		reply:      envelope("e9", "tool_use", `{"type":"tool_use","id":"toolu_e9","name":"canti_decision","input":{"kind":"bogus`+"\xff"+`","payload":"envelope e9"}}`),
		decision:   "{\"kind\":\"bogus\xff\",\"payload\":\"envelope e9\"}",
		diagnostic: "invalid UTF-8",
		shown:      "{\"kind\":\"bogus\uFFFD\",\"payload\":\"envelope e9\"}",
	})
	payload := "Every envelope was rejected."
	valid := envelope("e7", "tool_use", `{"type":"text","text":"Returning now."},{"type":"thinking","thinking":"Every envelope failed.","signature":"sig"},`+
		`{"type":"redacted_thinking","data":"cmVkYWN0ZWQ="},`+
		`{"type":"tool_use","id":"toolu_e7","name":"canti_decision","input":{"kind":"return","payload":"Every envelope was rejected."}}`)
	checkFrozenProtocolRepairs(t, true, cases, valid, payload)
}

func TestSplitTextEnvelopesReachFrozenProtocolRepair(t *testing.T) {
	cases := []frozenRepairCase{{
		reply: envelope("t1", "end_turn", `{"type":"thinking","thinking":"Plan.","text":"ignore-me","signature":"sig"},`+
			`{"type":"text","text":"{\"kind\":\"bogus\","},{"type":"text","text":"\"payload\":\"review-marker\"}"}`),
		decision:   `{"kind":"bogus","payload":"review-marker"}`,
		diagnostic: `unsupported kind "bogus"`,
	}}
	valid := envelope("t2", "end_turn", `{"type":"text","text":"{\"kind\":\"return\","},{"type":"text","text":"\"payload\":\"Split decisions are joined.\"}"}`)
	checkFrozenProtocolRepairs(t, false, cases, valid, "Split decisions are joined.")
}

func TestDecodeDiagnosticsReachSuccessiveFrozenProtocolRepairs(t *testing.T) {
	cases := []frozenRepairCase{
		{decision: `"just a string"`, diagnostic: "top-level value must be an object"},
		{decision: `{"kind":"exec","kind":"return","payload":"done"}`, diagnostic: `duplicate field "kind"`},
		{decision: `{"kind": }`, diagnostic: "invalid character '}' looking for beginning of value"},
		{decision: `{"kind":"exec","payload":"x"} {}`, diagnostic: "trailing value {"},
		{decision: `{"kind":"exec","extra":"x"}`, diagnostic: `unknown field "extra"`},
		{decision: `{"kind":1,"payload":"x"}`, diagnostic: "kind must be a string"},
		{decision: `{"kind":"exec","payload":2}`, diagnostic: "payload must be a string"},
		{decision: `{"kind":"exec","payload":"x"`, diagnostic: "EOF"},
		{decision: `{"kind":"exec","purpose":1,"payload":"x"}`, diagnostic: "purpose must be a string"},
		{decision: `{"kind":"exec","purpose":"Run nothing.","payload":" \n"}`, diagnostic: "exec payload is blank"},
		{decision: `{"purpose":"p","y":"a","x":"b"}`, diagnostic: `unknown field "y"`},
	}
	payload := "Every malformed decision was rejected."
	checkFrozenProtocolRepairs(t, false, cases, corpusDecision(core.Return, payload), payload)
}

func checkFrozenProtocolRepairs(t *testing.T, tool bool, cases []frozenRepairCase, valid, payload string) {
	t.Helper()
	replies := make([]string, 0, len(cases)+1)
	for _, c := range cases {
		if c.reply != "" {
			replies = append(replies, c.reply)
		} else {
			replies = append(replies, c.decision)
		}
	}
	replies = append(replies, valid)
	sc := frozenScenario{tool: tool, limits: core.Limits{MaxSteps: 2, MaxAttempts: len(replies)},
		runs: []frozenRun{{id: corpusRun, goal: "Report done.", replies: replies}}}
	rig := newCorpusRig(t, sc)
	rig.drive(t, sc)
	rig.checkOutcomes(t, sc.runs[0])
	requests, sent := rig.frozen(t, corpusRun), rig.model.sent()
	if len(requests) != len(replies) || len(sent) != len(requests) {
		t.Fatalf("requests=%d sends=%d want=%d", len(requests), len(sent), len(replies))
	}
	var stableSystem string
	var stableBlock json.RawMessage
	for i, tr := range requests {
		request := tr.Facts.Request
		if tr.To.Step != 1 || tr.To.Attempt != i+1 || tr.To.RequestRev != i+1 || !bytes.Equal(request.Body, sent[i]) {
			t.Fatalf("repair %d did not freeze/send a same-step revision: %+v", i, tr.To)
		}
		var body struct {
			System   string
			Messages []struct{ Content []json.RawMessage }
		}
		if err := json.Unmarshal(request.Body, &body); err != nil || len(body.Messages) != 1 || len(body.Messages[0].Content) != 2 {
			t.Fatalf("repair %d provider input: %v", i, err)
		}
		blocks := body.Messages[0].Content
		var tail struct {
			Text         string
			CacheControl json.RawMessage `json:"cache_control"`
		}
		if err := json.Unmarshal(blocks[1], &tail); err != nil || len(tail.CacheControl) != 0 {
			t.Fatalf("repair %d cached its dynamic tail: %s, %v", i, blocks[1], err)
		}
		if i == 0 {
			stableSystem, stableBlock = body.System, blocks[0]
			if strings.Contains(tail.Text, "## Protocol repair") || request.Context.Preparation != "first_request" {
				t.Fatal("first request fabricated a repair")
			}
			continue
		}
		if body.System != stableSystem || !bytes.Equal(blocks[0], stableBlock) || bytes.Equal(request.Body, requests[i-1].Facts.Request.Body) || request.Context.Preparation != "protocol_repair" {
			t.Fatalf("repair %d changed its stable prefix or reused the previous revision", i)
		}
		prior := cases[i-1]
		shown := prior.decision
		if prior.shown != "" {
			shown = prior.shown
		}
		want := "\n## Protocol repair\nThe prior reply of this step was invalid. Choose exactly one decision using the system protocol.\n" +
			"Error:\n```\ncore: invalid decision: " + prior.diagnostic + "\n```\nRejected reply:\n```\n" + shown + "\n```\n"
		if !strings.HasSuffix(tail.Text, want) || strings.Contains(tail.Text, "ignore-me") {
			t.Fatalf("repair %d lost the exact extraction/fallback or diagnostic:\ngot %q\nwant suffix %q", i, tail.Text, want)
		}
	}
	checkProtocolRepairRecords(t, rig, cases, replies, payload)
}

func checkProtocolRepairRecords(t *testing.T, rig *corpusRig, cases []frozenRepairCase, replies []string, payload string) {
	t.Helper()
	all, err := rig.store.Transitions(t.Context(), corpusRun, 0, 1000)
	if err != nil || len(all) != 4*len(replies) {
		t.Fatalf("protocol-only trajectory: transitions=%d error=%v", len(all), err)
	}
	for i, tr := range all {
		if want := []string{"request_frozen", "attempt_started", "response_saved", "response_loaded"}[i%4]; tr.Event.Kind() != want || tr.From.Step != 1 || tr.To.Step != 1 || tr.Facts.Action != nil || tr.Facts.Execution != nil {
			t.Fatalf("malformed reply escaped repair into execution: transition=%+v", tr)
		}
		response := i / 4
		if i%4 == 2 {
			if raw, ok := strings.CutPrefix(replies[response], "raw:"); ok && !bytes.Equal(tr.Facts.Response.Body, []byte(raw)) {
				t.Fatalf("response %d lost its exact raw envelope", response)
			}
		}
		if event, ok := tr.Event.(core.ResponseLoaded); ok {
			want := corpusDecision(core.Return, payload)
			if response < len(cases) {
				want = cases[response].decision
				if tr.To.Phase != core.PhaseIdle {
					t.Fatalf("response %d did not request repair: %+v", response, tr.To)
				}
			}
			if !bytes.Equal(event.Body, []byte(want)) {
				t.Fatalf("response %d decoded %q, want exact extracted/fallback bytes %q", response, event.Body, want)
			}
		}
	}
	final, err := rig.store.Latest(t.Context(), corpusRun)
	if err != nil || final.State.Phase != core.PhaseReturned || final.State.Decision.Payload != payload {
		t.Fatalf("valid final decision lost: %+v, %v", final, err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(rig.path), "artifacts")); !os.IsNotExist(err) {
		t.Fatalf("protocol repairs dispatched an executor command: %v", err)
	}
}
