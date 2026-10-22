package harness_test

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

const corpusThirdRun = "run_5d2a8e6f1c9b4a07e3f8b2d6c1a9e4f0"

// boundaryScenarios exercise provider envelopes, frozen evidence, and field caps.
func boundaryScenarios() []frozenScenario {
	stepLimits := core.Limits{MaxSteps: 20, MaxAttempts: 3}
	imported := strings.Repeat("a", 250) + "/" + strings.Repeat("b", 250) + "/" + strings.Repeat("c", 250) + "/" + strings.Repeat("d", 250) + ".md"
	return slices.Concat([]frozenScenario{

		{
			name: "tool-envelopes", tool: true, limits: core.Limits{MaxSteps: 20, MaxAttempts: 9},
			runs: []frozenRun{{id: corpusRun, goal: "Report done.", replies: []string{
				envelope("e1", "end_turn", toolBlock("toolu_e1", `"name":"canti_decision"`, "e1")),
				envelope("e2", "tool_use", toolBlock("", `"name":"canti_decision"`, "e2")),
				envelope("e3", "tool_use", toolBlock("toolu_e3", `"name":"other_tool"`, "e3")),
				envelope("e4", "tool_use", toolBlock("toolu_e4", `"name":"other_tool","name":"canti_decision"`, "e4")),
				envelope("e5", "tool_use", `{"type":"mystery"},`+toolBlock("toolu_e5", `"name":"canti_decision"`, "e5")),
				envelope("e6", "tool_use", toolBlock("toolu_e6a", `"name":"canti_decision"`, "e6")+","+toolBlock("toolu_e6b", `"name":"canti_decision"`, "e6")),
				envelope("e8", "tool_use", `{"type":"tool_use","id":"toolu_e8a","name":"canti_decision"},`+toolBlock("toolu_e8b", `"name":"canti_decision"`, "e8")),
				envelope("e9", "tool_use", `{"type":"tool_use","id":"toolu_e9","name":"canti_decision","input":{"kind":"bogus`+"\xff"+`","payload":"envelope e9"}}`),
				envelope("e7", "tool_use", `{"type":"text","text":"Returning now."},{"type":"thinking","thinking":"Every envelope failed.","signature":"sig"},`+
					`{"type":"redacted_thinking","data":"cmVkYWN0ZWQ="},`+
					`{"type":"tool_use","id":"toolu_e7","name":"canti_decision","input":{"kind":"return","payload":"Every envelope was rejected."}}`),
			}}},
			marks: map[string][]string{
				"run1-step01-rev2.json": {"Rejected reply:\n```\n{\"id\":\"msg_e1\""},
				"run1-step01-rev3.json": {"Rejected reply:\n```\n{\"id\":\"msg_e2\""},
				"run1-step01-rev4.json": {"Rejected reply:\n```\n{\"id\":\"msg_e3\""},
				"run1-step01-rev5.json": {"Rejected reply:\n```\n{\"id\":\"msg_e4\""},
				"run1-step01-rev6.json": {"Rejected reply:\n```\n{\"id\":\"msg_e5\""},
				"run1-step01-rev7.json": {"Rejected reply:\n```\n{\"id\":\"msg_e6\""},
				"run1-step01-rev8.json": {"Rejected reply:\n```\n{\"id\":\"msg_e8\""},
				"run1-step01-rev9.json": {"core: invalid decision: invalid UTF-8", "Rejected reply:\n```\n{\"kind\":\"bogus\uFFFD\""},
			},
		},
		{
			name: "text-envelopes", limits: stepLimits,
			runs: []frozenRun{{id: corpusRun, goal: "Report done.", replies: []string{
				envelope("t1", "end_turn", `{"type":"thinking","thinking":"Plan.","text":"ignore-me","signature":"sig"},`+
					`{"type":"text","text":"{\"kind\":\"bogus\","},{"type":"text","text":"\"payload\":\"review-marker\"}"}`),
				envelope("t2", "end_turn", `{"type":"text","text":"{\"kind\":\"return\","},{"type":"text","text":"\"payload\":\"Split decisions are joined.\"}"}`),
			}}},
			marks: map[string][]string{
				"run1-step01-rev2.json": {"Rejected reply:\n```\n{\"kind\":\"bogus\",\"payload\":\"review-marker\"}\n```\n"},
			},
		},
		{
			name: "decode-diagnostics", limits: core.Limits{MaxSteps: 20, MaxAttempts: 12},
			runs: []frozenRun{{id: corpusRun, goal: "Report done.", replies: []string{
				`"just a string"`,
				`{"kind":"exec","kind":"return","payload":"done"}`,
				`{"kind": }`,
				`{"kind":"exec","payload":"x"} {}`,
				`{"kind":"exec","extra":"x"}`,
				`{"kind":1,"payload":"x"}`,
				`{"kind":"exec","payload":2}`,
				`{"kind":"exec","payload":"x"`,
				`{"kind":"exec","purpose":1,"payload":"x"}`,
				`{"kind":"exec","purpose":"Run nothing.","payload":" \n"}`,
				`{"purpose":"p","y":"a","x":"b"}`,
				corpusDecision(core.Return, "Every malformed decision was rejected."),
			}}},
			marks: map[string][]string{
				"run1-step01-rev2.json":  {"core: invalid decision: top-level value must be an object"},
				"run1-step01-rev3.json":  {"core: invalid decision: duplicate field \"kind\""},
				"run1-step01-rev4.json":  {"core: invalid decision: invalid character '}' looking for beginning of value"},
				"run1-step01-rev5.json":  {"core: invalid decision: trailing value {"},
				"run1-step01-rev6.json":  {"core: invalid decision: unknown field \"extra\""},
				"run1-step01-rev7.json":  {"core: invalid decision: kind must be a string"},
				"run1-step01-rev8.json":  {"core: invalid decision: payload must be a string"},
				"run1-step01-rev9.json":  {"core: invalid decision: EOF\n"},
				"run1-step01-rev10.json": {"core: invalid decision: purpose must be a string"},
				"run1-step01-rev11.json": {"core: invalid decision: exec payload is blank"},
				"run1-step01-rev12.json": {"core: invalid decision: unknown field \"y\""},
			},
		},
		{
			name: "decision-purpose", limits: stepLimits, outputs: map[string]string{"ls /workspace": "go.mod\nmain.go\n", "cat go.mod": "module example\n"},
			runs: []frozenRun{{id: corpusRun, goal: "Name the module in /workspace.", replies: []string{
				corpusPurpose(core.Exec, "List the workspace, then read go.mod.", "ls /workspace"),
				corpusPurpose(core.Exec, "Read the module file\r\nthen name it. "+strings.Repeat("é", 300), "cat go.mod"),
				corpusPurpose(core.Exec, "Check the listing again.", `canti:retrieve {"operation":"read","path":"`+corpusSteps+`/00000001/preview","reason":"Recheck."}`),
				corpusPurpose(core.Return, "The module is named.", "The module is example."),
			}, steps: []string{"1 exited 0 output complete", "2 exited 0 output complete", "3 exited 0 retrieval 1 sources 0 entries"}}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"### Step 1\nPurpose: List the workspace, then read go.mod.\nCommand:\n"},
				"run1-step03-rev1.json": {"### Step 2\nPurpose: Read the module file  then name it. " + strings.Repeat("é", 238) + "…[truncated 124 bytes]\nCommand:\n"},
				"run1-step04-rev1.json": {"### Step 3\nPurpose: Check the listing again.\nCommand:\n"},
			},
		},
		{
			name: "symlinked-evidence", limits: stepLimits, imports: map[string]string{"sub/note": "same original evidence\n"},
			changes: map[string]func(*os.Root) error{"relink": func(r *os.Root) error {
				if err := r.Rename("sub", "target"); err != nil {
					return err
				}
				return r.Symlink("target", "sub")
			}},
			outputs: map[string]string{"relink": "done\n"},
			runs: []frozenRun{{id: corpusRun, goal: "What does the imported note say?", replies: []string{
				corpusRetrieve("read", "/canti/imports/sub/note", "", "Read the note."),
				corpusDecision(core.Exec, "relink"),
				corpusDecision(core.Return, "The note was moved behind a symlink."),
			}, steps: []string{"1 exited 0 retrieval 1 sources 0 entries", "2 exited 0 output complete"}}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"same original evidence\n"},
				"run1-step03-rev1.json": {"same original evidence\n"},
			},
		},
		{
			name: "retrieval-command-at-cap", limits: stepLimits, imports: map[string]string{imported: "notes\n"},
			runs: []frozenRun{{id: corpusRun, goal: "Read the imported notes twice.", replies: []string{
				capped(imported, 2048), capped(imported, 2049),
				corpusDecision(core.Return, "The notes say notes."),
			}, steps: []string{"1 exited 0 retrieval 1 sources 0 entries", "2 exited 0 retrieval 1 sources 0 entries"}}},
			marks: map[string][]string{
				"run1-step03-rev1.json": {"### Step 1\n", "Retrieval: 1 original ranges, 0 directory entries\nRetrieval continuation: ",
					"Retrieval: 1 original ranges, 0 directory entries\nPreview:\n"},
			},
		},
		{
			name: "goal-and-constraints-at-caps", limits: stepLimits,
			runs: []frozenRun{{id: corpusRun, goal: "  " + strings.Repeat("Check every module. ", 1638) + "Then go.\n",
				constraints: "\t" + strings.Repeat("Keep the tree clean. ", 780) + "Stop  ",
				replies:     []string{corpusDecision(core.Return, "Every module is fine.")}}},
			edge: func(t *testing.T, requests map[string]harness.Request) {
				if b := requests["run1-step01-rev1.json"].Context.Bytes; b.Goal != len("## Goal\n")+32768+1 || b.Constraints != len("\n## Constraints\n")+16384+1 {
					t.Errorf("the goal and constraints took %d and %d bytes; want them at their 32768- and 16384-byte caps", b.Goal, b.Constraints)
				}
			},
		},
		{
			name: "removed-evidence", limits: stepLimits,
			imports: map[string]string{imported: "```go\nfunc parse() {}\n```\n" + strings.Repeat("The parser was rewritten in v2.\n", 40) + "func parse is the entry point.\n"},
			changes: map[string]func(*os.Root) error{"sleep 1": func(r *os.Root) error { return r.Remove(imported) }},
			outputs: map[string]string{"sleep 1": "done\n"},
			runs: []frozenRun{{id: corpusRun, goal: "What do the imported notes say about the parser?", replies: []string{
				corpusRetrieve("read", "/canti/imports/"+imported, "", strings.Repeat("Check the imported notes. ", 39)),
				corpusRetrieve("search", "/canti/imports/"+imported, `,"query":"func parse","limit":64`, "Find every mention of parse."),
				corpusDecision(core.Exec, "sleep 1"),
				corpusDecision(core.Return, "The notes say the parser was rewritten in v2."),
			}, steps: []string{"1 exited 0 retrieval 1 sources 0 entries", "2 exited 0 retrieval 2 sources 0 entries", "3 exited 0 output complete"}}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"…[truncated ", "Preview:\n```\n", "Retrieved original evidence:\n````\n", "```go\nfunc parse() {}\n```\n"},
			},
			edge: func(t *testing.T, requests map[string]harness.Request) {
				before, after := evidenceBlocks(requests["run1-step03-rev1.json"].Body), evidenceBlocks(requests["run1-step04-rev1.json"].Body)
				if len(before) != 2 || !slices.Equal(before, after) || !strings.Contains(before[0], "The parser was rewritten in v2.") {
					t.Errorf("the evidence after the source was removed is %q, before it %q; want the same two blocks", after, before)
				}
			},
		},
	})
}

// evidenceBlocks returns the evidence blocks a frozen request body renders.
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
