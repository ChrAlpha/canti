package harness_test

import (
	"encoding/json"
	"strings"

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
