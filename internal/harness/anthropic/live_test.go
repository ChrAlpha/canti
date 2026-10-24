//go:build live

package anthropic_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
)

func TestLiveDecision(t *testing.T) {
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		t.Skip("ANTHROPIC_API_KEY is not set")
	}
	model := os.Getenv("ANTHROPIC_MODEL")
	if model == "" {
		model = "claude-opus-5"
	}
	m, err := anthropic.New(anthropic.Config{BaseURL: os.Getenv("ANTHROPIC_BASE_URL"), APIKey: key, Model: model, MaxTokens: 1024,
		StructuredOutput: os.Getenv("CANTI_LIVE_STRUCTURED") != "0", Timeout: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	req, err := m.Freeze(harness.Prompt{
		System: `Reply with one JSON object {"kind":"exec"|"return","payload":string}.`,
		Blocks: []string{"Hand control back to the caller ", `with the payload "ok".`},
		Stable: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	resp, err := m.Complete(ctx, req)
	if err != nil || !resp.Complete {
		t.Fatalf("Complete = %v, %v: %s", resp.Complete, err, resp.Body)
	}
	d, err := core.Decode(m.Decision(resp))
	if err != nil {
		t.Fatalf("core.Decode: %v: %s", err, resp.Body)
	}
	t.Logf("model %s decision %+v usage %+v", resp.Model, d, resp.Usage)
}

// Opt in explicitly: at most eight calls (twelve with the aged-cache check),
// each capped at 1,024 output tokens and two minutes. No credentials are logged.
func TestLiveCompactionPrefix(t *testing.T) {
	if os.Getenv("CANTI_LIVE_COMPACTION") != "1" {
		t.Skip("set CANTI_LIVE_COMPACTION=1 for bounded provider acceptance")
	}
	if os.Getenv("ANTHROPIC_API_KEY") == "" && os.Getenv("ANTHROPIC_AUTH_TOKEN") == "" {
		t.Skip("model authentication is not configured")
	}
	for _, transport := range []string{"text", "tool"} {
		t.Run(transport, func(t *testing.T) { liveCompactionPrefix(t, transport) })
	}
}

func liveCompactionPrefix(t *testing.T, transport string) {
	model := os.Getenv("ANTHROPIC_MODEL")
	if model == "" {
		model = "claude-opus-5"
	}
	m, err := anthropic.New(anthropic.Config{BaseURL: os.Getenv("ANTHROPIC_BASE_URL"),
		APIKey: os.Getenv("ANTHROPIC_API_KEY"), AuthToken: os.Getenv("ANTHROPIC_AUTH_TOKEN"),
		Model: model, MaxTokens: 1024, DecisionFormat: transport,
		StructuredOutput: os.Getenv("CANTI_LIVE_STRUCTURED") != "0", Timeout: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	f := harness.Frame{RunID: "cache-acceptance", Goal: "The supplied observations are already committed. Return a brief readiness acknowledgement; execute nothing.",
		Constraints: "Project ORION-4817 is read-only. The approved transport is sync; async was rejected. Never repeat completed effects.",
		Environment: "Isolated acceptance workspace " + rand.Text(), Step: 21, MaxSteps: 30, Attempt: 1, MaxAttempts: 3,
		Window: &harness.Window{FirstStep: 1}, DecisionTool: transport == "tool"}
	for n := 1; n <= 20; n++ {
		var evidence strings.Builder
		for i := 0; i < 50; i++ {
			fmt.Fprintf(&evidence, "Verified file source_%02d_%02d.go: deterministic parser boundary, explicit status, original evidence retained.\n", n, i)
		}
		f.Steps = append(f.Steps, harness.StepRecord{Step: n, Command: fmt.Sprintf("inspect source_%02d", n), Observation: harness.Observation{Execution: core.ExecutionExited, Preview: evidence.String()}})
	}
	p, err := f.Prompt(harness.DefaultPromptLimits())
	if err != nil {
		t.Fatal(err)
	}
	p.CacheThrough = p.Stable - 2
	parent, err := m.Freeze(p)
	if err != nil {
		t.Fatal(err)
	}
	liveCall(t, m, parent, "warm-parent")
	if transport == "text" && os.Getenv("CANTI_LIVE_COMPACTION_AGED") == "1" {
		for i := 0; i < 4; i++ {
			select {
			case <-time.After(90 * time.Second):
			case <-t.Context().Done():
				t.Fatal(t.Context().Err())
			}
			liveCall(t, m, parent, "refresh-latest-parent")
		}
	}
	instruction := harness.CompactionPromptPrefix + "\nReturn only continuation context in the payload of a return decision. Preserve the project identifier, access restriction, approved transport, rejected approach, and the rule about completed effects. Keep it below 2,048 UTF-8 bytes."
	fork, err := m.FreezeSummary(harness.SummaryInput{Parent: &parent, Blocks: p.CacheThrough, Instruction: instruction}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	warm := liveCall(t, m, fork, "warm-prefix-summary")
	summary, err := m.Summary(warm)
	if err != nil {
		t.Fatalf("summary decoding: %v", err)
	}
	if err := harness.ValidateSummary(summary, 2048); err != nil {
		t.Fatal(err)
	}
	// A nonce before all content makes the control cold while preserving its
	// history, output protocol and generation allowance.
	coldPrompt := p
	coldPrompt.System = "Cold control " + rand.Text() + "\n" + p.System
	coldParent, err := m.Freeze(coldPrompt)
	if err != nil {
		t.Fatal(err)
	}
	coldFork, err := m.FreezeSummary(harness.SummaryInput{Parent: &coldParent, Blocks: p.CacheThrough, Instruction: instruction}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	cold := liveCall(t, m, coldFork, "cold-prefix-control")
	if warm.Usage.Cache == nil || cold.Usage.Cache == nil || !warm.UsageKnown || !cold.UsageKnown {
		t.Fatal("provider did not expose complete cache usage; acceptance is unproven")
	}
	if warm.Usage.Cache.ReadTokens <= cold.Usage.Cache.ReadTokens {
		t.Fatal("warm compact did not read more cache than the cold control")
	}
	if warm.Usage.InputTokens <= 0 || float64(warm.Usage.Cache.ReadTokens)/float64(warm.Usage.InputTokens) < 0.8 {
		t.Fatal("warm compact reused less than 80 percent of its input; the long prefix is not established")
	}
	resume := harness.Frame{RunID: "continued", Goal: "From the inherited summary, report the project identifier, access restriction, approved transport, and whether completed effects may be repeated. Return a JSON object encoded inside payload with keys project, access, transport, repeat_completed_effects (boolean). Execute nothing.",
		Environment: "Isolated acceptance workspace", Step: 1, MaxSteps: 3, Attempt: 1, MaxAttempts: 3,
		Window: &harness.Window{FirstStep: 1, Through: 18, Summary: summary}, DecisionTool: transport == "tool"}
	next, err := resume.Prompt(harness.DefaultPromptLimits())
	if err != nil {
		t.Fatal(err)
	}
	req, err := m.Freeze(next)
	if err != nil {
		t.Fatal(err)
	}
	response := liveCall(t, m, req, "summary-continuation")
	d, err := core.Decode(m.Decision(response))
	if err != nil || d.Kind != core.Return {
		t.Fatalf("continuation did not return: %v", err)
	}
	var facts struct {
		Project   string `json:"project"`
		Access    string `json:"access"`
		Transport string `json:"transport"`
		Repeat    *bool  `json:"repeat_completed_effects"`
	}
	if err := json.Unmarshal([]byte(d.Payload), &facts); err != nil || facts.Project != "ORION-4817" || facts.Access != "read-only" || facts.Transport != "sync" || facts.Repeat == nil || *facts.Repeat {
		t.Fatalf("continuation lost its inherited contract: %+v, %v", facts, err)
	}
}

func liveCall(t *testing.T, m *anthropic.Model, req harness.Request, phase string) harness.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	started := time.Now()
	r, err := m.Complete(ctx, req)
	if err != nil {
		t.Fatalf("%s: %v", phase, err)
	}
	t.Logf("LIVE_COMPACT phase=%s model=%s request_bytes=%d input=%d output=%d cache=%+v usage_known=%t elapsed_ms=%d", phase, r.Model, len(req.Body), r.Usage.InputTokens, r.Usage.OutputTokens, r.Usage.Cache, r.UsageKnown, time.Since(started).Milliseconds())
	return r
}
