//go:build live

package anthropic_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
)

func TestLiveDecision(t *testing.T) {
	key, model := os.Getenv("ANTHROPIC_API_KEY"), os.Getenv("ANTHROPIC_MODEL")
	if key == "" || model == "" {
		t.Skip("ANTHROPIC_API_KEY and ANTHROPIC_MODEL name the credential and model")
	}
	effort := os.Getenv("CANTI_LIVE_EFFORT")
	format := os.Getenv("CANTI_LIVE_DECISION_FORMAT")
	system := `Reply with one JSON object {"kind":"exec"|"return","payload":string}.`
	if format == "tool" {
		system = "Call the canti_decision tool exactly once; never reply with text."
	}
	m, err := anthropic.New(anthropic.Config{BaseURL: os.Getenv("ANTHROPIC_BASE_URL"), APIKey: key, Model: model, MaxTokens: 1024,
		Effort: effort, DecisionFormat: format, StructuredOutput: os.Getenv("CANTI_LIVE_STRUCTURED") != "0", Timeout: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	req, err := m.Freeze(harness.Prompt{
		System: system,
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
