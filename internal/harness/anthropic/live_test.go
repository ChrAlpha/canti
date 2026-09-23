//go:build live

package anthropic_test

import (
	"context"
	"os"
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
		User:   `Hand control back to the caller with the payload "ok".`,
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
