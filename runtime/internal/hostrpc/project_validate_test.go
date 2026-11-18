package hostrpc

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/host"
	sdk "github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func TestValidateAndSubmitAgreeOnAQuiescentSteer(t *testing.T) {
	h, c := serving(t)
	state := filepath.Dir(c.Path)
	if err := os.WriteFile(filepath.Join(h.Workspace(), "AGENTS.md"), []byte(strings.Repeat("a", (64<<10)+1)), 0o644); err != nil {
		t.Fatal(err)
	}
	spec := sdk.Spec{Ephemeral: true, Workspace: h.Workspace(), Image: "debian:bookworm-slim", Network: "none", ContextVersion: host.ContextVersion,
		API: "anthropic-messages", Model: "m", MaxTokens: 1000, OutputLimit: 8 << 20, RequestBytes: 256 << 10,
		MaxSteps: 20, MaxAttempts: 3, WallTime: "30m", JobTimeout: "10m", OutputBudget: 8 << 20}
	submission := sdk.Submission{Goal: "steer", Delivery: "steer", Spec: spec}
	binding := sdk.Binding{AnthropicKey: "test-credential-key"}
	_, err := c.Validate(context.Background(), sdk.Validation{State: state, Submission: submission, Binding: binding})
	if err == nil || !strings.Contains(err.Error(), "65537 bytes") {
		t.Fatalf("quiescent steer validation = %v", err)
	}
	if _, err := c.Submit(context.Background(), submission, binding, true); err == nil || !strings.Contains(err.Error(), "65537 bytes") {
		t.Fatalf("quiescent steer submit = %v", err)
	}
	if _, err := h.View(context.Background(), ""); err == nil {
		t.Fatal("a refused steer created a run")
	}
}
