package hostrpc

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/host"
	sdk "github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func paritySpec(t *testing.T, h *host.Host) sdk.Spec {
	t.Helper()
	return sdk.Spec{Ephemeral: true, Workspace: h.Workspace(), Image: "debian:bookworm-slim", Network: "none",
		ContextVersion: host.ContextVersion, API: "anthropic-messages", Model: "m", MaxTokens: 1000, OutputLimit: 8 << 20,
		RequestBytes: 256 << 10, MaxSteps: 20, MaxAttempts: 3, WallTime: "30m", JobTimeout: "10m", OutputBudget: 8 << 20,
		ContextTokens: 1}
}

func TestValidateAndSubmitAgreeOnWorkingAndQuiescentSubmissions(t *testing.T) {
	h, c := serving(t)
	state := filepath.Dir(c.Path)
	binding := sdk.Binding{AnthropicKey: "test-credential-key"}
	if _, err := c.Submit(context.Background(), sdk.Submission{Goal: "open", Spec: paritySpec(t, h)}, binding, true); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		sub    sdk.Submission
		b      sdk.Binding
		refuse bool
	}{
		{"working missing delivery", sdk.Submission{Goal: "join", Spec: paritySpec(t, h)}, binding, true},
		{"working steer ignores request cap", sdk.Submission{Goal: "steer", Delivery: "steer", Spec: func() sdk.Spec { s := paritySpec(t, h); s.RequestBytes = 1; return s }()}, binding, false},
		{"working follow-up invalid provider", sdk.Submission{Goal: "follow", Delivery: "follow_up", Spec: paritySpec(t, h)}, sdk.Binding{Provider: "://bad", AnthropicKey: "test-credential-key"}, true},
		{"working malformed input ID", sdk.Submission{InputID: "bad id", Goal: "steer", Delivery: "steer", Spec: paritySpec(t, h)}, binding, true},
		{"working steer unsupported context version", sdk.Submission{Goal: "steer", Delivery: "steer", Spec: func() sdk.Spec { s := paritySpec(t, h); s.ContextVersion = host.ContextVersion - 1; return s }()}, binding, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, verr := c.Validate(context.Background(), sdk.Validation{State: state, Submission: tc.sub, Binding: tc.b})
			_, serr := c.Submit(context.Background(), tc.sub, tc.b, true)
			if (verr != nil) != tc.refuse || (serr != nil) != tc.refuse {
				t.Fatalf("validate=%v submit=%v, want refuse=%t", verr, serr, tc.refuse)
			}
		})
	}
	h2, c2 := serving(t)
	state2 := filepath.Dir(c2.Path)
	unknown := sdk.Submission{InputID: "valid-id", Goal: "unknown", Delivery: "bogus", Spec: paritySpec(t, h2)}
	_, verr := c2.Validate(context.Background(), sdk.Validation{State: state2, Submission: unknown, Binding: binding})
	_, serr := c2.Submit(context.Background(), unknown, binding, true)
	if verr == nil || serr == nil || !strings.Contains(verr.Error(), "unknown delivery") || !strings.Contains(serr.Error(), "unknown delivery") {
		t.Fatalf("validate=%v submit=%v", verr, serr)
	}
	malformed := sdk.Submission{InputID: "bad id", Goal: "open", Spec: paritySpec(t, h2)}
	_, verr = c2.Validate(context.Background(), sdk.Validation{State: state2, Submission: malformed, Binding: binding})
	_, serr = c2.Submit(context.Background(), malformed, binding, true)
	if verr == nil || serr == nil || !strings.Contains(verr.Error(), "input ID") || !strings.Contains(serr.Error(), "input ID") {
		t.Fatalf("malformed validate=%v submit=%v", verr, serr)
	}
}
