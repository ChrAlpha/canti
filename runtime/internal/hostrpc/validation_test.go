package hostrpc

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/host"
	wire "github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func TestServingHostValidateRejectsWrongLocationFirst(t *testing.T) {
	h, _, client, _ := servingWith(t, slog.New(slog.DiscardHandler))
	state := filepath.Dir(client.Path)
	base := wire.Validation{
		State: state,
		Submission: wire.Submission{
			Goal: "input",
			Spec: wire.Spec{Ephemeral: true, Workspace: h.Workspace(), Image: "debian:bookworm-slim", Network: "none",
				API: "anthropic-messages", Model: "m", MaxTokens: 1000, OutputLimit: 8 << 20, RequestBytes: 262144,
				MaxSteps: 20, MaxAttempts: 3, WallTime: "30m", JobTimeout: "10m", OutputBudget: 8 << 20},
		},
		Binding: wire.Binding{AnthropicKey: "test-key"},
	}
	if v, err := client.Validate(context.Background(), base); err != nil || v.State != state || v.Spec.Workspace != h.Workspace() {
		t.Fatalf("control validation = %+v, %v", v, err)
	}
	for _, tc := range []struct {
		name   string
		change func(*wire.Validation) string
	}{
		{"wrong state", func(v *wire.Validation) string {
			v.State = filepath.Join(t.TempDir(), "other-state")
			return v.State
		}},
		{"wrong workspace", func(v *wire.Validation) string {
			v.Submission.Spec.Workspace = filepath.Join(t.TempDir(), "other-workspace")
			return v.Submission.Spec.Workspace
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validation := base
			wrong := tc.change(&validation)
			_, err := client.Validate(context.Background(), validation)
			var spec wire.SpecError
			if !errors.As(err, &spec) || !strings.Contains(err.Error(), "this host's directories") || strings.Contains(err.Error(), wrong) {
				t.Fatalf("validation at wrong location = %v; want the location error that names no path", err)
			}
			if _, err := Validate(context.Background(), &validation, h); !errors.As(err, new(host.SpecError)) {
				t.Fatalf("direct validation at wrong location = %v", err)
			}
		})
	}
	if _, err := os.Stat(base.Submission.Spec.Workspace); err != nil {
		t.Fatal(err)
	}
}
