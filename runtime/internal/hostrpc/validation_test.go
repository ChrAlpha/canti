package hostrpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/host"
	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
	wire "github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "canti-hostrpc-helper-")
	if err != nil {
		panic(err)
	}
	helper := filepath.Join(dir, "canti-job")
	if err := os.WriteFile(helper, fmt.Appendf(nil, "#!/bin/sh\necho %d\n", jobrunner.Protocol), 0o555); err != nil {
		panic(err)
	}
	if err := os.Setenv("CANTI_JOB_HELPER", helper); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func TestValidationOfASteerNeedsNoNewRunWindow(t *testing.T) {
	h, _, client, _ := servingWith(t, slog.New(slog.DiscardHandler))
	state := filepath.Dir(client.Path)
	spec := host.Spec{
		Config: host.RunConfig{Ephemeral: true, Workspace: h.Workspace(), Image: "debian:bookworm-slim", Network: "none", Memory: "2g", Pids: 512, OutputLimit: 8 << 20,
			API: host.APIMessages, Model: "m", MaxTokens: 1000, Structured: true, RequestBytes: 256 << 10, ContextVersion: host.ContextVersion},
		Budget: harness.Budget{ContextTokens: 200_000},
		Limits: core.Limits{MaxSteps: 20, MaxAttempts: 2},
	}
	opened, err := h.Submit(context.Background(), host.Submission{Goal: "working", Spec: spec,
		Binding: host.Binding{Provider: "http://127.0.0.1:1", Keys: host.Keys{AnthropicKey: "test-credential-key"}}})
	if err != nil || !opened.Opened() || opened.Run.Budget.ContextTokens != 200_000 {
		t.Fatalf("opening run = %+v, %v", opened, err)
	}
	steer := SpecOf(spec)
	steer.ContextTokens = 0
	validation := wire.Validation{State: state, Submission: wire.Submission{Goal: "steer", Delivery: "steer", Spec: steer},
		Binding: wire.Binding{Provider: "http://127.0.0.1:1", AnthropicKey: "test-credential-key"}}
	if v, err := client.Validate(context.Background(), validation); err != nil || v.State != state {
		t.Fatalf("steer validation = %+v, %v", v, err)
	}
	ack, err := client.Submit(context.Background(), validation.Submission, validation.Binding, false)
	if err != nil || ack.Disposition != "steer" || ack.Run != nil {
		t.Fatalf("steer submission = %+v, %v", ack, err)
	}
}

func TestServingHostValidateRejectsWrongLocationFirst(t *testing.T) {
	h, _, client, _ := servingWith(t, slog.New(slog.DiscardHandler))
	state := filepath.Dir(client.Path)
	base := wire.Validation{
		State: state,
		Submission: wire.Submission{
			Goal: "input",
			Spec: wire.Spec{Ephemeral: true, Workspace: h.Workspace(), Image: "debian:bookworm-slim", Network: "none",
				API: "anthropic-messages", Model: "m", MaxTokens: 1000, OutputLimit: 8 << 20, RequestBytes: 262144,
				MaxSteps: 20, MaxAttempts: 3, WallTime: "30m", JobTimeout: "10m", OutputBudget: 8 << 20, ContextTokens: 200_000},
		},
		Binding: wire.Binding{AnthropicKey: "test-credential-key"},
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
