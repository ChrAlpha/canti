package main

import (
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/host"
)

func TestStoredRunConfigKeepsItsEncoding(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   []string
		stored string
		want   host.RunConfig
	}{
		{
			name: "defaults",
			args: []string{"-api", "openai-responses", "-model", "test-model"},
			stored: `{"workspace":"/srv/project","image":"debian:bookworm-slim","network":"none","memory":"2g","cpus":"","pids":512,` +
				`"output_limit":8388608,"api":"openai-responses","model":"test-model","max_tokens":16000,"structured":true,"fallbacks":false,` +
				`"context_bytes":262144,"context_version":13,"handover_bytes":2048}`,
			want: host.RunConfig{Workspace: "/srv/project", Image: "debian:bookworm-slim", Network: "none", Memory: "2g", Pids: 512,
				OutputLimit: 8388608, API: "openai-responses", Model: "test-model", MaxTokens: 16000, Structured: true,
				ContextBytes: 262144, ContextVersion: 13, HandoverBytes: 2048},
		},
		{
			name: "every field",
			args: []string{"-image", "alpine:3.23", "-network", "bridge", "-memory", "512m", "-cpus", "1.5", "-pids", "64",
				"-output-limit", "1048576", "-api", "anthropic-messages", "-model", "claude-sonnet-5", "-max-tokens", "4096", "-effort", "low", "-structured=false",
				"-decision-format", "tool", "-fallbacks", "-context-bytes", "131072", "-history-imports", "/",
				"-window-steps", "12", "-window-bytes", "65536", "-handover-bytes", "1024"},
			stored: `{"workspace":"/srv/project","image":"alpine:3.23","network":"bridge","memory":"512m","cpus":"1.5","pids":64,` +
				`"output_limit":1048576,"api":"anthropic-messages","model":"claude-sonnet-5","max_tokens":4096,"effort":"low","structured":false,"decision_format":"tool",` +
				`"fallbacks":true,"context_bytes":131072,"context_version":13,"history_imports":"/","window_steps":12,` +
				`"window_bytes":65536,"handover_bytes":1024}`,
			want: host.RunConfig{Workspace: "/srv/project", Image: "alpine:3.23", Network: "bridge", Memory: "512m", CPUs: "1.5", Pids: 64,
				OutputLimit: 1048576, API: "anthropic-messages", Model: "claude-sonnet-5", MaxTokens: 4096, Effort: "low", DecisionFormat: "tool", Fallbacks: true,
				ContextBytes: 131072, ContextVersion: 13, HistoryImports: "/", WindowSteps: 12, WindowBytes: 65536, HandoverBytes: 1024},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := parseSpec(t, tc.args...)
			s.Config.Workspace = "/srv/project"
			run, _, err := host.NewRun("Fix the failing test.", "", s, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if string(run.Config) != tc.stored {
				t.Errorf("stored config\n%s\nwant\n%s", run.Config, tc.stored)
			}
			got, err := host.DecodeConfig(harness.Run{ID: "run_stored", Config: []byte(tc.stored)}, "/srv/project")
			if err != nil || got != tc.want {
				t.Errorf("stored config decodes as %+v, %v\nwant %+v", got, err, tc.want)
			}
		})
	}
}
