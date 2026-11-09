package main

import (
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/host"
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
			args: []string{"-ephemeral", "-api", "openai-responses", "-model", "test-model"},
			stored: `{"ephemeral":true,"workspace":"/srv/project","image":"debian:bookworm-slim","network":"none","memory":"2g","cpus":"","pids":512,` +
				`"output_limit":8388608,"api":"openai-responses","model":"test-model","provider":"http://127.0.0.1:1","max_tokens":16000,"structured":true,"fallbacks":false,` +
				`"request_bytes":8388608,"context_version":18}`,
			want: host.RunConfig{Ephemeral: true, Workspace: "/srv/project", Image: "debian:bookworm-slim", Network: "none", Memory: "2g", Pids: 512,
				OutputLimit: 8388608, API: "openai-responses", Model: "test-model", Provider: "http://127.0.0.1:1", MaxTokens: 16000, Structured: true,
				RequestBytes: 8388608, ContextVersion: 18},
		},
		{
			name: "every field",
			args: []string{"-ephemeral", "-image", "alpine:3.23", "-network", "bridge", "-memory", "512m", "-cpus", "1.5", "-pids", "64",
				"-output-limit", "1048576", "-api", "anthropic-messages", "-model", "claude-sonnet-5", "-max-tokens", "4096", "-effort", "low", "-structured=false",
				"-decision-format", "tool", "-fallbacks", "-request-bytes", "131072", "-history-imports", "/"},
			stored: `{"ephemeral":true,"workspace":"/srv/project","image":"alpine:3.23","network":"bridge","memory":"512m","cpus":"1.5","pids":64,` +
				`"output_limit":1048576,"api":"anthropic-messages","model":"claude-sonnet-5","provider":"http://127.0.0.1:1","max_tokens":4096,"effort":"low","structured":false,"decision_format":"tool",` +
				`"fallbacks":true,"request_bytes":131072,"context_version":18,"history_imports":"/"}`,
			want: host.RunConfig{Ephemeral: true, Workspace: "/srv/project", Image: "alpine:3.23", Network: "bridge", Memory: "512m", CPUs: "1.5", Pids: 64,
				OutputLimit: 1048576, API: "anthropic-messages", Model: "claude-sonnet-5", Provider: "http://127.0.0.1:1", MaxTokens: 4096, Effort: "low", DecisionFormat: "tool", Fallbacks: true,
				RequestBytes: 131072, ContextVersion: 18, HistoryImports: "/"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := parseSpec(t, tc.args...)
			s.Config.Workspace = "/srv/project"
			s.Config.Provider = "http://127.0.0.1:1"
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
