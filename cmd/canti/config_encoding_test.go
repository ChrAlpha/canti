package main

import (
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
)

func TestStoredRunConfigKeepsItsEncoding(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   []string
		stored string
		want   config
	}{
		{
			name: "defaults",
			stored: `{"workspace":"/srv/project","image":"debian:bookworm-slim","network":"none","memory":"2g","cpus":"","pids":512,` +
				`"output_limit":8388608,"model":"claude-opus-5","max_tokens":16000,"structured":true,"fallbacks":true,` +
				`"context_bytes":262144,"context_version":11,"handover_bytes":2048}`,
			want: config{Workspace: "/srv/project", Image: "debian:bookworm-slim", Network: "none", Memory: "2g", Pids: 512,
				OutputLimit: 8388608, Model: "claude-opus-5", MaxTokens: 16000, Structured: true, Fallbacks: true,
				ContextBytes: 262144, ContextVersion: 11, HandoverBytes: 2048},
		},
		{
			name: "every field",
			args: []string{"-image", "alpine:3.23", "-network", "bridge", "-memory", "512m", "-cpus", "1.5", "-pids", "64",
				"-output-limit", "1048576", "-model", "claude-sonnet-5", "-max-tokens", "4096", "-structured=false",
				"-decision-format", "tool", "-fallbacks=false", "-context-bytes", "131072", "-history-imports", "/",
				"-window-steps", "12", "-window-bytes", "65536", "-handover-bytes", "1024"},
			stored: `{"workspace":"/srv/project","image":"alpine:3.23","network":"bridge","memory":"512m","cpus":"1.5","pids":64,` +
				`"output_limit":1048576,"model":"claude-sonnet-5","max_tokens":4096,"structured":false,"decision_format":"tool",` +
				`"fallbacks":false,"context_bytes":131072,"context_version":11,"history_imports":"/","window_steps":12,` +
				`"window_bytes":65536,"handover_bytes":1024}`,
			want: config{Workspace: "/srv/project", Image: "alpine:3.23", Network: "bridge", Memory: "512m", CPUs: "1.5", Pids: 64,
				OutputLimit: 1048576, Model: "claude-sonnet-5", MaxTokens: 4096, DecisionFormat: "tool",
				ContextBytes: 131072, ContextVersion: 11, HistoryImports: "/", WindowSteps: 12, WindowBytes: 65536, HandoverBytes: 1024},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := parseSpec(t, tc.args...)
			s.config.Workspace = "/srv/project"
			run, _, err := newRun("Fix the failing test.", "", s, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if string(run.Config) != tc.stored {
				t.Errorf("stored config\n%s\nwant\n%s", run.Config, tc.stored)
			}
			got, err := decodeConfig(harness.Run{ID: "run_stored", Config: []byte(tc.stored)}, "/srv/project")
			if err != nil || got != tc.want {
				t.Errorf("stored config decodes as %+v, %v\nwant %+v", got, err, tc.want)
			}
		})
	}
}
