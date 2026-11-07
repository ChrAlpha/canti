package main

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/host"
)

func TestDecisionFormatPersistsAndDefaults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   []string
		stored string
	}{
		{"default", nil, ""},
		{"text", []string{"-decision-format", "text"}, "text"},
		{"tool", []string{"-decision-format", "tool"}, "tool"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := runSpec(t, tc.args...)
			s.Config.Workspace = "/work"
			run, _, err := host.NewRun("goal", "", s, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			stored, err := host.DecodeConfig(run, "/work")
			if err != nil || stored.DecisionFormat != tc.stored {
				t.Fatalf("decodeConfig = %+v, %v", stored, err)
			}
			if got := host.AnthropicConfig(stored, "http://localhost", host.Keys{AnthropicKey: "test"}).DecisionFormat; got != tc.stored {
				t.Fatalf("model format = %q", got)
			}
		})
	}
	defaults := runSpec(t)
	defaults.Config.Workspace = "/work"
	raw, err := json.Marshal(defaults.Config)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["decision_format"]; ok {
		t.Fatal("default config unexpectedly writes an empty decision format")
	}
	if got, err := host.DecodeConfig(harness.Run{ID: "default", Config: raw}, "/work"); err != nil || got.DecisionFormat != "" {
		t.Fatalf("default config = %+v, %v", got, err)
	}
}

func TestDecisionFormatRejectsUnknownValue(t *testing.T) {
	s := runSpec(t, "-decision-format", "other")
	if _, _, err := host.NewRun("goal", "", s, time.Time{}); err == nil {
		t.Fatal("unknown decision format accepted")
	}
}

func TestDecisionFormatSelectsTheFrozenSystemPrompt(t *testing.T) {
	for _, format := range []string{"", "text", "tool"} {
		t.Run(format, func(t *testing.T) {
			f := newFixture(t, nil)
			args := []string{"-once", "-goal", "freeze one request"}
			if format != "" {
				args = append(args, "-decision-format", format)
			}
			if r := f.cli(nil, "run", args...); r.code != exitActive || len(f.model.requests()) != 0 {
				t.Fatalf("run -once = %+v", r)
			}
			var system string
			for _, e := range f.history() {
				if req := e.Facts.Request; req != nil {
					var body struct{ System string }
					if err := json.Unmarshal(req.Body, &body); err != nil {
						t.Fatal(err)
					}
					system = body.System
				}
			}
			tool := format == "tool"
			if strings.Contains(system, "Call the canti_decision tool exactly once per reply; never reply with text.") != tool ||
				strings.Contains(system, "The purpose and payload are JSON strings, so escape quotes, backslashes, and newlines.") == tool {
				t.Fatalf("format %q froze system:\n%s", format, system)
			}
		})
	}
}

func TestDecisionFormatAdmitsContextByItsSystem(t *testing.T) {
	smallest := func(format string) int {
		admits := func(n int) bool {
			s := runSpec(t, "-decision-format", format, "-context-bytes", strconv.Itoa(n))
			s.Config.Workspace = "/work"
			_, _, err := host.NewRun("goal", "", s, time.Time{})
			return err == nil
		}
		lo, hi := 1, 1<<20
		if admits(lo) || !admits(hi) {
			t.Fatalf("format %s: no admission boundary in %d..%d", format, lo, hi)
		}
		for lo+1 < hi {
			if mid := (lo + hi) / 2; admits(mid) {
				hi = mid
			} else {
				lo = mid
			}
		}
		return hi
	}
	if text, tool := smallest("text"), smallest("tool"); tool >= text {
		t.Fatalf("smallest admitted context: text %d, tool %d bytes; the shorter tool system must admit less", text, tool)
	}
}
