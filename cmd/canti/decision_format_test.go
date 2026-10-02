package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
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
			s := parseSpec(t, tc.args...)
			s.config.Workspace = "/work"
			run, _, err := newRun("goal", "", s, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			stored, err := decodeConfig(run, "/work")
			if err != nil || stored.DecisionFormat != tc.stored {
				t.Fatalf("decodeConfig = %+v, %v", stored, err)
			}
			if got := modelConfig(stored, "http://localhost", credential{apiKey: "test"}).DecisionFormat; got != tc.stored {
				t.Fatalf("model format = %q", got)
			}
		})
	}
	old := parseSpec(t)
	old.config.Workspace = "/work"
	raw, err := json.Marshal(old.config)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["decision_format"]; ok {
		t.Fatal("old config shape changed")
	}
	if got, err := decodeConfig(harness.Run{ID: "old", Config: raw}, "/work"); err != nil || got.DecisionFormat != "" {
		t.Fatalf("old config = %+v, %v", got, err)
	}
}

func TestDecisionFormatRejectsUnknownValue(t *testing.T) {
	s := parseSpec(t, "-decision-format", "other")
	if _, _, err := newRun("goal", "", s, time.Time{}); err == nil {
		t.Fatal("unknown decision format accepted")
	}
}
