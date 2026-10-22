package main

import (
	"encoding/json"
	"flag"
	"io"
	"testing"
	"time"
)

func TestContextConfigurationSurvivesResume(t *testing.T) {
	imported := t.TempDir()
	s := parseSpec(t, "-history-imports", imported, "-compact-steps", "3", "-compact-bytes", "32768", "-summary-bytes", "1024")
	s.config.Workspace = t.TempDir()
	run, _, err := newRun("goal", "constraints", s, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	c, err := decodeConfig(run, s.config.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	if c.ContextVersion != contextVersion || c.HistoryImports != imported || c.CompactSteps != 3 || c.CompactBytes != 32768 || c.SummaryBytes != 1024 {
		t.Fatalf("config lost on resume: %#v", c)
	}
}

func TestContextConfigurationRejectsInvalidLimits(t *testing.T) {
	for _, args := range [][]string{{"-compact-steps", "-1"}, {"-compact-bytes", "-1"}, {"-summary-bytes", "0"}, {"-summary-bytes", "65537"}} {
		if _, _, err := newRun("goal", "", parseSpec(t, args...), time.Now()); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

func TestRemovedContextStrategiesAreRejected(t *testing.T) {
	for _, name := range []string{"retrieval", "without-handover"} {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		var s spec
		s.register(fs)
		if err := fs.Parse([]string{"-" + name}); err == nil {
			t.Fatalf("accepted removed flag %s", name)
		}
	}
}

func TestDecodeConfigKeepsAnyStoredContextVersion(t *testing.T) {
	s := parseSpec(t)
	run, _, err := newRun("goal", "", s, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var stored map[string]any
	if err := json.Unmarshal(run.Config, &stored); err != nil {
		t.Fatal(err)
	}
	for _, version := range []any{nil, 0, 1, 3} {
		if version == nil {
			delete(stored, "context_version")
		} else {
			stored["context_version"] = version
		}
		run.Config, err = json.Marshal(stored)
		if err != nil {
			t.Fatal(err)
		}
		want := s.config
		want.ContextVersion, _ = version.(int)
		if c, err := decodeConfig(run, s.config.Workspace); err != nil || c != want {
			t.Fatalf("context version %v decodes as %+v, %v\nwant %+v", version, c, err, want)
		}
	}
	for _, config := range []string{"", `{"context_version":2,"history":8}`} {
		run.Config = []byte(config)
		if _, err := decodeConfig(run, s.config.Workspace); err == nil {
			t.Errorf("accepted config %q", config)
		}
	}
}
