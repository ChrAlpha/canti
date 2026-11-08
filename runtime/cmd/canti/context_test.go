package main

import (
	"encoding/json"
	"flag"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/host"
)

func TestContextConfigurationSurvivesResume(t *testing.T) {
	imported := t.TempDir()
	s := runSpec(t, "-history-imports", imported, "-request-bytes", "1048576")
	s.Config.Workspace = t.TempDir()
	run, _, err := host.NewRun("goal", "constraints", s, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	c, err := host.DecodeConfig(run, s.Config.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	if c.ContextVersion != host.ContextVersion || c.HistoryImports != imported || c.RequestBytes != 1048576 {
		t.Fatalf("config lost on resume: %#v", c)
	}
}

func TestContextConfigurationRejectsInvalidLimits(t *testing.T) {
	for _, args := range [][]string{{"-request-bytes", "-1"}, {"-request-bytes", "0"}, {"-request-bytes", "8388609"}} {
		if _, _, err := host.NewRun("goal", "", runSpec(t, args...), time.Now()); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

func TestRemovedContextStrategiesAreRejected(t *testing.T) {
	for _, name := range []string{"retrieval", "without-handover", "context-bytes", "window-steps", "window-bytes", "handover-bytes"} {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		var s host.Spec
		registerSpec(fs, &s)
		if err := fs.Parse([]string{"-" + name}); err == nil {
			t.Fatalf("accepted removed flag %s", name)
		}
	}
}

func TestDecodeConfigRejectsUnsupportedStoredContextVersions(t *testing.T) {
	s := runSpec(t)
	run, _, err := host.NewRun("goal", "", s, time.Now())
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
		if _, err := host.DecodeConfig(run, s.Config.Workspace); err == nil || !strings.Contains(err.Error(), "unsupported context version") || !strings.Contains(err.Error(), "use fresh state") {
			t.Fatalf("accepted unsupported version %v: %v", version, err)
		}
	}
	for _, config := range []string{"", `{"context_version":2,"history":8}`} {
		run.Config = []byte(config)
		if _, err := host.DecodeConfig(run, s.Config.Workspace); err == nil {
			t.Errorf("accepted config %q", config)
		}
	}
}
