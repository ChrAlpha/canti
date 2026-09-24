package main

import (
	"testing"
	"time"
)

func TestContextConfigurationSurvivesResume(t *testing.T) {
	archive := t.TempDir()
	s := parseSpec(t, "-retrieval", "-history-imports", archive, "-window-steps", "3", "-window-bytes", "32768", "-handover-bytes", "1024")
	s.config.Workspace = t.TempDir()
	run, _, err := newRun("goal", "constraints", s, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	c, err := decodeConfig(run, s.config.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Retrieval || c.HistoryImports != archive || c.WindowSteps != 3 || c.WindowBytes != 32768 || c.HandoverBytes != 1024 {
		t.Fatalf("config lost on resume: %#v", c)
	}
}

func TestContextConfigurationRejectsInvalidLimits(t *testing.T) {
	for _, args := range [][]string{{"-window-steps", "-1"}, {"-window-bytes", "-1"}, {"-handover-bytes", "0"}, {"-handover-bytes", "8193"}, {"-history-imports", t.TempDir()}} {
		if _, _, err := newRun("goal", "", parseSpec(t, args...), time.Now()); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
