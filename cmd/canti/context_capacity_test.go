package main

import (
	"strings"
	"testing"
	"time"
)

func TestRunRejectsContextWithoutRoomForMaximumHandover(t *testing.T) {
	s := parseSpec(t, "-context-bytes", "65536", "-handover-bytes", "8192", "-window-steps", "1")
	if _, _, err := newRun(strings.Repeat("g", 23000), "", s, time.Now()); err == nil {
		t.Fatal("accepted a run whose next window cannot accommodate a legal handover")
	}
}

func TestRunAcceptsDefaultHandoverAtMinimumContext(t *testing.T) {
	s := parseSpec(t, "-context-bytes", "65536")
	if _, _, err := newRun("Complete the task", "Preserve evidence", s, time.Now()); err != nil {
		t.Fatal(err)
	}
}
