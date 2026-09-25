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

func TestRunRejectsContextWithoutRetrievalReserve(t *testing.T) {
	s := parseSpec(t, "-context-bytes", "65536")
	if _, _, err := newRun("Complete the task", "Preserve evidence", s, time.Now()); err == nil || !strings.Contains(err.Error(), "handover reserve") {
		t.Fatalf("accepted context without room for a full retrieval and handover: %v", err)
	}
}

func TestRunAcceptsDefaultHandoverWithRetrievalReserve(t *testing.T) {
	s := parseSpec(t, "-context-bytes", "98304")
	if _, _, err := newRun("Complete the task", "Preserve evidence", s, time.Now()); err != nil {
		t.Fatal(err)
	}
}
