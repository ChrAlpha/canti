package main

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/host"
)

func TestRunRejectsContextWithoutRoomForMaximumHandover(t *testing.T) {
	s := runSpec(t, "-context-bytes", "65536", "-handover-bytes", "8192", "-window-steps", "1")
	if _, _, err := host.NewRun(strings.Repeat("g", 23000), "", s, time.Now()); err == nil {
		t.Fatal("accepted a run whose next window cannot accommodate a legal handover")
	}
}

func TestRunRejectsContextWithoutRetrievalReserve(t *testing.T) {
	s := runSpec(t, "-context-bytes", "65536")
	if _, _, err := host.NewRun("Complete the task", "Preserve evidence", s, time.Now()); err == nil || !strings.Contains(err.Error(), "handover reserve") {
		t.Fatalf("accepted context without room for a full retrieval and handover: %v", err)
	}
}

func TestRunAcceptsDefaultHandoverWithRetrievalReserve(t *testing.T) {
	s := runSpec(t, "-context-bytes", "98304")
	if _, _, err := host.NewRun("Complete the task", "Preserve evidence", s, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// TestRunNamesTheContextItRequires requires that a run refused for its
// -context-bytes names the smallest value its goal, constraints, and flags
// admit, which is admitted, at the default, smallest, and largest handover
// allowance, with and without imports.
func TestRunNamesTheContextItRequires(t *testing.T) {
	imports := t.TempDir()
	for _, flags := range [][]string{nil, {"-handover-bytes", "1"}, {"-handover-bytes", "8192", "-history-imports", imports}} {
		create := func(n int) error {
			s := runSpec(t, append([]string{"-context-bytes", strconv.Itoa(n)}, flags...)...)
			_, _, err := host.NewRun("Complete the task", "Preserve evidence", s, time.Now())
			return err
		}
		refused, admitted := 1, 1<<20
		if create(refused) == nil || create(admitted) != nil {
			t.Fatalf("%v: no bracket of the smallest admitted -context-bytes", flags)
		}
		for admitted-refused > 1 {
			if mid := (refused + admitted) / 2; create(mid) == nil {
				admitted = mid
			} else {
				refused = mid
			}
		}
		err := create(admitted - 1)
		if want := "at least " + strconv.Itoa(admitted) + " bytes"; err == nil || !strings.HasPrefix(err.Error(), "-context-bytes: ") || !strings.Contains(err.Error(), want) {
			t.Errorf("%v: -context-bytes %d is refused with %v, which does not name the %d bytes admitted", flags, admitted-1, err, admitted)
		}
	}
}
