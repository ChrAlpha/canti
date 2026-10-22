package main

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRunRejectsContextWithoutRoomForMaximumSummary(t *testing.T) {
	s := parseSpec(t, "-context-bytes", "65536", "-summary-bytes", "8192", "-compact-steps", "1")
	if _, _, err := newRun(strings.Repeat("g", 23000), "", s, time.Now()); err == nil {
		t.Fatal("accepted a run whose next window cannot accommodate a legal summary")
	}
}

func TestRunRejectsContextWithoutRetrievalReserve(t *testing.T) {
	s := parseSpec(t, "-context-bytes", "65536")
	if _, _, err := newRun("Complete the task", "Preserve evidence", s, time.Now()); err == nil || !strings.Contains(err.Error(), "summary reserve") {
		t.Fatalf("accepted context without room for a full retrieval and summary: %v", err)
	}
}

func TestRunAcceptsDefaultSummaryWithRetrievalReserve(t *testing.T) {
	s := parseSpec(t, "-context-bytes", "131072")
	if _, _, err := newRun("Complete the task", "Preserve evidence", s, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// TestRunNamesTheContextItRequires requires that a run refused for its
// -context-bytes names the smallest value its goal, constraints, and flags
// admit, which is admitted, at the default, smallest, and largest summary
// allowance, with and without imports.
func TestRunNamesTheContextItRequires(t *testing.T) {
	imports := t.TempDir()
	for _, flags := range [][]string{nil, {"-summary-bytes", "1"}, {"-summary-bytes", "8192", "-history-imports", imports}} {
		create := func(n int) error {
			s := parseSpec(t, append([]string{"-context-bytes", strconv.Itoa(n)}, flags...)...)
			_, _, err := newRun("Complete the task", "Preserve evidence", s, time.Now())
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

func TestRunRejectsSerializedAndTokenOverflowBeforeCreatingState(t *testing.T) {
	for _, args := range [][]string{{"-context-tokens", "16001"}, {"-request-bytes", "100"}, {"-max-tokens", "9223372036854775807"}} {
		f := newFixture(t, nil)
		r := f.cli(nil, "run", append([]string{"-goal", "A fixed request with constraints.", "-constraints", "Read only."}, args...)...)
		if r.code != exitUsage || len(f.model.requests()) != 0 {
			t.Fatalf("bad admission %v: %+v", args, r)
		}
		if _, err := os.Stat(f.state); !os.IsNotExist(err) {
			t.Fatalf("admission created state %v", err)
		}
	}
}
