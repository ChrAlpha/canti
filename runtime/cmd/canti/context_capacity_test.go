package main

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/host"
)

func TestRunAcceptsOrientationWithoutHypotheticalReserve(t *testing.T) {
	s := runSpec(t, "-request-bytes", "65536")
	if _, _, err := host.NewRun(strings.Repeat("g", 23000), "Preserve evidence", s, time.Now()); err != nil {
		t.Fatal(err)
	}
}
func TestRunRejectsRemovedContextPolicyFlags(t *testing.T) {
	f := newFixture(t, nil)
	for _, flag := range []string{"-context-bytes", "-handover-bytes", "-window-steps", "-window-bytes"} {
		r := f.cli(nil, "run", "-goal", "task", flag, "1")
		if r.code != exitUsage || !strings.Contains(r.stderr, "flag provided but not defined") {
			t.Fatalf("%s: %+v", flag, r)
		}
	}
}

// Admission measures only actual orientation, including the exact authoritative
// text and navigation, without reserving hypothetical future evidence.
func TestRunNamesTheContextItRequires(t *testing.T) {
	imports := t.TempDir()
	for _, flags := range [][]string{nil, {"-decision-format", "tool"}, {"-history-imports", imports}} {
		create := func(n int) error {
			s := runSpec(t, append([]string{"-request-bytes", strconv.Itoa(n)}, flags...)...)
			_, _, err := host.NewRun("Complete the task", "Preserve evidence", s, time.Now())
			return err
		}
		refused, admitted := 1, 1<<20
		if create(refused) == nil || create(admitted) != nil {
			t.Fatalf("%v: no bracket of the smallest admitted -request-bytes", flags)
		}
		for admitted-refused > 1 {
			if mid := (refused + admitted) / 2; create(mid) == nil {
				admitted = mid
			} else {
				refused = mid
			}
		}
		err := create(admitted - 1)
		if want := "at least " + strconv.Itoa(admitted) + " bytes"; err == nil || !strings.HasPrefix(err.Error(), "-request-bytes: ") || !strings.Contains(err.Error(), want) {
			t.Errorf("%v: -request-bytes %d is refused with %v, which does not name the %d bytes admitted", flags, admitted-1, err, admitted)
		}
	}
}
