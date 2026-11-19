package sqlite

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestLogPageDoesNotDecodeALargeAnswerForQuiescence(t *testing.T) {
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "large answer", true)
	d := newDriver(t, s, latestOf(t, s, run.Run.ID))
	d.decide([]byte(`{"kind":"return","payload":"`+strings.Repeat("x", 1<<20)+`"}`), harness.Usage{})
	ctx := context.Background()
	if _, _, err := s.LogPage(ctx, 0, 64); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const calls = 5
	for range calls {
		if _, _, err := s.LogPage(ctx, 0, 64); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	if allocated := (after.TotalAlloc - before.TotalAlloc) / calls; allocated > 256<<10 {
		t.Fatalf("LogPage allocated %d bytes per call for a 1 MiB answer; it decodes more than the phase", allocated)
	}
}
