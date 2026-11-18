package docker

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestRecordedExitIsFinal(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.exited(3)
	f.output("hello\n")
	j, err := f.x.Inspect(t.Context(), f.effect())
	if err != nil || j.Status != harness.JobFailed || j.ExitCode == nil || *j.ExitCode != 3 || j.Preview != "hello\n" || !j.Output.Complete {
		t.Fatalf("Inspect: %+v %v", j, err)
	}
	if _, err := os.Stat(f.x.record("e")); err != nil || len(f.calls("rm")) != 1 {
		t.Fatalf("record %v release %v", err, f.calls("rm"))
	}
}
func TestMissingHelperCompletionStaysUnknown(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.reply("inspect", f.container("exited", 255))
	j, err := f.x.Inspect(t.Context(), f.effect())
	if err != nil || j.Status != harness.JobUnknown || j.ExitCode != nil || j.ID != "c-e" {
		t.Fatalf("missing completion = %+v, %v", j, err)
	}
	if _, err := os.Stat(f.x.record("e")); !errors.Is(err, fs.ErrNotExist) || len(f.calls("rm")) != 0 {
		t.Fatalf("unknown became terminal: %v", err)
	}
}
func TestDamagedHelperCaptureDoesNotBecomeKnown(t *testing.T) {
	for name, set := range map[string]struct {
		damage func(*fake)
		want   harness.JobStatus
		err    bool
	}{
		"missing completion": {func(f *fake) { os.Remove(filepath.Join(f.x.capture("e"), "completion.json")) }, harness.JobUnknown, false},
		"invalid completion": {func(f *fake) {
			os.WriteFile(filepath.Join(f.x.capture("e"), "completion.json"), []byte("{"), 0600)
		}, "", true},
		"oversized completion": {func(f *fake) {
			os.WriteFile(filepath.Join(f.x.capture("e"), "completion.json"), []byte(strings.Repeat("x", 20<<10)), 0600)
		}, "", true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.latch(latchStart)
			f.exited(0)
			set.damage(f)
			j, err := f.x.Inspect(t.Context(), f.effect())
			if set.err {
				if err == nil {
					t.Fatalf("damaged completion became terminal: %+v", j)
				}
			} else if err != nil || j.Status != set.want {
				t.Fatalf("damaged completion = %+v, %v", j, err)
			}
			if _, err := os.Stat(f.x.record("e")); !errors.Is(err, fs.ErrNotExist) || len(f.calls("rm")) != 0 {
				t.Fatalf("released damaged owner: %v", err)
			}
		})
	}
}
func TestContainerWithoutCurrentExecutionProtocolIsRefused(t *testing.T) {
	f := newFake(t)
	f.reply("inspect", f.legacy("exited", 255))
	if _, err := f.x.Inspect(t.Context(), f.effect()); err == nil {
		t.Fatal("accepted foreign execution protocol")
	}
	if len(f.calls("rm"))+len(f.calls("start"))+len(f.calls("stop")) != 0 {
		t.Fatal("acted on foreign owner")
	}
}
