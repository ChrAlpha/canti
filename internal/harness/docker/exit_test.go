package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/internal/harness"
)

// tarEntry is the archive the daemon sends of one entry.
func tarEntry(t *testing.T, h tar.Header, body string) string {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	h.Size = int64(len(body))
	if h.Typeflag != tar.TypeReg {
		h.Size = 0
	}
	if err := w.WriteHeader(&h); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(body[:h.Size])); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func exitArchive(t *testing.T, body string) string {
	return tarEntry(t, tar.Header{Name: ".canti-exit", Typeflag: tar.TypeReg, Mode: 0o644}, body)
}

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
func TestUnrecordedExitStaysUnknown(t *testing.T) {
	cases := map[string]func(*fake){
		"missing": func(f *fake) { f.fail("cp", 404, "Could not find the file /tmp/.canti-exit in container c-e") },
		"directory": func(f *fake) {
			f.reply("cp", tarEntry(f.t, tar.Header{Name: ".canti-exit/", Typeflag: tar.TypeDir}, ""))
		},
		"symlink": func(f *fake) {
			f.reply("cp", tarEntry(f.t, tar.Header{Name: ".canti-exit", Typeflag: tar.TypeSymlink, Linkname: "/workspace/x"}, ""))
		},
	}
	for _, value := range []string{"", "ok\n", "25", "300\n", "+0\n", "-1\n"} {
		value := value
		cases["invalid:"+value] = func(f *fake) { f.reply("cp", exitArchive(f.t, value)) }
	}
	for name, set := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.latch(latchStart)
			f.reply("inspect", f.container("exited", 255))
			set(f)
			e := f.effect()
			for _, op := range []func(context.Context, harness.Effect) (harness.Job, error){f.x.Inspect, freshExecutor(f).Drive, freshExecutor(f).Cancel} {
				j, err := op(t.Context(), e)
				if err != nil || j.Status != harness.JobUnknown || j.ExitCode != nil || j.ID != "c-e" {
					t.Fatalf("unknown: %+v %v", j, err)
				}
			}
			for _, op := range []string{"create", "start", "stop", "logs", "rm"} {
				if len(f.calls(op)) != 0 {
					t.Fatalf("uncertain effect called %s: %v", op, f.calls(op))
				}
			}
			if _, err := os.Stat(f.x.record("e")); !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("unknown became terminal")
			}
		})
	}
}
func TestFailedExitRecordReadRetainsPhysicalEvidence(t *testing.T) {
	for name, set := range map[string]func(*fake){"daemon down": func(f *fake) { f.down("cp") }, "daemon error": func(f *fake) { f.fail("cp", http.StatusInternalServerError, "unavailable") }, "oversized": func(f *fake) { f.reply("cp", exitArchive(f.t, "0\n"+strings.Repeat("x", 1<<20))) }} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.latch(latchStart)
			f.exited(0)
			set(f)
			if _, err := f.x.Inspect(t.Context(), f.effect()); err == nil {
				t.Fatal("unreadable record became known")
			}
			if _, err := os.Stat(f.x.record("e")); !errors.Is(err, fs.ErrNotExist) || len(f.calls("rm")) != 0 {
				t.Fatalf("released unreadable owner: %v", err)
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
