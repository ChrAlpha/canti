package docker

import (
	"archive/tar"
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/internal/harness"
)

// tarEntry is the tar stream docker cp writes for one entry.
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
	f.set("inspect.out", f.container("exited", 3))
	f.set("cp.out", exitArchive(t, "3\n"))
	f.set("logs.out", "hello\n")
	job, found, err := f.x.Inspect(t.Context(), "e")
	if err != nil || !found || job.Status != harness.JobFailed || job.ExitCode == nil || *job.ExitCode != 3 || job.Preview != "hello\n" || !job.Output.Complete {
		t.Fatalf("Inspect() = %+v, %v, %v; want failed with the recorded exit status 3", job, found, err)
	}
	if cps := f.calls("cp"); len(cps) != 1 || cps[0] != "cp cp c-e:"+exitFile+" -" {
		t.Fatalf("cp calls = %q, want one read of %s from the inspected container", cps, exitFile)
	}
	if _, err := os.Stat(f.x.record("e")); err != nil || len(f.calls("rm")) != 1 {
		t.Fatalf("record %v, rms %q; want the job recorded and released", err, f.calls("rm"))
	}
}

func TestUnrecordedExitStaysUnknown(t *testing.T) {
	for name, set := range map[string]func(*fake){
		"no file": func(f *fake) {
			f.set("cp.err", "Error response from daemon: Could not find the file /tmp/.canti-exit in container c-e\n")
			f.set("cp.exit", "1")
		},
		"no file, earlier daemon": func(f *fake) {
			f.set("cp.err", "Error: No such container:path: c-e:/tmp/.canti-exit\n")
			f.set("cp.exit", "1")
		},
		"directory": func(f *fake) {
			f.set("cp.out", tarEntry(f.t, tar.Header{Name: ".canti-exit/", Typeflag: tar.TypeDir, Mode: 0o755}, ""))
		},
		"symlink": func(f *fake) {
			f.set("cp.out", tarEntry(f.t, tar.Header{Name: ".canti-exit", Typeflag: tar.TypeSymlink, Linkname: "/workspace/x"}, ""))
		},
		"empty":        func(f *fake) { f.set("cp.out", exitArchive(f.t, "")) },
		"not a status": func(f *fake) { f.set("cp.out", exitArchive(f.t, "ok\n")) },
		"unterminated": func(f *fake) { f.set("cp.out", exitArchive(f.t, "25")) },
		"out of range": func(f *fake) { f.set("cp.out", exitArchive(f.t, "300\n")) },
		"signed":       func(f *fake) { f.set("cp.out", exitArchive(f.t, "+0\n")) },
		"negative":     func(f *fake) { f.set("cp.out", exitArchive(f.t, "-1\n")) },
		"not a tar":    func(f *fake) { f.set("cp.out", "0\n") },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.latch(latchStart)
			f.set("inspect.out", f.container("exited", 255))
			set(f)
			unknown := func(what string, job harness.Job, err error) {
				t.Helper()
				if err != nil || job.ID != "c-e" || job.Status != harness.JobUnknown || job.ExitCode != nil || job.Output.Detail != unrecorded {
					t.Fatalf("%s = %+v, %v; want unknown with detail %q", what, job, err, unrecorded)
				}
			}
			job, found, err := f.x.Inspect(t.Context(), "e")
			if !found {
				t.Fatalf("Inspect() found no job: %v", err)
			}
			unknown("Inspect()", job, err)
			for name, report := range map[string]func() (harness.Job, error){
				"Bind()":  func() (harness.Job, error) { return f.x.Bind(t.Context(), effect) },
				"Start()": func() (harness.Job, error) { return f.x.Start(t.Context(), "e") },
			} {
				if job, err := report(); err != nil || job.ID != "c-e" || job.Status != harness.JobUnknown || job.ExitCode != nil || job.Output != (harness.ArtifactRef{}) {
					t.Fatalf("%s = %+v, %v; want unknown without the detail of a record it did not read", name, job, err)
				}
			}
			if err := f.x.Cancel(t.Context(), "e"); err != nil {
				t.Fatal(err)
			}
			job, _, err = f.x.Inspect(t.Context(), "e")
			unknown("Inspect() after Cancel", job, err)
			for _, path := range []string{f.x.record("e"), filepath.Join(f.x.dir("e"), "cancel")} {
				if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("%s exists: %v", filepath.Base(path), err)
				}
			}
			for _, cmd := range []string{"create", "start", "stop", "logs", "rm"} {
				if calls := f.calls(cmd); len(calls) != 0 {
					t.Fatalf("%s calls = %q", cmd, calls)
				}
			}
			if cps := f.calls("cp"); len(cps) != 2 {
				t.Fatalf("cp calls = %q, want one read for each Inspect", cps)
			}
		})
	}
}

func TestFailedExitRecordReadIsAnError(t *testing.T) {
	for name, set := range map[string]func(*fake){
		"daemon down": func(f *fake) {
			f.set("cp.err", "Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?\n")
			f.set("cp.exit", "1")
		},
		"container gone": func(f *fake) {
			f.set("cp.err", "Error response from daemon: No such container: c-e\n")
			f.set("cp.exit", "1")
		},
		"oversized": func(f *fake) { f.set("cp.out", exitArchive(f.t, "0\n"+strings.Repeat("x", 1<<20))) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.latch(latchStart)
			f.set("inspect.out", f.container("exited", 0))
			set(f)
			if job, found, err := f.x.Inspect(t.Context(), "e"); found || err == nil {
				t.Fatalf("Inspect() = %+v, %v, %v; want an error", job, found, err)
			}
			if _, err := os.Stat(f.x.record("e")); !errors.Is(err, fs.ErrNotExist) || len(f.calls("rm")) != 0 || len(f.calls("logs")) != 0 {
				t.Fatalf("record %v, rms %q; want nothing recorded", err, f.calls("rm"))
			}
		})
	}
}

func TestCancelledJobNeedsNoExitRecord(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	if err := mark(f.x.dir("e")); err != nil {
		t.Fatal(err)
	}
	f.set("inspect.out", f.container("exited", 143))
	f.set("cp.exit", "1")
	job, _, err := f.x.Inspect(t.Context(), "e")
	if err != nil || job.Status != harness.JobCancelled || job.ExitCode == nil || *job.ExitCode != 143 || len(f.calls("cp")) != 0 {
		t.Fatalf("Inspect() = %+v, %v, cp calls %q; want cancelled without reading an exit record", job, err, f.calls("cp"))
	}
}

func TestJobOfAnEarlierRevisionIsDerivedFromDocker(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.set("inspect.out", f.legacy("exited", 255))
	f.set("cp.exit", "1")
	job, _, err := f.x.Inspect(t.Context(), "e")
	if err != nil || job.Status != harness.JobFailed || job.ExitCode == nil || *job.ExitCode != 255 || len(f.calls("cp")) != 0 {
		t.Fatalf("Inspect() = %+v, %v, cp calls %q; want failed with Docker's exit code", job, err, f.calls("cp"))
	}
}

func TestStartRecordsAResultThatNeedsNoExitRecord(t *testing.T) {
	for name, tt := range map[string]struct {
		marked bool
		out    func(*fake) string
		want   harness.JobStatus
	}{
		"earlier revision": {false, func(f *fake) string { return f.legacy("exited", 3) }, harness.JobFailed},
		"cancelled":        {true, func(f *fake) string { return f.container("exited", 143) }, harness.JobCancelled},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.latch(latchStart)
			if tt.marked {
				if err := mark(f.x.dir("e")); err != nil {
					t.Fatal(err)
				}
			}
			f.set("inspect.out", tt.out(f))
			want, err := f.x.Start(t.Context(), "e")
			if err != nil || want.ID != "c-e" || want.Status != tt.want || !want.Output.Available {
				t.Fatalf("Start() = %+v, %v; want the job %s with its output", want, err, tt.want)
			}
			f.missing()
			if job, found, err := f.x.Inspect(t.Context(), "e"); err != nil || !found || !same(job, want) {
				t.Fatalf("Inspect() after the container was removed = %+v, %v, %v; want %+v", job, found, err, want)
			}
			if cps, starts := f.calls("cp"), f.calls("start"); len(cps) != 0 || len(starts) != 0 {
				t.Fatalf("cps %q, starts %q; want neither", cps, starts)
			}
		})
	}
}

func TestStartLeavesAnExitedJobToInspection(t *testing.T) {
	f := newFake(t)
	f.set("inspect.out", f.container("created", 0))
	f.set("exited.json", f.container("exited", 3))
	f.set("start.sh", `cp "$ctl/exited.json" "$ctl/inspect.out"`+"\n")
	f.set("cp.err", "Error response from daemon: boom\n")
	f.set("cp.exit", "1")
	f.set("logs.out", "quick\n")
	job, err := f.x.Start(t.Context(), "e")
	if err != nil || job.ID != "c-e" || job.Status != harness.JobUnknown || job.Output != (harness.ArtifactRef{}) || job.Preview != "" {
		t.Fatalf("Start() of a job that exited at once = %+v, %v; want the bound job, its result left to Inspect", job, err)
	}
	if cps, logs, rms := f.calls("cp"), f.calls("logs"), f.calls("rm"); len(cps) != 0 || len(logs) != 0 || len(rms) != 0 {
		t.Fatalf("cps %q, logs %q, rms %q; want Start to read no exit record and capture nothing", cps, logs, rms)
	}
	if _, err := os.Stat(f.x.record("e")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("record after Start: %v", err)
	}
	if job, found, err := f.x.Inspect(t.Context(), "e"); found || err == nil {
		t.Fatalf("Inspect(exit record unreadable) = %+v, %v, %v; want an error", job, found, err)
	}
	f.unset("cp.err", "cp.exit")
	f.set("cp.out", exitArchive(t, "3\n"))
	job, _, err = f.x.Inspect(t.Context(), "e")
	if err != nil || job.Status != harness.JobFailed || job.ExitCode == nil || *job.ExitCode != 3 || job.Preview != "quick\n" {
		t.Fatalf("Inspect() = %+v, %v; want failed with the recorded exit status 3", job, err)
	}
	if starts, cps := f.calls("start"), f.calls("cp"); len(starts) != 1 || len(cps) != 2 {
		t.Fatalf("starts %q, cps %q; want one start and the read retried once", starts, cps)
	}
}
