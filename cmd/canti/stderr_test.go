package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

func TestStderrWriterOrdered(t *testing.T) {
	var out bytes.Buffer
	w := newStderrWriter(&out)
	for _, s := range []string{"first\n", "second\n", "third\n"} {
		if n, err := io.WriteString(w, s); n != len(s) || err != nil {
			t.Fatalf("write = %d, %v", n, err)
		}
	}
	w.Close()
	if got := out.String(); got != "first\nsecond\nthird\n" {
		t.Fatalf("stderr = %q", got)
	}
}

func TestStderrWriterBoundsAndLoss(t *testing.T) {
	r, out, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer out.Close()
	fillPipe(t, out)
	w := newStderrWriter(out)
	oversized := strings.Repeat("界", stderrEntryBytes)
	for range stderrQueueEntries + 20 {
		if n, err := io.WriteString(w, oversized); n != len(oversized) || err != nil {
			t.Fatalf("write = %d, %v", n, err)
		}
	}
	if w.Dropped() == 0 {
		t.Fatal("saturated queue did not report loss")
	}
drain:
	for {
		select {
		case entry := <-w.queue:
			if len(entry.data) > stderrEntryBytes || !utf8.Valid(entry.data) {
				t.Errorf("entry bytes = %d, valid UTF-8 = %t", len(entry.data), utf8.Valid(entry.data))
			}
		default:
			break drain
		}
	}
	w.Close()
	go io.Copy(io.Discard, r)
	<-w.done
	out.Close()
}

func TestStderrWriterCloseWithBlockedPipe(t *testing.T) {
	r, out, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer out.Close()
	fillPipe(t, out)
	w := newStderrWriter(out)
	io.WriteString(w, "blocked\n")
	start := time.Now()
	w.Close()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("close blocked for %s", elapsed)
	}
	go io.Copy(io.Discard, r)
	<-w.done
}

type gatedWriter struct {
	ready chan struct{}
	out   bytes.Buffer
}

func (g *gatedWriter) Write(p []byte) (int, error) {
	<-g.ready
	return g.out.Write(p)
}

func TestStderrWriterReportsLossAfterReaderResumes(t *testing.T) {
	g := &gatedWriter{ready: make(chan struct{})}
	w := newStderrWriter(g)
	for range stderrQueueEntries + 10 {
		io.WriteString(w, "line\n")
	}
	if w.Dropped() == 0 {
		t.Fatal("expected queue overflow")
	}
	close(g.ready)
	w.Close()
	<-w.done
	if !strings.Contains(g.out.String(), "canti: stderr dropped ") {
		t.Fatalf("loss not reported: %q", g.out.String())
	}
}

func TestStderrWriterPromptAfterFullQueue(t *testing.T) {
	g := &gatedWriter{ready: make(chan struct{})}
	w := newStderrWriter(g)
	for range stderrQueueEntries + 10 {
		io.WriteString(w, "progress\n")
	}
	finished := make(chan error, 1)
	go func() { finished <- w.WriteSync([]byte("> ")) }()
	close(g.ready)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	w.Close()
	<-w.done
	if got := g.out.String(); !strings.HasSuffix(got, "> ") || !strings.Contains(got, "stderr dropped") {
		t.Fatalf("prompt order or loss notice: %q", got)
	}
}

func TestStderrWriterFinalLineWaitsBoundedForAFullQueue(t *testing.T) {
	for _, resumes := range []bool{true, false} {
		g := &gatedWriter{ready: make(chan struct{})}
		w := newStderrWriter(g)
		for range stderrQueueEntries + 10 {
			io.WriteString(w, "progress\n")
		}
		if w.Dropped() == 0 {
			t.Fatal("progress on a full queue was not dropped")
		}
		start := time.Now()
		finished := make(chan error, 1)
		go func() { _, err := io.WriteString(finalStderr{w}, "final\n"); finished <- err }()
		if resumes {
			time.Sleep(stderrDrainTime)
			close(g.ready)
		}
		var err error
		select {
		case err = <-finished:
		case <-time.After(3 * stderrFinalTime):
			t.Fatalf("resumes=%v: final line still blocked after %s", resumes, time.Since(start))
		}
		elapsed := time.Since(start)
		if resumes && (err != nil || elapsed >= stderrFinalTime) || !resumes && (!errors.Is(err, os.ErrDeadlineExceeded) || elapsed < stderrFinalTime || elapsed > 2*stderrFinalTime) {
			t.Fatalf("resumes=%v: final line = %v after %s", resumes, err, elapsed)
		}
		if !resumes {
			close(g.ready)
		}
		w.Close()
		<-w.done
		if got := g.out.String(); resumes && !strings.HasSuffix(got, "final\n") {
			t.Fatalf("final line missing after the reader resumed: %q", got)
		}
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestStderrWriterPromptFailsWithSink(t *testing.T) {
	w := newStderrWriter(failedWriter{})
	if err := w.WriteSync([]byte("> ")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("prompt error = %v", err)
	}
	w.Close()
}

func TestStderrWriterSyncLimitAndClose(t *testing.T) {
	sink := &heldWriter{entered: make(chan struct{}), release: make(chan struct{})}
	w := newStderrWriter(sink)
	if err := w.WriteSync(make([]byte, stderrEntryBytes+1)); !errors.Is(err, errStderrEntryTooLarge) {
		t.Fatalf("oversized sync write = %v", err)
	}
	finished := make(chan error, 1)
	go func() { finished <- w.WriteSync([]byte("> ")) }()
	<-sink.entered
	w.Close()
	select {
	case err := <-finished:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("closed sync write = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("synchronous writer remained blocked after close")
	}
	close(sink.release)
	<-w.done
}

type heldWriter struct {
	entered chan struct{}
	release chan struct{}
}

func (w *heldWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	return len(p), nil
}

func TestBlockedStderrSignalCleanup(t *testing.T) {
	for _, twice := range []bool{false, true} {
		t.Run(map[bool]string{false: "one signal", true: "two signals"}[twice], func(t *testing.T) {
			f := newFixture(t, nil)
			marker := filepath.Join(t.TempDir(), "blocked")
			env := []string{"ANTHROPIC_API_KEY=" + f.key}
			if twice {
				env = append(env, "CANTI_TEST_CRASH=block:stop_requested", "CANTI_TEST_BLOCKED="+marker)
			} else {
				env = append(env, "CANTI_TEST_SETUP=signal", "CANTI_TEST_BLOCKED="+marker)
			}
			var held <-chan struct{}
			if twice {
				held = f.model.holdRequests()
			}
			cmd, wait, _ := startCLIWithBlockedStderr(t, nil, env, "run", "-workspace", f.workspace, "-state", f.state, "-model-url", f.model.URL, "-goal", "g")
			if twice {
				waitFor(t, func() bool { return len(held) > 0 })
			} else {
				waitFor(t, func() bool { _, err := os.Stat(marker); return err == nil })
			}
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			if twice {
				waitFor(t, func() bool { _, err := os.Stat(marker); return err == nil })
				if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
			}
			start := time.Now()
			r := wait()
			if elapsed := time.Since(start); elapsed > 3*time.Second {
				t.Fatalf("signal exit took %s", elapsed)
			}
			if !twice {
				if r.code != exitStopped || f.status().Phase != core.PhaseStopped {
					t.Fatalf("single signal: exit %d, phase %s", r.code, f.status().Phase)
				}
				return
			}
			if r.code != exitError {
				t.Fatalf("second signal: exit %d", r.code)
			}
			if stop := f.cli(nil, "stop"); stop.code != exitStopped || f.status().Phase != core.PhaseStopped {
				t.Fatalf("external stop: exit %d, phase %s", stop.code, f.status().Phase)
			}
		})
	}
}

func TestBlockedStderrDeliversTheFinalLinesOnceDrained(t *testing.T) {
	for _, abandon := range []bool{false, true} {
		t.Run(map[bool]string{false: "stop report", true: "error after abandoned cleanup"}[abandon], func(t *testing.T) {
			f := newFixture(t, nil)
			marker := filepath.Join(t.TempDir(), "blocked")
			env := []string{"ANTHROPIC_API_KEY=" + f.key}
			if abandon {
				env = append(env, "CANTI_TEST_CRASH=block:stop_requested", "CANTI_TEST_BLOCKED="+marker)
			}
			held := f.model.holdRequests()
			cmd, _, stderr := startCLIWithBlockedStderr(t, nil, env, "run", "-workspace", f.workspace, "-state", f.state, "-model-url", f.model.URL, "-goal", "g")
			waitFor(t, func() bool { return len(held) > 0 })
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			if abandon {
				waitFor(t, func() bool { _, err := os.Stat(marker); return err == nil })
				if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
			}
			ended := make(chan struct{})
			go func() { cmd.Wait(); close(ended) }()
			select {
			case <-ended:
			case <-time.After(6 * stderrDrainTime):
			}
			out, err := io.ReadAll(stderr)
			<-ended
			if err != nil {
				t.Fatal(err)
			}
			out = bytes.TrimLeft(out, "\x00")
			id := f.status().Run.ID
			want, code := []string{"canti: run " + id + " stopped: cancelled\n"}, exitStopped
			if abandon {
				want, code = []string{"canti run: stopping run " + id + ": ", `; finish with "canti stop -run ` + id + `"`}, exitError
			}
			for _, w := range want {
				if !bytes.Contains(out, []byte(w)) {
					t.Errorf("stderr drained after the exit lacks %q:\n%s", w, out)
				}
			}
			if got := cmd.ProcessState.ExitCode(); got != code {
				t.Errorf("exit %d, want %d", got, code)
			}
		})
	}
}

func TestBlockedStderrNeverKeepsTheWorkspaceLocked(t *testing.T) {
	f := newFixture(t, nil)
	held := f.model.holdRequests()
	cmd, _, stderr := startCLIWithBlockedStderr(t, nil, []string{"ANTHROPIC_API_KEY=" + f.key}, "run", "-workspace", f.workspace, "-state", f.state, "-model-url", f.model.URL, "-goal", "g")
	waitFor(t, func() bool { return len(held) > 0 })
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		store, err := sqlite.OpenReadOnly(context.Background(), filepath.Join(f.state, "canti.db"))
		if err != nil {
			return false
		}
		defer store.Close()
		run, err := store.LastRun(context.Background())
		if err != nil {
			return false
		}
		snap, err := store.Latest(context.Background(), run.ID)
		return err == nil && snap.State.Phase == core.PhaseStopped
	})
	r := f.cli(nil, "stop")
	go io.Copy(io.Discard, stderr)
	if err := cmd.Wait(); err != nil && cmd.ProcessState.ExitCode() != exitStopped {
		t.Fatal(err)
	}
	if r.code != exitStopped {
		t.Fatalf("stop while the stopped run reports to a blocked stderr = %+v, want exit %d", r, exitStopped)
	}
}

func startCLIWithBlockedStderr(t *testing.T, stdin io.Reader, env []string, args ...string) (*exec.Cmd, func() result, *os.File) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	fillPipe(t, w)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	cmd := exec.CommandContext(ctx, exe, args...)
	t.Cleanup(func() {
		cancel()
		if cmd.Process != nil && cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
		r.Close()
	})
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "ANTHROPIC_") && !strings.HasPrefix(kv, "CANTI_TEST_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	if _, ok := os.LookupEnv("GORACE"); !ok {
		cmd.Env = append(cmd.Env, "GORACE=atexit_sleep_ms=0")
	}
	cmd.Env = append(append(cmd.Env, "CANTI_TEST_MAIN=1"), env...)
	var stdout bytes.Buffer
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, &stdout, w
	if err := cmd.Start(); err != nil {
		w.Close()
		t.Fatal(err)
	}
	w.Close()
	return cmd, func() result {
		t.Helper()
		var exit *exec.ExitError
		if err := cmd.Wait(); err != nil && !errors.As(err, &exit) {
			t.Fatalf("canti %q: %v", args, err)
		}
		return result{code: cmd.ProcessState.ExitCode(), stdout: stdout.String()}
	}, r
}
