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
	"golang.org/x/sys/unix"
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
	ended := make(chan struct{})
	go func() { cmd.Wait(); close(ended) }()
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	awaitUnlockedWhileRunning(t, f.state, ended)
	out, err := io.ReadAll(stderr)
	<-ended
	if err != nil {
		t.Fatal(err)
	}
	if code, line := cmd.ProcessState.ExitCode(), "canti: run "+f.status().Run.ID+" stopped: cancelled\n"; code != exitStopped || !bytes.Contains(out, []byte(line)) {
		t.Fatalf("exit %d, stderr drained after the release lacks %q:\n%s", code, line, out)
	}
}

func TestBlockedStdoutNeverKeepsTheWorkspaceLocked(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "the answer"}})
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	fillPipe(t, w)
	_, wait := f.startIO(nil, w, nil, "run", "-goal", "g")
	w.Close()
	ended := make(chan struct{})
	var res result
	go func() { res = wait(); close(ended) }()
	awaitUnlockedWhileRunning(t, f.state, ended)
	out, err := io.ReadAll(r)
	<-ended
	if err != nil {
		t.Fatal(err)
	}
	if res.code != exitOK || !bytes.HasSuffix(bytes.TrimRight(out, "\x00"), []byte("the answer\n")) {
		t.Fatalf("returned run with a blocked stdout: %+v, stdout tail %q", res, out[max(0, len(out)-40):])
	}
}

func TestChatSetupNoteLeadsAStderrItWaitedOn(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reopens a pipe whatever its mode")
	}
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "first answer"}})
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer outR.Close()
	if err := unix.Fchmod(int(outW.Fd()), 0); err != nil {
		t.Fatal(err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer errR.Close()
	fillPipe(t, errW)
	cmd := cliCommand(t, []string{"ANTHROPIC_API_KEY=" + f.key}, "chat", "-workspace", f.workspace, "-state", f.state, "-model-url", f.model.URL)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = strings.NewReader("one\n"), outW, errW
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	outW.Close()
	errW.Close()
	defer cmd.Wait()
	var answer []byte
	for buf := make([]byte, 256); !bytes.Contains(answer, []byte("first answer\n")); {
		n, err := outR.Read(buf)
		if err != nil {
			t.Fatalf("chat behind a blocked stderr never answered: %q, %v", answer, err)
		}
		answer = append(answer, buf[:n]...)
	}
	stderr, err := io.ReadAll(errR)
	if err != nil {
		t.Fatal(err)
	}
	note := "canti chat: standard output: reopen /proc/self/fd/1: permission denied; "
	if lines := strings.Split(string(bytes.TrimLeft(stderr, "\x00")), "\n"); !strings.HasPrefix(lines[0], note) || strings.Count(string(stderr), note) != 1 {
		t.Fatalf("stderr drained behind the setup note does not start with it, once:\n%s", bytes.TrimLeft(stderr, "\x00"))
	}
}

// cliCommand is the canti test binary running args with the test's
// environment, without inherited credentials or test hooks, and env.
func cliCommand(t *testing.T, env []string, args ...string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, args...)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "ANTHROPIC_") && !strings.HasPrefix(kv, "CANTI_TEST_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	if _, ok := os.LookupEnv("GORACE"); !ok {
		cmd.Env = append(cmd.Env, "GORACE=atexit_sleep_ms=0")
	}
	cmd.Env = append(append(cmd.Env, "CANTI_TEST_MAIN=1"), env...)
	return cmd
}

// awaitUnlockedWhileRunning fails unless the workspace lock of state can be
// taken while the command, which ends when ended closes, still runs, and keeps
// running for a while after: the command waits on its blocked output only
// once it has released the lock.
func awaitUnlockedWhileRunning(t *testing.T, state string, ended <-chan struct{}) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-ended:
			t.Fatal("the command ended before it released the workspace lock while its output was blocked")
		case <-deadline:
			t.Fatal("the command kept the workspace locked while its output was blocked")
		default:
		}
		if lock, err := os.OpenFile(filepath.Join(state, "lock"), os.O_RDWR, 0); err == nil {
			err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			lock.Close()
			if err == nil {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case <-ended:
		t.Fatal("the command ended as soon as it released the workspace lock, so it did not wait on its output unlocked")
	case <-time.After(200 * time.Millisecond):
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
