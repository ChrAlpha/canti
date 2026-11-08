//go:build acceptance && linux

package acceptance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
	"github.com/charmbracelet/x/term"
	"github.com/charmbracelet/x/vt"
	"github.com/creack/pty"
	"golang.org/x/sys/unix"
)

type ptyChat struct {
	cmd      *exec.Cmd
	master   *os.File
	slave    *os.File
	slaveFD  int
	base     *unix.Termios
	flags    int
	mu       sync.Mutex
	output   bytes.Buffer
	screen   *vt.Emulator
	frames   []screenFrame
	frameSeq frameCursor
	wake     chan struct{}
	done     chan struct{}
	waited   bool
}

func startPTYChat(t *testing.T, f *fixture, stderr *os.File, args ...string) *ptyChat {
	return startPTYChatWithEnv(t, f, stderr, nil, args...)
}

func startPTYChatWithEnv(t *testing.T, f *fixture, stderr *os.File, env []string, args ...string) *ptyChat {
	return startPTYChatWithMode(t, f, stderr, env, true, args...)
}

func startPTYChatWithMode(t *testing.T, f *fixture, stderr *os.File, env []string, canonical bool, args ...string) *ptyChat {
	return startPTYChatWithDeadline(t, f, stderr, env, canonical, 30*time.Second, args...)
}

func startPTYChatWithDeadline(t *testing.T, f *fixture, stderr *os.File, env []string, canonical bool, deadline time.Duration, args ...string) *ptyChat {
	return startPTYChatWithAttr(t, f, stderr, env, canonical, deadline, nil, args...)
}

func startPTYChatWithAttr(t *testing.T, f *fixture, stderr *os.File, env []string, canonical bool, deadline time.Duration, attr *syscall.SysProcAttr, args ...string) *ptyChat {
	t.Helper()
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	return startPTYChatOn(t, f, master, slave, stderr, env, canonical, deadline, attr, args...)
}

// startPTYChatOn is startPTYChatWithAttr on the terminal of master and slave.
func startPTYChatOn(t *testing.T, f *fixture, master, slave, stderr *os.File, env []string, canonical bool, deadline time.Duration, attr *syscall.SysProcAttr, args ...string) *ptyChat {
	t.Helper()
	if err := pty.Setsize(master, &pty.Winsize{Rows: 24, Cols: 100}); err != nil {
		t.Fatal(err)
	}
	if err := unix.SetNonblock(int(master.Fd()), true); err != nil {
		t.Fatal(err)
	}
	slaveFD := int(slave.Fd())
	base, err := unix.IoctlGetTermios(slaveFD, unix.TCGETS)
	if err != nil {
		t.Fatal(err)
	}
	if !canonical {
		base.Lflag &^= unix.ICANON
		base.Cc[unix.VMIN], base.Cc[unix.VTIME] = 1, 0
		if err := unix.IoctlSetTermios(slaveFD, unix.TCSETS, base); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), deadline)
	argv := append(append([]string{tuiCommand, "-workspace", f.workspace, "-state", f.state, "-model-url", f.model.URL, "-session", "pty"}, fixtureModel...), args...)
	exe, argv := executable(t, env, argv)
	cmd := exec.CommandContext(ctx, exe, argv...)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "ANTHROPIC_") && !strings.HasPrefix(kv, "CANTI_TEST_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "CANTI_TEST_MAIN=1", "ANTHROPIC_API_KEY="+f.key, "GORACE=atexit_sleep_ms=0")
	cmd.Env = append(cmd.Env, env...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = slave, slave, slave
	if stderr != nil {
		cmd.Stderr = stderr
	}
	cmd.SysProcAttr = attr
	p := &ptyChat{screen: newScreen(100), cmd: cmd, master: master, slave: slave, slaveFD: slaveFD, base: base, flags: descriptionFlags(t, slaveFD), wake: make(chan struct{}, 1), done: make(chan struct{})}
	if stderr != nil && !term.IsTerminal(stderr.Fd()) {
		p.screen.Close()
		p.screen = newScreen(80)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		master.Close()
		slave.Close()
		t.Fatal(err)
	}
	go func() {
		defer close(p.done)
		buf := make([]byte, 4096)
		for {
			n, err := master.Read(buf)
			if n > 0 {
				p.mu.Lock()
				p.record(buf[:n])
				p.mu.Unlock()
				select {
				case p.wake <- struct{}{}:
				default:
				}
			}
			if err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			if !p.waited {
				waited := make(chan struct{})
				go func() { _ = cmd.Wait(); close(waited) }()
				select {
				case <-waited:
				case <-time.After(2 * time.Second):
					t.Error("chat process did not reap after kill")
				}
			}
		}
		master.Close()
		slave.Close()
		<-p.done
		p.screen.Close()
		if slices.Contains(cmd.Args, "owned") {
			waitFor(t, func() bool { return !locked(f.state) })
		}
	})
	return p
}

type frameCursor int

type screenFrame struct {
	sequence frameCursor
	text     string
}

func newScreen(width int) *vt.Emulator {
	screen := vt.NewEmulator(width, 24)
	// This observer consumes rendering only; terminal query replies are not
	// injected as user input. Closing the response pipe keeps parsing nonblocking.
	_ = screen.InputPipe().(io.Closer).Close()
	return screen
}

// record is called under p.mu by either the PTY or redirected-output reader.
func (p *ptyChat) record(data []byte) {
	p.output.Write(data)
	_, _ = p.screen.Write(data)
	p.frameSeq++
	p.frames = append(p.frames, screenFrame{p.frameSeq, p.screen.String()})
	if len(p.frames) > 512 {
		copy(p.frames, p.frames[len(p.frames)-512:])
		p.frames = p.frames[:512]
	}
}

func (p *ptyChat) text() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.output.String()
}

func (p *ptyChat) mark() frameCursor { p.mu.Lock(); defer p.mu.Unlock(); return p.frameSeq }
func screenStatus(screen string) string {
	lines := strings.Split(screen, "\n")
	if len(lines) < 2 || !strings.HasPrefix(lines[0], "canti-tui · session ") {
		return ""
	}
	status, _, _ := strings.Cut(lines[1], " · ")
	return status
}
func (p *ptyChat) status() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return screenStatus(p.screen.String())
}
func (p *ptyChat) frameAfter(after frameCursor, match func(string) bool) (frameCursor, bool) {
	for _, frame := range p.frames {
		if frame.sequence > after && match(frame.text) {
			return frame.sequence, true
		}
	}
	return 0, false
}
func (p *ptyChat) awaitStatus(t *testing.T, status string, after frameCursor) frameCursor {
	t.Helper()
	return p.awaitMatch(t, "status "+status, after, func(screen string) bool { return screenStatus(screen) == status })
}
func (p *ptyChat) await(t *testing.T, needle string, after frameCursor) frameCursor {
	t.Helper()
	return p.awaitMatch(t, needle, after, func(screen string) bool { return strings.Contains(screen, needle) })
}
func (p *ptyChat) awaitMatch(t *testing.T, description string, after frameCursor, match func(string) bool) frameCursor {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		p.mu.Lock()
		found, ok := p.frameAfter(after, match)
		screen := p.screen.String()
		p.mu.Unlock()
		if ok {
			return found
		}
		select {
		case <-p.wake:
		case <-deadline.C:
			t.Fatalf("terminal never showed %q after frame %d; screen: %q; process: %v", description, after, screen, p.cmd.Process.Signal(syscall.Signal(0)))
		}
	}
}
func (p *ptyChat) awaitFinalOutput(t *testing.T, needle string) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if strings.Contains(ansi.Strip(p.text()), needle) {
			return
		}
		select {
		case <-p.wake:
		case <-deadline.C:
			t.Fatalf("final output missing %q", needle)
		}
	}
}
func (p *ptyChat) capture(data []byte) {
	p.mu.Lock()
	p.record(data)
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}
func readPTYOutput(p *ptyChat, fd int, buf []byte) (int, error) {
	n, err := unix.Read(fd, buf)
	if n > 0 {
		p.capture(buf[:n])
	}
	return n, err
}

func (p *ptyChat) send(t *testing.T, input string) {
	t.Helper()
	if _, err := io.WriteString(p.master, input); err != nil {
		t.Fatal(err)
	}
}

func (p *ptyChat) awaitRaw(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		flags, err := unix.IoctlGetTermios(p.slaveFD, unix.TCGETS)
		if err != nil {
			t.Fatal(err)
		}
		if flags.Lflag&unix.ICANON == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("chat did not put the PTY into raw mode")
}

func (p *ptyChat) finish(t *testing.T, want int) {
	t.Helper()
	p.waited = true
	ended := make(chan error, 1)
	go func() { ended <- p.cmd.Wait() }()
	select {
	case err := <-ended:
		var exit *exec.ExitError
		if err != nil && !errors.As(err, &exit) {
			t.Fatal(err)
		}
		if p.cmd.ProcessState.ExitCode() != want {
			t.Fatalf("chat exit %d, want %d; output tail: %q", p.cmd.ProcessState.ExitCode(), want, p.text()[max(0, len(p.text())-500):])
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		select {
		case <-ended:
		case <-time.After(2 * time.Second):
		}
		t.Fatal("chat did not exit within five seconds")
	}
	got, err := unix.IoctlGetTermios(p.slaveFD, unix.TCGETS)
	if err != nil {
		t.Fatal(err)
	}
	if got.Lflag != p.base.Lflag || got.Iflag != p.base.Iflag || got.Oflag != p.base.Oflag || got.Cflag != p.base.Cflag {
		t.Fatalf("terminal flags not restored: before=%+v after=%+v", p.base, got)
	}
	if flags := descriptionFlags(t, p.slaveFD); flags != p.flags {
		t.Fatalf("chat changed the terminal's file status flags from %#x to %#x", p.flags, flags)
	}
}

// holdFirstAnswer returns the URL of a model that answers as f.model does but
// holds the first answer until release is called.
func holdFirstAnswer(t *testing.T, f *fixture) (url string, release func()) {
	t.Helper()
	held := make(chan struct{})
	var releaseOnce, holdOnce sync.Once
	release = func() { releaseOnce.Do(func() { close(held) }) }
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		holdOnce.Do(func() {
			select {
			case <-held:
			case <-r.Context().Done():
			}
		})
		f.model.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { release(); gate.Close() })
	return gate.URL, release
}

func descriptionFlags(t *testing.T, fd int) int {
	t.Helper()
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		t.Fatal(err)
	}
	return flags
}

func TestChatKeepsInheritedDescriptorsBlocking(t *testing.T) {
	for _, isTerminal := range []bool{false, true} {
		for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
			t.Run(fmt.Sprintf("terminal=%v/signal=%v", isTerminal, sig), func(t *testing.T) {
				f := newFixture(t, nil)
				if isTerminal {
					p := startPTYChat(t, f, nil)
					p.awaitStatus(t, "ready", 0)
					if err := p.cmd.Process.Signal(sig); err != nil {
						t.Fatal(err)
					}
					want := exitOK
					if sig == syscall.SIGTERM {
						want = exitStopped
					}
					p.finish(t, want)
				} else {
					reader, out, err := os.Pipe()
					if err != nil {
						t.Fatal(err)
					}
					defer reader.Close()
					defer out.Close()
					before := descriptionFlags(t, int(out.Fd()))
					in, input, err := os.Pipe()
					if err != nil {
						t.Fatal(err)
					}
					defer in.Close()
					defer input.Close()
					cmd, wait := f.startIO(in, out, nil, tuiCommand)
					waitFor(t, func() bool { _, err := os.Stat(filepath.Join(f.state, "history")); return err == nil })
					cmd.Process.Signal(sig)
					if r := wait(); r.code != exitStopped {
						t.Fatalf("signal result: %+v", r)
					}
					if after := descriptionFlags(t, int(out.Fd())); after != before {
						t.Fatalf("flags %x -> %x", before, after)
					}
				}
			})
		}
	}
	t.Run("SIGKILL preserves inherited file description", func(t *testing.T) {
		f := newFixture(t, nil)
		p := startPTYChat(t, f, nil)
		p.awaitStatus(t, "ready", 0)
		p.waited = true
		p.cmd.Process.Kill()
		p.cmd.Wait()
		if got := descriptionFlags(t, p.slaveFD); got != p.flags {
			t.Fatalf("flags %x -> %x", p.flags, got)
		}
	})
	for _, kind := range []string{"unreopenable pipe", "unreopenable terminal", "terminal master", "hung-up terminal"} {
		t.Run(kind, func(t *testing.T) {
			if strings.HasPrefix(kind, "unreopenable") && os.Geteuid() == 0 {
				t.Skip("root can reopen regardless of mode")
			}
			f := newFixture(t, nil)
			var reader, out *os.File
			var err error
			if kind == "unreopenable pipe" {
				reader, out, err = os.Pipe()
			} else {
				reader, out, err = pty.Open()
			}
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			defer out.Close()
			if kind == "terminal master" {
				reader, out = out, reader
			}
			before := descriptionFlags(t, int(out.Fd()))
			if strings.HasPrefix(kind, "unreopenable") {
				if err = unix.Fchmod(int(out.Fd()), 0); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "hung-up terminal" {
				reader.Close()
			}
			_, wait := f.startIO(strings.NewReader("never\n"), out, nil, tuiCommand)
			if r := wait(); r.code != exitError || !strings.Contains(r.stderr, "standard output") {
				t.Fatalf("unsupported transport: %+v", r)
			}
			if got := descriptionFlags(t, int(out.Fd())); got != before {
				t.Fatalf("flags %x -> %x", before, got)
			}
			if _, err = os.Stat(f.state); !os.IsNotExist(err) {
				t.Fatalf("refusal created state: %v", err)
			}
		})
	}
}

func TestChatPTYBusyInputDoesNotBecomeNextRun(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		name := "returned"
		if interrupted {
			name = "interrupted"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "first answer"}})
			f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"second answer"}`})
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
			var once sync.Once
			served := make(chan struct{})
			gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(bytes.NewReader(body))
				first := false
				once.Do(func() {
					first = true
					entered <- struct{}{}
					select {
					case <-release:
					case <-r.Context().Done():
					}
				})
				f.model.ServeHTTP(w, r)
				if first {
					close(served)
				}
			}))
			t.Cleanup(func() { releaseGate(); gate.Close() })
			p := startPTYChat(t, f, nil, "-model-url", gate.URL)
			pos := p.awaitStatus(t, "ready", 0)
			p.send(t, "first request\r")
			pos = p.awaitStatus(t, "running", pos)
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the model did not receive the first run")
			}
			p.send(t, "second request\r")
			p.await(t, "Send unavailable", pos)
			if interrupted {
				p.send(t, "\x03")
				deadline := time.Now().Add(5 * time.Second)
				for f.status().Phase != "stopped" {
					if time.Now().After(deadline) {
						t.Fatal("Ctrl-C did not stop the active run")
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			releaseGate()
			pos = p.awaitStatus(t, "ready", pos)
			if s := f.status(); s.Run.SessionSeq != 1 || s.Run.Goal != "first request" {
				t.Fatalf("busy input became a run: %+v", s.Run)
			}
			time.Sleep(200 * time.Millisecond)
			if s := f.status(); s.Run.SessionSeq != 1 {
				t.Fatalf("buffered busy input became a run: %+v", s.Run)
			}
			select {
			case <-served:
			case <-time.After(5 * time.Second):
				t.Fatal("the model did not answer the first run")
			}
			p.send(t, "\r")
			p.await(t, "second answer", pos)
			if s := f.status(); s.Run.SessionSeq != 2 || s.Run.Goal != "second request" || s.Phase != "returned" {
				t.Fatalf("explicit input did not finish: %+v", s)
			}
			prompts := f.model.prompts(1)
			if len(prompts) != 2 || !strings.Contains(prompts[1], "first request") || !strings.Contains(prompts[1], "second request") {
				t.Fatalf("wrong session history: %q", prompts)
			}
			if interrupted && !strings.Contains(prompts[1], "stopped, reason cancelled") {
				t.Fatalf("stopped run missing from context: %q", prompts[1])
			}
			p.send(t, "\x03")
			p.finish(t, exitOK)
		})
	}
}

func TestChatPTYStderrBackpressureCancelsActiveRun(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "unused"}})
	held := f.model.holdRequests()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	writeFD := int(w.Fd())
	p := startPTYChat(t, f, w)
	p.awaitRaw(t)
	p.send(t, "busy request\r")
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("the model did not receive the active run")
	}
	if flags := descriptionFlags(t, writeFD); flags&unix.O_NONBLOCK != 0 {
		t.Fatalf("chat made the stderr it inherited nonblocking: flags=%#x", flags)
	}
	if err := unix.SetNonblock(writeFD, true); err != nil {
		t.Fatal(err)
	}
	fillLivePipe(t, writeFD)
	p.send(t, "\x03")
	p.finish(t, exitError)
	if s := f.status(); s.Phase != "stopped" || s.Reason != "cancelled" || s.Run.SessionSeq != 1 {
		t.Fatalf("output failure left an active or repeated run: %+v", s)
	}
}

func fillLivePipe(t *testing.T, fd int) {
	t.Helper()
	for _, chunk := range [][]byte{make([]byte, 4096), {0}} {
		for {
			if _, err := unix.Write(fd, chunk); errors.Is(err, unix.EAGAIN) {
				break
			} else if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestChatPTYHangupStopsTheActiveRun(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "unused"}})
	held := f.model.holdRequests()
	p := startPTYChatWithAttr(t, f, nil, nil, true, 30*time.Second, &syscall.SysProcAttr{Setsid: true, Setctty: true})
	p.awaitStatus(t, "ready", 0)
	p.send(t, "busy request\r")
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("the model did not receive the active run")
	}
	p.waited = true
	ended := make(chan error, 1)
	go func() { ended <- p.cmd.Wait() }()
	if err := p.master.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ended:
	case <-time.After(10 * time.Second):
		t.Fatal("chat did not exit after its terminal hung up")
	}
	s := f.status()
	if code := p.cmd.ProcessState.ExitCode(); code != exitError || s.Phase != "stopped" || s.Reason != "cancelled" || s.Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
		t.Fatalf("hangup: %v, exit %d, run %+v", p.cmd.ProcessState, code, s)
	}
	f.model.mu.Lock()
	f.model.hold = nil
	f.model.mu.Unlock()
	if r := f.cli(nil, "run", "-session", "pty", "-input", "next", "-goal", "fresh"); r.code != exitOK || f.status().Run.SessionSeq != 2 {
		t.Fatalf("the session did not continue after the hangup: %+v", r)
	}
}

func TestChatPTYEndsWithUsageWhenNoInputFits(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "first answer"}})
	url, release := holdFirstAnswer(t, f)
	imports := t.TempDir()
	p := startPTYChat(t, f, nil, "-model-url", url, "-history-imports", imports)
	pos := p.awaitStatus(t, "ready", 0)
	p.send(t, "one\r")
	pos = p.awaitStatus(t, "running", pos)
	release()
	p.await(t, "first answer", pos)
	p.awaitStatus(t, "ready", pos)
	if err := os.Remove(imports); err != nil {
		t.Fatal(err)
	}
	p.send(t, "two\r")
	p.finish(t, exitUsage)
	out := p.text()
	altEnd := strings.LastIndex(out, "\x1b[?1049l")
	if altEnd < 0 || !strings.Contains(out[altEnd:], "canti-tui: history imports: ") || !strings.Contains(out[altEnd:], "Usage: canti-tui [flags]") {
		t.Fatalf("usage advice absent after alt-screen exit: alt-end=%d, output tail=%q", altEnd, out[max(0, len(out)-1200):])
	}
	if s := f.status(); s.Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
		t.Fatalf("an input was submitted after no input fit: %+v", s)
	}
}

func TestChatPTYResolutionGuidanceSurvivesAltScreen(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "exec", Payload: "true"}})
	t.Setenv("PATH", t.TempDir())
	p := startPTYChat(t, f, nil)
	p.awaitStatus(t, "ready", 0)
	p.send(t, "needs inspection\r")
	p.finish(t, exitResolution)
	s := f.status()
	if s.Phase != "needs_resolution" || s.Run.SessionSeq != 1 {
		t.Fatalf("missing durable resolution state: %+v", s)
	}
	out := p.text()
	altEnd := strings.LastIndex(out, "\x1b[?1049l")
	if altEnd < 0 || !strings.Contains(out[altEnd:], "canti resolve -run "+s.Run.ID) || !strings.Contains(out[altEnd:], "canti-tui -session pty") {
		t.Fatalf("resolution and session guidance absent after alt-screen exit: alt-end=%d, output tail=%q", altEnd, out[max(0, len(out)-1200):])
	}
}

func TestChatPTYSecondInterruptExitsBlockedCleanupWithFullStderr(t *testing.T) {
	f := newFixture(t, nil)
	held := f.model.holdRequests()
	marker := filepath.Join(t.TempDir(), "stopping")
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	writeFD := int(w.Fd())
	p := startPTYChatWithEnv(t, f, w, []string{"CANTI_TEST_CRASH=block:stop_requested", "CANTI_TEST_BLOCKED=" + marker})
	p.awaitRaw(t)
	p.send(t, "request needing cleanup\r")
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not reach provider")
	}
	if flags := descriptionFlags(t, writeFD); flags&unix.O_NONBLOCK != 0 {
		t.Fatalf("chat made the stderr it inherited nonblocking: flags=%#x", flags)
	}
	if err := unix.SetNonblock(writeFD, true); err != nil {
		t.Fatal(err)
	}
	fillLivePipe(t, writeFD)
	p.send(t, "\x03")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first Ctrl-C never entered blocked stop commit")
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.send(t, "\x03")
	p.finish(t, exitError)
	s := f.status()
	if s.Run.SessionSeq != 1 || s.Run.Goal != "request needing cleanup" || s.Phase == "stopped" {
		t.Fatalf("blocked cleanup unexpectedly committed or started another run: %+v", s)
	}
	if result := f.cli(nil, "stop", "-run", s.Run.ID); result.code != exitStopped {
		t.Fatalf("retrying stop after abandoned cleanup: %+v", result)
	}
	if final := f.status(); final.Phase != "stopped" || final.Reason != "cancelled" || final.Run.SessionSeq != 1 {
		t.Fatalf("manual stop did not finish original run: %+v", final)
	}
}

func TestChatPTYHangupDetachesFromAHostProcess(t *testing.T) {
	f := newFixture(t, nil)
	gate := make(chan struct{})
	f.model.decideWith(func(int, string) (decision, error) {
		<-gate
		return decision{Kind: "return", Payload: "answered after the hangup"}, nil
	})
	f.autoHost()
	p := startPTYChatWithAttr(t, f, nil, nil, true, 30*time.Second, &syscall.SysProcAttr{Setsid: true, Setctty: true}, "-host", "auto")
	p.awaitStatus(t, "ready", 0)
	p.send(t, "busy request\r")
	waitFor(t, func() bool { return len(f.model.requests()) == 1 })
	p.waited = true
	ended := make(chan error, 1)
	go func() { ended <- p.cmd.Wait() }()
	if err := p.master.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ended:
	case <-time.After(10 * time.Second):
		t.Fatal("chat did not exit after its terminal hung up")
	}
	close(gate)
	waitFor(t, func() bool { return f.status().Phase == "returned" })
	if h := f.history(); slicesContain(kinds(h), "stop_requested") || len(f.model.requests()) != 1 {
		t.Fatalf("a hangup of a chat attached to a host stopped its run: %q", kinds(h))
	}
}

func TestScreenObserverRequiresExactStatusAndFreshFrame(t *testing.T) {
	p := &ptyChat{screen: newScreen(100)}
	defer p.screen.Close()
	p.record([]byte("canti-tui · session pty\r\nrunning · draft editable\r\nEnter send when ready"))
	ready := func(screen string) bool { return screenStatus(screen) == "ready" }
	if _, ok := p.frameAfter(0, ready); ok {
		t.Fatal("footer matched ready status")
	}
	p.record([]byte("\x1b[2;1H\x1b[2Kready · current launch flags apply"))
	frame, ok := p.frameAfter(0, ready)
	if !ok {
		t.Fatal("exact ready status was not observed")
	}
	if _, ok = p.frameAfter(frame, ready); ok {
		t.Fatal("the same frame was reused")
	}
	p.record([]byte("\x1b[3;1Hnew output"))
	next, ok := p.frameAfter(frame, ready)
	if !ok || next <= frame {
		t.Fatal("fresh frame was not distinguished")
	}
	if next != 3 {
		t.Fatalf("frame sequence depended on raw or stripped byte offsets: %d", next)
	}
}
