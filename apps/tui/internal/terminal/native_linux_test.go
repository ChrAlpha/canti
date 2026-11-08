//go:build linux

package terminal

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	uv "github.com/charmbracelet/ultraviolet"
	"github.com/charmbracelet/x/term"
	"github.com/creack/pty"
	"golang.org/x/sys/unix"
)

func TestStreamCancelBlockedDelivery(t *testing.T) {
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	defer w.Close()
	cr, e := uv.NewCancelReader(r)
	if e != nil {
		t.Fatal(e)
	}
	defer cr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan uv.Event)
	done := make(chan error, 1)
	go func() { done <- uv.NewTerminalReader(cr, "xterm").StreamEvents(ctx, events) }()
	w.Write([]byte("x"))
	time.Sleep(50 * time.Millisecond)
	cancel()
	cr.Cancel()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("cancellation blocked on event delivery")
	}
}

func TestPasteOverflowRejectsWholeAndResynchronizes(t *testing.T) {
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	defer w.Close()
	cr, e := uv.NewCancelReader(r)
	if e != nil {
		t.Fatal(e)
	}
	defer cr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer cr.Cancel()
	events := make(chan uv.Event)
	done := make(chan error, 1)
	go func() { done <- uv.NewTerminalReader(cr, "xterm").StreamEvents(ctx, events) }()
	go func() { io.WriteString(w, "\x1b[200~"+strings.Repeat("x", 80<<10)+"\x1b[201~y\x1b[200~ok\x1b[201~") }()
	deadline := time.After(4 * time.Second)
	overflows := 0
	gotY := false
	for {
		select {
		case event := <-events:
			switch e := event.(type) {
			case uv.PasteOverflowEvent:
				overflows++
				if e.Limit != 64<<10 {
					t.Fatal(e)
				}
			case uv.KeyPressEvent:
				if e.Text == "y" {
					gotY = true
				}
			case uv.PasteEvent:
				if e.Content != "ok" || overflows != 1 || !gotY {
					t.Fatalf("invalid recovery paste=%q overflows=%d gotY=%v", e.Content, overflows, gotY)
				}
				cancel()
				cr.Cancel()
				select {
				case <-done:
					return
				case <-deadline:
					t.Fatal("reader stuck")
				}
			}
		case <-deadline:
			t.Fatal("no paste")
		}
	}
}

type quitModel struct {
	seen    chan struct{}
	release chan struct{}
}

func (m quitModel) Init() tea.Cmd  { return nil }
func (m quitModel) View() tea.View { return tea.NewView("test") }
func (m quitModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if _, ok := msg.(tea.InputClosedMsg); ok {
		return m, tea.Quit
	}
	if _, ok := msg.(tea.KeyPressMsg); ok {
		select {
		case m.seen <- struct{}{}:
		default:
		}
		<-m.release
	}
	return m, nil
}
func TestProgramQuitWithPendingNativeEvents(t *testing.T) {
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	defer w.Close()
	m := quitModel{make(chan struct{}, 1), make(chan struct{})}
	p := tea.NewProgram(m, tea.WithInput(r), tea.WithOutput(io.Discard), tea.WithoutSignalHandler())
	done := make(chan error, 1)
	go func() { _, err := p.Run(); done <- err }()
	io.WriteString(w, strings.Repeat("x", 4096))
	select {
	case <-m.seen:
	case <-time.After(time.Second):
		t.Fatal("no key")
	}
	quitSent := make(chan struct{})
	go func() { p.Quit(); close(quitSent) }()
	time.Sleep(30 * time.Millisecond)
	start := time.Now()
	close(m.release)
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("program stuck")
	}
	elapsed := time.Since(start)
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	t.Logf("Program.Run quit elapsed %v; native sendEvents goroutine remains=%v", elapsed, strings.Contains(string(buf[:n]), ".(*TerminalReader).sendEvents"))
	if elapsed > 250*time.Millisecond {
		t.Fatalf("quit exceeded cancellation bound: %v", elapsed)
	}
	if strings.Contains(string(buf[:n]), ".(*TerminalReader).sendEvents") {
		t.Fatal("native input goroutine leaked")
	}
}

func TestPTYNativeRawRestore(t *testing.T) {
	master, slave, e := pty.Open()
	if e != nil {
		t.Fatal(e)
	}
	defer master.Close()
	defer slave.Close()
	fd := slave.Fd()
	before, e := term.GetState(fd)
	if e != nil {
		t.Fatal(e)
	}
	flagsBefore, e := unix.FcntlInt(fd, unix.F_GETFL, 0)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := tea.NewProgram(quitModel{make(chan struct{}, 1), make(chan struct{})}, tea.WithInput(slave), tea.WithOutput(io.Discard), tea.WithContext(ctx), tea.WithoutSignalHandler())
	done := make(chan error, 1)
	go func() { _, e := p.Run(); done <- e }()
	raw := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current, _ := term.GetState(fd)
		if !reflect.DeepEqual(before, current) {
			raw = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !raw {
		t.Fatal("not raw")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel stuck")
	}
	after, e := term.GetState(fd)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("terminal not restored")
	}
	flagsAfter, e := unix.FcntlInt(fd, unix.F_GETFL, 0)
	if e != nil {
		t.Fatal(e)
	}
	if flagsBefore != flagsAfter {
		t.Fatalf("fd flags %x -> %x", flagsBefore, flagsAfter)
	}
}

func TestNativeEOFReachesModel(t *testing.T) {
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	defer w.Close()
	p := tea.NewProgram(quitModel{make(chan struct{}, 1), make(chan struct{})}, tea.WithInput(r), tea.WithOutput(io.Discard), tea.WithoutSignalHandler())
	done := make(chan error, 1)
	go func() { _, e := p.Run(); done <- e }()
	w.Close()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(250 * time.Millisecond):
		p.Kill()
		t.Fatal("EOF not delivered to model")
	}
}

func TestStreamRejectsIncompleteSequenceOverLimit(t *testing.T) {
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	defer w.Close()
	cr, e := uv.NewCancelReader(r)
	if e != nil {
		t.Fatal(e)
	}
	defer cr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan uv.Event)
	done := make(chan error, 1)
	go func() { done <- uv.NewTerminalReader(cr, "xterm").StreamEvents(ctx, events) }()
	go func() { io.WriteString(w, "\x1b]"+strings.Repeat("x", 128<<10)+"\a") }()
	select {
	case e := <-events:
		t.Fatalf("unexpected event %T", e)
	case e := <-done:
		if !errors.Is(e, uv.ErrInputLimit) {
			t.Fatalf("error %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("no limit enforcement")
	}
}

func TestNativeInitFailureRestoresPTY(t *testing.T) {
	if os.Getenv("CANTI_NATIVE_INIT_FAIL") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestNativeInitFailureRestoresPTY$", "-test.v")
		cmd.Env = append(os.Environ(), "CANTI_NATIVE_INIT_FAIL=1")
		output, e := cmd.CombinedOutput()
		t.Log(string(output))
		if e != nil {
			t.Fatal(e)
		}
		return
	}
	master, slave, e := pty.Open()
	if e != nil {
		t.Fatal(e)
	}
	defer master.Close()
	defer slave.Close()
	fd := slave.Fd()
	before, e := term.GetState(fd)
	if e != nil {
		t.Fatal(e)
	}
	defer term.Restore(fd, before)
	var limits unix.Rlimit
	if e = unix.Getrlimit(unix.RLIMIT_NOFILE, &limits); e != nil {
		t.Fatal(e)
	}
	defer unix.Setrlimit(unix.RLIMIT_NOFILE, &limits)
	restricted := limits
	restricted.Cur = 0
	if e = unix.Setrlimit(unix.RLIMIT_NOFILE, &restricted); e != nil {
		t.Fatal(e)
	}
	p := tea.NewProgram(quitModel{make(chan struct{}, 1), make(chan struct{})}, tea.WithInput(slave), tea.WithOutput(io.Discard), tea.WithoutSignalHandler())
	_, runErr := p.Run()
	if e = unix.Setrlimit(unix.RLIMIT_NOFILE, &limits); e != nil {
		t.Fatal(e)
	}
	after, e := term.GetState(fd)
	if e != nil {
		t.Fatal(e)
	}
	if runErr == nil {
		t.Fatal("expected input initialization error")
	}
	t.Logf("initialization error=%v; state restored=%v", runErr, reflect.DeepEqual(before, after))
	if !reflect.DeepEqual(before, after) {
		t.Fatal("native restore missing after initialization error")
	}
}

type eventModel struct{ events chan tea.Msg }

func (m eventModel) Init() tea.Cmd  { return nil }
func (m eventModel) View() tea.View { return tea.NewView("test") }
func (m eventModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg.(type) {
	case tea.PasteOverflowMsg, tea.PasteMsg, tea.KeyPressMsg:
		m.events <- msg
	case tea.InputClosedMsg:
		m.events <- msg
		return m, tea.Quit
	}
	return m, nil
}
func TestTeaInputLimitsReachNativeReader(t *testing.T) {
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	defer w.Close()
	events := make(chan tea.Msg, 8)
	p := tea.NewProgram(eventModel{events}, tea.WithInput(r), tea.WithInputLimits(64, 8), tea.WithOutput(io.Discard), tea.WithoutSignalHandler())
	done := make(chan error, 1)
	go func() { _, e := p.Run(); done <- e }()
	io.WriteString(w, "\x1b[200~123456789\x1b[201~y\x1b[200~ok\x1b[201~")
	w.Close()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		p.Kill()
		t.Fatal("program stuck")
	}
	got := []tea.Msg{}
	for len(events) > 0 {
		got = append(got, <-events)
	}
	if len(got) != 4 {
		t.Fatalf("events %#v", got)
	}
	if overflow, ok := got[0].(tea.PasteOverflowMsg); !ok || overflow.Limit != 8 {
		t.Fatalf("overflow %#v", got[0])
	}
	if key, ok := got[1].(tea.KeyPressMsg); !ok || key.Text != "y" {
		t.Fatalf("recovery key %#v", got[1])
	}
	if paste, ok := got[2].(tea.PasteMsg); !ok || paste.Content != "ok" {
		t.Fatalf("recovery paste %#v", got[2])
	}
}
func TestTeaSequenceLimitReturnsNativeError(t *testing.T) {
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	defer w.Close()
	events := make(chan tea.Msg, 8)
	p := tea.NewProgram(eventModel{events}, tea.WithInput(r), tea.WithInputLimits(64, 8), tea.WithOutput(io.Discard), tea.WithoutSignalHandler())
	done := make(chan error, 1)
	go func() { _, e := p.Run(); done <- e }()
	io.WriteString(w, "\x1b]"+strings.Repeat("x", 100))
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
		msg := <-events
		closed, ok := msg.(tea.InputClosedMsg)
		if !ok || !errors.Is(closed.Err, uv.ErrInputLimit) {
			t.Fatalf("native error %#v", msg)
		}
	case <-time.After(time.Second):
		p.Kill()
		t.Fatal("program stuck")
	}
}

func TestOverflowResyncWithDelayedPasteTerminator(t *testing.T) {
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	defer w.Close()
	cr, e := uv.NewCancelReader(r)
	if e != nil {
		t.Fatal(e)
	}
	defer cr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer cr.Cancel()
	reader := uv.NewTerminalReader(cr, "xterm")
	reader.MaxPasteBytes = 8
	events := make(chan uv.Event, 8)
	done := make(chan error, 1)
	go func() { done <- reader.StreamEvents(ctx, events) }()
	io.WriteString(w, "\x1b[200~123456789\x1b[20")
	time.Sleep(100 * time.Millisecond)
	io.WriteString(w, "1~z")
	deadline := time.After(250 * time.Millisecond)
	overflow := false
	for {
		select {
		case event := <-events:
			switch e := event.(type) {
			case uv.PasteOverflowEvent:
				overflow = true
			case uv.KeyPressEvent:
				if e.Text == "z" && overflow {
					cancel()
					cr.Cancel()
					<-done
					return
				}
			}
		case <-deadline:
			t.Fatal("overflow did not resynchronize after delayed closing delimiter")
		}
	}
}
