//go:build linux

package main

import (
	"errors"
	"os"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/term"
	"github.com/creack/pty"
	"golang.org/x/sys/unix"
)

func TestChatInputReadsItsOwnNonblockingDescription(t *testing.T) {
	for _, unreopenable := range []bool{false, true} {
		t.Run(map[bool]string{false: "reopened", true: "unreopenable terminal"}[unreopenable], func(t *testing.T) {
			if unreopenable && os.Geteuid() == 0 {
				t.Skip("root reopens a terminal whatever its mode")
			}
			master, slave, err := pty.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer master.Close()
			defer slave.Close()
			if unreopenable {
				if err := unix.Fchmod(int(slave.Fd()), 0); err != nil {
					t.Fatal(err)
				}
			}
			inherited := int(slave.Fd())
			flags, err := unix.FcntlInt(uintptr(inherited), unix.F_GETFL, 0)
			if err != nil {
				t.Fatal(err)
			}
			g, cleanup, err := startChatInput(slave, 32768, func(tea.Msg) {})
			if err != nil {
				t.Fatal(err)
			}
			fd := g.fd
			own, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
			if cleanupErr := cleanup(); err != nil || cleanupErr != nil {
				t.Fatal(err, cleanupErr)
			}
			if after, err := unix.FcntlInt(uintptr(inherited), unix.F_GETFL, 0); err != nil || after != flags {
				t.Fatalf("inherited file status flags %#x, then %#x: %v", flags, after, err)
			}
			_, closedErr := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
			switch {
			case unreopenable && fd != inherited:
				t.Fatalf("read descriptor %d, want the inherited %d of an unreopenable terminal", fd, inherited)
			case !unreopenable && (fd == inherited || own&unix.O_NONBLOCK == 0 || !errors.Is(closedErr, unix.EBADF)):
				t.Fatalf("read descriptor %d (inherited %d) with flags %#x, closed by cleanup: %v", fd, inherited, own, closedErr)
			}
		})
	}
}

func TestChatInputTerminalReadNeverWaitsForInput(t *testing.T) {
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer slave.Close()
	fd := int(slave.Fd())
	before, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		t.Fatal(err)
	}
	restore, err := rawInput(slave.Fd())
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan error, 1)
	go func() {
		n, err := unix.Read(fd, make([]byte, 16))
		if err == nil && n != 0 {
			err = errors.New("read input that was never written")
		}
		got <- err
	}()
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		master.Write([]byte("x"))
		t.Fatal("a read of the raw chat terminal waited for input")
	}
	if err := restore(); err != nil {
		t.Fatal(err)
	}
	if after, err := unix.IoctlGetTermios(fd, unix.TCGETS); err != nil || *after != *before {
		t.Fatalf("restored terminal mode %+v, want %+v: %v", after, before, err)
	}
}

func TestChatInputReadsABlockingTerminalOnlyWhenInputIsReady(t *testing.T) {
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer slave.Close()
	if _, err := term.MakeRaw(slave.Fd()); err != nil {
		t.Fatal(err)
	}
	g := &chatInputGate{goalLimit: 32768, fd: int(slave.Fd())}
	read := func() (int, bool, error) {
		type result struct {
			n     int
			ready bool
			err   error
		}
		got := make(chan result, 1)
		go func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			n, ready, err := g.readReadyLocked(make([]byte, 16))
			got <- result{n, ready, err}
		}()
		select {
		case r := <-got:
			return r.n, r.ready, r.err
		case <-time.After(2 * time.Second):
			master.Write([]byte("x"))
			t.Fatal("a read of the blocking terminal waited for input it did not have")
			return 0, false, nil
		}
	}
	if n, ready, err := read(); n != 0 || ready || err != nil {
		t.Fatalf("read without input = %d, %v, %v", n, ready, err)
	}
	if _, err := master.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		n, ready, err := read()
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			if n != 2 {
				t.Fatalf("read %d bytes of the 2 written", n)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("written input never became ready")
		}
		time.Sleep(time.Millisecond)
	}
	if n, ready, err := read(); n != 0 || ready || err != nil {
		t.Fatalf("read after the input was taken = %d, %v, %v", n, ready, err)
	}
}

func TestChatOutputBackpressureDiscardsInputBeforeReopening(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer inR.Close()
	defer inW.Close()
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer outR.Close()
	defer outW.Close()
	inFD, outReadFD, outWriteFD := int(inR.Fd()), int(outR.Fd()), int(outW.Fd())
	if err := unix.SetNonblock(inFD, true); err != nil {
		t.Fatal(err)
	}
	if err := unix.SetNonblock(outWriteFD, true); err != nil {
		t.Fatal(err)
	}
	if err := unix.SetNonblock(outReadFD, true); err != nil {
		t.Fatal(err)
	}
	fill := make([]byte, 4096)
	for {
		_, err := unix.Write(outWriteFD, fill)
		if errors.Is(err, unix.EAGAIN) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	g := &chatInputGate{goalLimit: 32768, fd: inFD, outputFD: outWriteFD}
	if got := g.decode([]byte("stale\r")); len(got.events) != 0 {
		t.Fatalf("accepted input before blocked frame: %+v", got.events)
	}
	if got := g.decode([]byte("\x1b[200~old paste")); len(got.events) != 0 || !g.discarding() {
		t.Fatalf("blocked paste was not marked for draining: %+v", got.events)
	}
	w := &chatOutput{file: outW, fd: outWriteFD, gate: g, failed: make(chan error, 1)}
	done := make(chan error, 1)
	go func() { _, err := w.Write([]byte("frame")); done <- err }()
	congested := func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.outputCongested
	}
	deadline := time.Now().Add(100 * time.Millisecond)
	for !congested() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !congested() {
		t.Fatal("output did not report congestion")
	}
	if _, err := inW.Write([]byte("queued\r")); err != nil {
		t.Fatal(err)
	}
	if got := g.decode([]byte("more\r")); len(got.events) != 0 {
		t.Fatalf("accepted input during blocked frame: %+v", got.events)
	}
	buf := make([]byte, 65536)
	if _, err := unix.Read(outReadFD, buf); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("short output block did not recover")
	}
	if _, err := unix.Read(inFD, buf); !errors.Is(err, unix.EAGAIN) {
		t.Fatalf("queued input remained after output recovery: %v", err)
	}
	if got := g.decode([]byte(" tail\x1b[201~stale\r")); len(got.events) != 0 || g.discarding() {
		t.Fatalf("blocked paste crossed recovery: %+v", got.events)
	}
	if got := g.decode([]byte("fresh\r")); len(got.events) != 6 {
		t.Fatalf("fresh input unavailable after frame: %+v", got.events)
	}
	for {
		_, err := unix.Write(outWriteFD, fill)
		if errors.Is(err, unix.EAGAIN) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	go func() { _, err := w.Write([]byte("idle frame")); done <- err }()
	deadline = time.Now().Add(100 * time.Millisecond)
	for !congested() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !congested() {
		t.Fatal("idle output did not report congestion")
	}
	if _, err := inW.Write([]byte{3}); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Read(outReadFD, buf); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("idle output did not recover")
	}
	g.mu.Lock()
	interrupts := g.pendingInterrupts
	g.pendingInterrupts = 0
	g.mu.Unlock()
	if interrupts != 1 {
		t.Fatalf("idle Ctrl+C was lost during output drain: %d", interrupts)
	}
	g.enterBusy()
	for {
		_, err := unix.Write(outWriteFD, fill)
		if errors.Is(err, unix.EAGAIN) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	go func() { _, err := w.Write([]byte("busy frame")); done <- err }()
	deadline = time.Now().Add(100 * time.Millisecond)
	for !congested() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !congested() {
		t.Fatal("busy output did not report congestion")
	}
	if _, err := inW.Write([]byte{3}); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Read(outReadFD, buf); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("busy output did not recover")
	}
	n, err := unix.Read(inFD, buf)
	if err != nil || n != 1 {
		t.Fatalf("busy Ctrl+C was drained: n=%d err=%v", n, err)
	}
	if got := g.decode(buf[:n]); len(got.events) != 1 {
		t.Fatalf("busy Ctrl+C was not delivered: %+v", got.events)
	}
}

func TestChatInputDrainedInterruptReachesProgram(t *testing.T) {
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer slave.Close()
	msgs := make(chan tea.Msg, 2)
	g, stop, err := startChatInput(slave, 32768, func(msg tea.Msg) { msgs <- msg })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	}()
	g.mu.Lock()
	_, writeErr := master.Write([]byte{3})
	if writeErr != nil {
		g.mu.Unlock()
		t.Fatal(writeErr)
	}
	fds := []unix.PollFd{{Fd: int32(g.fd), Events: unix.POLLIN}}
	n, pollErr := unix.Poll(fds, 100)
	if pollErr != nil || n == 0 || fds[0].Revents&unix.POLLIN == 0 {
		g.mu.Unlock()
		t.Fatalf("terminal interrupt was not readable: n=%d err=%v", n, pollErr)
	}
	drainErr := g.drainAvailableLocked()
	g.mu.Unlock()
	if drainErr != nil {
		t.Fatal(drainErr)
	}
	select {
	case msg := <-msgs:
		got, ok := msg.(chatSignal)
		if !ok || got.sig != os.Interrupt {
			t.Fatalf("drained Ctrl+C was not delivered as an interrupt: %#v", msg)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("drained Ctrl+C was not delivered")
	}
}
