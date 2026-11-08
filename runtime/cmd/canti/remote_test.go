package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/host"
	hostserver "github.com/ChrAlpha/canti/runtime/internal/hostrpc"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

// inProcessHost serves a host of a new state directory from this process,
// on the listener listen returns for its socket, which exists before the
// client is returned, and returns an app that is a client of it with the
// fixture's key, showing its steps on steps. The host holds the state
// directory's lock all along and its socket always answers or is gone with
// the lock held, so the client never starts a host process of its own.
func inProcessHost(t *testing.T, f *fixture, steps io.Writer, listen func(path string) net.Listener) (*app, *host.Host) {
	t.Helper()
	if err := os.MkdirAll(f.state, 0o700); err != nil {
		t.Fatal(err)
	}
	h, err := host.Open(context.Background(), host.Options{Workspace: f.workspace, State: f.state}, true)
	if err != nil {
		t.Fatal(err)
	}
	path, err := hostrpc.SocketPath(f.state)
	if err != nil {
		t.Fatal(err)
	}
	srv := &hostserver.Server{Host: h, Version: "test"}
	ln := listen(path)
	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background(), ln) }()
	t.Cleanup(func() {
		srv.Close()
		h.Shutdown(context.Background())
		<-done
	})
	var stderr bytes.Buffer
	a := &app{stdout: io.Discard, stderr: &stderr, log: slog.New(slog.DiscardHandler), workspace: f.workspace, state: f.state, steps: steps,
		stopSignals: make(chan os.Signal, 2), cred: host.Keys{AnthropicKey: f.key}, hostMode: hostAuto, hostWait: 10 * time.Second}
	return a, h
}

func listenAt(t *testing.T) func(string) net.Listener {
	return func(path string) net.Listener {
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		return ln
	}
}

// losing is a listener whose second connection, the first after a Hello,
// loses its answer: its write waits until halted reports true, then fails
// and closes, as a connection does that breaks before its answer arrives.
type losing struct {
	net.Listener
	n      atomic.Int32
	halted func() bool
}

func (l *losing) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil || l.n.Add(1) != 2 {
		return c, err
	}
	return &losingConn{Conn: c, halted: l.halted}, nil
}

type losingConn struct {
	net.Conn
	halted func() bool
}

// SyscallConn is the connection's, so that the host checks its peer.
func (c *losingConn) SyscallConn() (syscall.RawConn, error) {
	return c.Conn.(syscall.Conn).SyscallConn()
}

func (c *losingConn) Write([]byte) (int, error) {
	for deadline := time.Now().Add(10 * time.Second); !c.halted() && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
	}
	c.Conn.Close()
	return 0, errors.New("the answer is lost")
}

// TestALostAnswerShowsTheWholeRunAlsoOnceItHalted requires that a client
// whose own submission's answer is lost while the host it reached drives the
// run to its halt shows, after its retry replays the halted run, every step
// of it from its start, not only its result.
func TestALostAnswerShowsTheWholeRunAlsoOnceItHalted(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "canti:nothing first"}, 2: {Kind: core.Exec, Payload: "canti:nothing second"},
		3: {Kind: core.Return, Payload: "done"}})
	var steps syncBuffer
	var h *host.Host
	var once sync.Once
	halted := func() bool {
		v, err := h.View(context.Background(), "")
		return err == nil && v.Snapshot.State.Phase == core.PhaseReturned && !v.Driving
	}
	a, h := inProcessHost(t, f, &steps, func(path string) net.Listener {
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		var l net.Listener
		once.Do(func() { l = &losing{Listener: ln, halted: func() bool { return h != nil && halted() }} })
		return l
	})
	s := runSpec(t)
	s.Config.Workspace = f.workspace
	code, err := a.remoteSubmit(context.Background(), hostrpc.Submission{InputID: "in-1", Goal: "g", ExplicitConstraints: true, Spec: hostserver.SpecOf(s)},
		host.Binding{Provider: f.model.URL, Keys: a.cred}, false)
	if err != nil || code != exitOK {
		t.Fatalf("remoteSubmit = %d, %v\nstderr: %s", code, err, a.stderr)
	}
	shown := steps.String()
	if !strings.Contains(shown, "$ canti:nothing first\n") || !strings.Contains(shown, "$ canti:nothing second\n") || len(f.model.requests()) != 3 {
		t.Fatalf("after a lost answer the client showed:\n%s\nwith %d model requests", shown, len(f.model.requests()))
	}
}

// TestAHelloLostAsTheHostStopsServingIsAskedAgain requires that a client
// whose Hello a host accepted into its listener's queue and dropped as it
// stopped serving asks again, within -host-wait, and reaches the host that
// serves next, instead of failing.
func TestAHelloLostAsTheHostStopsServingIsAskedAgain(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	serve := make(chan string, 1)
	a, _ := inProcessHost(t, f, nil, func(path string) net.Listener {
		stale, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			time.Sleep(200 * time.Millisecond)
			stale.Close()
			serve <- path
		}()
		return &later{ready: serve, t: t}
	})
	start := time.Now()
	c, err := a.connect(context.Background())
	if err != nil {
		t.Fatalf("connect after a Hello lost at %v = %v", time.Since(start), err)
	}
	if _, err := c.Hello(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// later is a listener that listens at the path ready names once it does,
// standing in for the host that serves next.
type later struct {
	ready  <-chan string
	t      *testing.T
	mu     sync.Mutex
	ln     net.Listener
	closed bool
}

func (l *later) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.ln == nil && !l.closed {
		l.mu.Unlock()
		path := <-l.ready
		ln, err := net.Listen("unix", path)
		if err != nil {
			l.t.Error(err)
			return nil, net.ErrClosed
		}
		l.mu.Lock()
		if l.closed {
			ln.Close()
		}
		l.ln = ln
	}
	ln, closed := l.ln, l.closed
	l.mu.Unlock()
	if closed {
		return nil, net.ErrClosed
	}
	return ln.Accept()
}

func (l *later) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	if l.ln != nil {
		return l.ln.Close()
	}
	return nil
}

func (l *later) Addr() net.Addr { return &net.UnixAddr{Name: "later", Net: "unix"} }

// syncBuffer is a buffer that a follower writes and a test reads.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.b.String() }
