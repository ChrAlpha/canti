package hostrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/provider"
	"github.com/ChrAlpha/canti/internal/host"
)

// SocketName is the socket of a host in its state directory, beside the lock
// that admits one host per state directory.
const SocketName = "host.sock"

// maxPath bounds the path of a Unix socket on Linux, without its NUL.
const maxPath = 107

// SocketPath returns the socket path of the host of state, or an error when
// the path is too long for a Unix socket.
func SocketPath(state string) (string, error) {
	p := filepath.Join(state, SocketName)
	if len(p) > maxPath {
		return p, fmt.Errorf("the host socket %s is %d bytes, more than the %d a Unix socket allows; use a shorter -state or -host embedded", p, len(p), maxPath)
	}
	return p, nil
}

// The bounds of one connection: the time to read a request and to write a
// response, the size of a request, and the longest wait.
const (
	readTimeout  = 30 * time.Second
	writeTimeout = 10 * time.Second
	maxRequest   = 1 << 20
	// maxResponse bounds an answer by what the valid facts it carries can
	// take as JSON, which writes any string in at most 6 bytes per byte (a
	// control byte as \u00XX): a run's full result, a decision payload that
	// a complete model response of at most provider.DefaultMaxResponseBytes
	// carried, its goal and constraints, which a request of at most
	// maxRequest carried, and 1 MiB of every other field.
	maxResponse    = 6*(provider.DefaultMaxResponseBytes+maxRequest) + 1<<20
	maxWait        = 30 * time.Second
	defaultCleanup = 30 * time.Second
	maxCleanup     = 10 * time.Minute
)

// cleanupOf is the cleanup deadline a request names in milliseconds:
// defaultCleanup when it names none, and at most maxCleanup.
func cleanupOf(ms int64) time.Duration {
	if ms <= 0 {
		return defaultCleanup
	}
	return min(time.Duration(min(ms, int64(maxCleanup/time.Millisecond)))*time.Millisecond, maxCleanup)
}

// Server serves the operations of one host on a listener.
type Server struct {
	Host    *host.Host
	Version string
	Logger  *slog.Logger
	// Shutdown is called when a client asks the host to shut down.
	Shutdown func()

	base     context.Context // outlives every connection; Close ends it
	end      context.CancelFunc
	active   atomic.Int64
	lastMu   sync.Mutex
	lastSeen time.Time
	conns    sync.WaitGroup

	mu      sync.Mutex
	ln      net.Listener
	open    map[net.Conn]struct{}
	closing bool
}

// Serve accepts connections on ln until it is closed, serving each one
// request in its own goroutine. Operations that outlive their connection, a
// drive or a stop's cleanup, belong to the host, not to the connection, so a
// client that goes away detaches from them and stops nothing.
//
// Each request is an operation of the host (Host.Enter): the host's shutdown
// awaits it before it releases the state directory. Close stops accepting,
// ends the context of every request, and makes the reads and writes of every
// open connection fail at once.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	s.mu.Lock()
	s.base, s.end = context.WithCancel(context.WithoutCancel(ctx))
	s.ln, s.open = ln, map[net.Conn]struct{}{}
	closing := s.closing
	s.mu.Unlock()
	if closing {
		ln.Close()
	}
	if s.Logger == nil {
		s.Logger = slog.New(slog.DiscardHandler)
	}
	s.touch()
	for {
		conn, err := ln.Accept()
		if err != nil {
			s.conns.Wait()
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if !s.track(conn) {
			conn.Close()
			continue
		}
		s.active.Add(1)
		s.conns.Add(1)
		go func() {
			defer s.conns.Done()
			defer s.active.Add(-1)
			defer s.touch()
			defer s.untrack(conn)
			s.serve(conn)
		}()
	}
}

// Close stops the server: it closes the listener, which removes the socket,
// ends the context of every request in flight, and expires the deadlines of
// every open connection, so that no connection waits on a client. Serve
// returns once every connection has ended.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return
	}
	s.closing = true
	if s.ln != nil {
		s.ln.Close()
	}
	if s.end != nil {
		s.end()
	}
	for c := range s.open {
		c.SetDeadline(time.Now())
	}
}

func (s *Server) track(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.open[c] = struct{}{}
	return true
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.open, c)
	s.mu.Unlock()
}

// Idle reports whether the server has had no connection and the host no
// drive for at least d.
func (s *Server) Idle(d time.Duration) bool {
	if s.active.Load() > 0 || s.Host.Busy() {
		s.touch()
		return false
	}
	s.lastMu.Lock()
	defer s.lastMu.Unlock()
	return time.Since(s.lastSeen) >= d
}

func (s *Server) touch() {
	s.lastMu.Lock()
	s.lastSeen = time.Now()
	s.lastMu.Unlock()
}

// serve answers the one request of conn, refusing a peer of another user.
func (s *Server) serve(conn net.Conn) {
	defer conn.Close()
	if err := samePeer(conn); err != nil {
		s.Logger.Warn("refused a connection", "error", err)
		return
	}
	conn.SetReadDeadline(time.Now().Add(readTimeout))
	var req Request
	resp := Response{Protocol: Protocol}
	if err := readRequest(conn, &req); err != nil {
		resp.Error = &Error{Code: CodeFailed, Message: fmt.Sprintf("hostrpc: unreadable request: %v", err)}
	} else if req.Protocol != Protocol {
		resp.Error = &Error{Code: CodeProtocol, Message: fmt.Sprintf("the client speaks host protocol %d and this host, pid %d, speaks %d; "+
			"stop it with the canti that started it, or with \"kill -TERM %[2]d\" once it drives nothing", req.Protocol, os.Getpid(), Protocol)}
	} else if ctx, leave, err := s.Host.Enter(s.base); err != nil {
		resp.Error = errorOf(err)
	} else {
		resp = s.do(ctx, req)
		leave()
	}
	body, err := encodeResponse(resp)
	if err != nil {
		s.Logger.Warn("refused to answer", "op", req.Op, "error", err)
		body, _ = encodeResponse(Response{Protocol: Protocol, Error: &Error{Code: CodeFailed, Message: err.Error()}})
	}
	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if _, err := conn.Write(body); err != nil {
		s.Logger.Warn("dropped a response", "op", req.Op, "error", err)
	}
}

// encodeResponse encodes resp as one line, refusing one over maxResponse.
func encodeResponse(resp Response) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(resp); err != nil {
		return nil, err
	}
	if b.Len() > maxResponse {
		return nil, fmt.Errorf("hostrpc: the answer of %d bytes exceeds the %d a response may hold", b.Len(), maxResponse)
	}
	return b.Bytes(), nil
}

// readRequest reads one request of at most maxRequest bytes into req. It
// reads the protocol version first and decodes the request strictly, with no
// unknown field, only in this version, so that a request of another version
// is refused as such and none of its fields is interpreted.
func readRequest(conn net.Conn, req *Request) error {
	var raw json.RawMessage
	if err := json.NewDecoder(io.LimitReader(conn, maxRequest)).Decode(&raw); err != nil {
		return err
	}
	var version struct {
		Protocol int `json:"protocol"`
	}
	if err := json.Unmarshal(raw, &version); err != nil {
		return err
	}
	if version.Protocol != Protocol {
		req.Protocol = version.Protocol
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	return d.Decode(req)
}

// samePeer refuses a peer whose user is not the host's.
func samePeer(conn net.Conn) error {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return errors.New("hostrpc: not a Unix connection")
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return err
	}
	var cred *unix.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) { cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil {
		return err
	}
	if credErr != nil {
		return credErr
	}
	if int(cred.Uid) != os.Getuid() {
		return fmt.Errorf("hostrpc: peer uid %d is not %d", cred.Uid, os.Getuid())
	}
	return nil
}

func (s *Server) do(ctx context.Context, req Request) Response {
	resp := Response{Protocol: Protocol}
	var err error
	switch req.Op {
	case OpHello:
		resp.Hello = &Hello{Protocol: Protocol, Version: s.Version, ContextVersion: host.ContextVersion, SemanticsVersion: core.SemanticsVersion,
			PID: os.Getpid(), Bindings: PairsOf(s.Host.Bindings())}
	case OpSubmit:
		resp.Ack, err = s.submit(ctx, req)
	case OpDrive:
		_, err = s.Host.Drive(ctx, req.RunID, req.Binding.binding(), host.DriveOptions{Once: req.Once})
		if err == nil {
			resp.Run, err = s.view(ctx, req.RunID)
		}
	case OpInspect:
		var sample host.Inspection
		sample, err = s.Host.Inspect(ctx, req.RunID)
		if err == nil {
			wire := InspectionOf(sample)
			resp.Inspection = &wire
		}
	case OpView:
		resp.Run, err = s.view(ctx, req.RunID)
	case OpProgress:
		var c host.Cursor
		var p host.Page
		if c, err = ParseCursor(req.Cursor); err == nil {
			p, err = s.Host.Progress(ctx, c, req.Limit)
		}
		if err == nil {
			w := PageOf(p)
			resp.Page = &w
		}
	case OpWait:
		resp.Wait, err = s.wait(ctx, req)
	case OpStop:
		resp.Run, err = s.stop(ctx, req)
	case OpResolve:
		resp.Run, err = s.resolve(ctx, req)
	case OpInherited:
		var c string
		if c, err = s.Host.InheritedConstraints(ctx, req.SessionID); err == nil {
			resp.Constraints = &c
		}
	case OpShutdown:
		if s.Shutdown != nil {
			go s.Shutdown()
		}
	default:
		err = fmt.Errorf("hostrpc: unknown operation %q", req.Op)
	}
	if err != nil {
		resp.Error = errorOf(err)
	}
	return resp
}

func (s *Server) submit(ctx context.Context, req Request) (*Ack, error) {
	if req.Submit == nil {
		return nil, errors.New("hostrpc: submit without a submission")
	}
	spec, err := req.Submit.Spec.spec()
	if err != nil {
		return nil, host.SpecError{Err: err}
	}
	switch {
	case spec.Config.ContextVersion != host.ContextVersion:
		return nil, host.SpecError{Err: fmt.Errorf("the client creates runs of context version %d and this host renders %d", spec.Config.ContextVersion, host.ContextVersion)}
	case spec.Config.Workspace != s.Host.Workspace():
		return nil, host.SpecError{Err: fmt.Errorf("the run's workspace %s is not the host's %s", spec.Config.Workspace, s.Host.Workspace())}
	}
	sub := req.Submit
	ack, err := s.Host.Submit(ctx, host.Submission{SessionID: sub.SessionID, InputID: sub.InputID, Goal: sub.Goal, Constraints: sub.Constraints,
		ExplicitConstraints: sub.ExplicitConstraints, Spec: spec, Binding: req.Binding.binding(), Now: time.Now()})
	if err != nil {
		return nil, err
	}
	var driveErr error
	if !ack.Replayed {
		_, driveErr = s.Host.Drive(ctx, ack.Run.ID, req.Binding.binding(), host.DriveOptions{Once: req.Once})
	}
	v, err := s.Host.View(ctx, ack.Run.ID)
	if err != nil {
		return nil, err
	}
	return &Ack{Replayed: ack.Replayed, Differs: append([]string{}, ack.Differs...), Run: RunOf(v, cmpErr(driveErr, s.Host.DriveError(ack.Run.ID)))}, nil
}

func cmpErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) view(ctx context.Context, id string) (*Run, error) {
	v, err := s.Host.View(ctx, id)
	if err != nil {
		return nil, err
	}
	w := RunOf(v, s.Host.DriveError(v.Run.ID))
	return &w, nil
}

func (s *Server) wait(ctx context.Context, req Request) (*Wait, error) {
	d := min(time.Duration(req.TimeoutMS)*time.Millisecond, maxWait)
	if d <= 0 {
		d = maxWait
	}
	c, err := ParseCursor(req.Cursor)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	r, err := s.Host.Wait(ctx, c)
	if errors.Is(err, context.DeadlineExceeded) {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	return &Wait{Cursor: CursorOf(r.Cursor), Halted: r.Halted, Driver: driver(r.Driving)}, nil
}

// stop carries out a stop within the host's lifetime, not the connection's:
// a client that goes away abandons nothing. Its cleanup is bounded by the
// request, or by defaultCleanup, and never by more than maxCleanup; the
// host's shutdown ends it too.
func (s *Server) stop(ctx context.Context, req Request) (*Run, error) {
	ctx, cancel := context.WithTimeout(ctx, cleanupOf(req.CleanupMS))
	defer cancel()
	snap, stopErr := s.Host.Stop(ctx, req.RunID)
	run, err := s.view(ctx, cmpID(req.RunID, snap.State.RunID))
	if stopErr != nil {
		return run, stopErr
	}
	return run, err
}

func cmpID(ids ...string) string {
	for _, id := range ids {
		if id != "" {
			return id
		}
	}
	return ""
}

func (s *Server) resolve(ctx context.Context, req Request) (*Run, error) {
	r := req.Resolve
	if r == nil {
		return nil, errors.New("hostrpc: resolve without a resolution")
	}
	ctx, cancel := context.WithTimeout(ctx, maxCleanup)
	defer cancel()
	var err error
	if r.Job {
		_, err = s.Host.ResolveJob(ctx, req.RunID)
	} else {
		execution := value(executionWords, r.Execution)
		if execution == "" {
			return nil, host.SpecError{Err: fmt.Errorf("hostrpc: %q is no execution state of the protocol", r.Execution)}
		}
		obs := harness.Observation{Execution: execution, ExitCode: r.ExitCode, Preview: r.Note, Output: harness.ArtifactRef{Detail: "resolved by operator"}}
		_, err = s.Host.Resolve(ctx, req.RunID, obs, r.OwnerID)
	}
	run, viewErr := s.view(ctx, req.RunID)
	return run, cmpErr(err, viewErr)
}

// errorOf classifies err for the wire.
func errorOf(err error) *Error {
	code := CodeFailed
	switch {
	case errors.As(err, new(host.SpecError)):
		code = CodeSpec
	case errors.Is(err, host.ErrConflict):
		code = CodeConflict
	case errors.Is(err, host.ErrClosing):
		code = CodeClosing
	case errors.Is(err, host.ErrDriving):
		code = CodeDriving
	case errors.Is(err, host.ErrCursor):
		code = CodeCursor
	}
	return &Error{Code: code, Message: err.Error()}
}
