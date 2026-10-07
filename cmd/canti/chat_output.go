package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const displayTimeout = 250 * time.Millisecond

type chatOutput struct {
	file     *os.File
	mu       sync.Mutex
	fd       int
	socket   bool
	blocking error
	gate     *chatInputGate
	failed   chan<- error
	err      error
}

func newChatOutput(w io.Writer, failed chan<- error) (io.Writer, func(), error) {
	f, ok := w.(*os.File)
	if !ok {
		return w, func() {}, nil
	}
	out := &chatOutput{file: f, fd: int(f.Fd()), failed: failed}
	var stat unix.Stat_t
	if err := unix.Fstat(out.fd, &stat); err != nil {
		return nil, nil, err
	}
	switch stat.Mode & unix.S_IFMT {
	case unix.S_IFCHR, unix.S_IFIFO:
		fd, err := reopenNonblocking(out.fd, unix.O_WRONLY)
		switch {
		case errors.Is(err, unix.EIO):
			return nil, nil, err
		case err != nil:
			out.blocking = err
			return out, func() {}, nil
		}
		out.fd = fd
		return out, out.close, nil
	case unix.S_IFSOCK:
		out.socket = true
	}
	return out, func() {}, nil
}

// reopenNonblocking gives fd's pipe or terminal a new open file description:
// O_NONBLOCK on fd itself would change the one it shares with the shell, and a
// signal can keep the process from restoring it. The reopen needs /proc and
// the access the inode's permissions grant, which a descriptor passed from
// another user lacks, and a pseudo-terminal master would open a new terminal;
// chat then keeps the inherited description.
func reopenNonblocking(fd, mode int) (int, error) {
	if _, err := unix.IoctlGetInt(fd, unix.TIOCGPTN); err == nil {
		return -1, errors.New("a pseudo-terminal master opens a new terminal when reopened")
	}
	path := fmt.Sprintf("/proc/self/fd/%d", fd)
	private, err := unix.Open(path, mode|unix.O_NONBLOCK|unix.O_CLOEXEC|unix.O_NOCTTY, 0)
	if err != nil {
		return -1, &os.PathError{Op: "reopen", Path: path, Err: err}
	}
	return private, nil
}

// blockingNote tells that writes to stream, the chat output out, keep their
// inherited blocking description, or is empty when they do not.
func blockingNote(stream string, out io.Writer) string {
	if o, ok := out.(*chatOutput); ok && o.blocking != nil {
		return fmt.Sprintf("canti chat: %s: %v; writes to it lose the %v display deadline and can block chat\n", stream, o.blocking, displayTimeout)
	}
	return ""
}

func (w *chatOutput) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.err = os.ErrClosed
	_ = unix.Close(w.fd)
}

func (w *chatOutput) write(p []byte) (int, error) {
	if w.socket {
		return unix.SendmsgN(w.fd, p, nil, nil, unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL)
	}
	return unix.Write(w.fd, p)
}

func (w *chatOutput) Read(p []byte) (int, error) { return w.file.Read(p) }

func (w *chatOutput) Close() error { return w.file.Close() }

func (w *chatOutput) Fd() uintptr { return uintptr(w.fd) }

func (w *chatOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	deadline := time.Now().Add(displayTimeout)
	total := 0
	congested := false
	for len(p) > 0 {
		var n int
		var err error
		if congested && w.gate != nil {
			n, err = w.writeGated(p, deadline)
			if w.err != nil {
				break
			}
		} else {
			n, err = w.write(p)
		}
		if n > 0 {
			total += n
			p = p[n:]
		}
		if err == nil && len(p) == 0 {
			return total, nil
		}
		if len(p) > 0 && !congested {
			congested = true
			if w.gate != nil {
				w.gate.congest()
			}
		}
		if err != nil && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			w.err = err
			break
		}
		if w.err = awaitWritable(w.fd, deadline); w.err != nil {
			break
		}
	}
	if w.err != nil {
		select {
		case w.failed <- w.err:
		default:
		}
	}
	return total, w.err
}

func (w *chatOutput) writeGated(p []byte, deadline time.Time) (int, error) {
	g := w.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.busy || g.draining {
		if err := g.drainAvailableLocked(); err != nil {
			w.err = err
			return 0, nil
		}
		g.epoch++
		g.draining = g.draining || g.inPaste || len(g.pending) > 0
		if g.draining {
			g.paste = nil
		}
	}
	if time.Until(deadline) <= 0 {
		w.err = os.ErrDeadlineExceeded
		return 0, nil
	}
	n, err := w.write(p)
	if err == nil && n == len(p) {
		g.outputCongested = false
	}
	return n, err
}

func awaitWritable(fd int, deadline time.Time) error {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return os.ErrDeadlineExceeded
	}
	_, err := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}, int(remaining.Milliseconds())+1)
	if err != nil && !errors.Is(err, unix.EINTR) {
		return err
	}
	return nil
}
