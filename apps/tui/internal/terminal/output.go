package terminal

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

type Output struct {
	file   *os.File
	mu     sync.Mutex
	fd     int
	socket bool
	failed chan<- error
	err    error
}

func NewOutput(w io.Writer, failed chan<- error) (io.Writer, func(), error) {
	f, ok := w.(*os.File)
	if !ok {
		return w, func() {}, nil
	}
	out := &Output{file: f, fd: int(f.Fd()), failed: failed}
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
			return nil, nil, fmt.Errorf("bounded terminal output unavailable: %w", err)
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
// such transports are refused before runtime admission.
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

func (w *Output) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.err = os.ErrClosed
	_ = unix.Close(w.fd)
}

func (w *Output) write(p []byte) (int, error) {
	if w.socket {
		return unix.SendmsgN(w.fd, p, nil, nil, unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL)
	}
	return unix.Write(w.fd, p)
}

func (w *Output) Read(p []byte) (int, error) { return w.file.Read(p) }

func (w *Output) Close() error { return w.file.Close() }

func (w *Output) Fd() uintptr { return uintptr(w.fd) }

func (w *Output) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	deadline := time.Now().Add(displayTimeout)
	total := 0
	for len(p) > 0 {
		n, err := w.write(p)
		if n > 0 {
			total += n
			p = p[n:]
		}
		if err == nil && len(p) == 0 {
			return total, nil
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
