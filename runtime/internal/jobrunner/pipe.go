package jobrunner

import (
	"errors"
	"io"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// writersRemain reports whether a process may still hold a write end of the
// capture pipe that r reads. The kernel reports POLLHUP on a pipe's read end
// once every write end is closed, so EOF is then certain however late the
// collector runs. A poll that gives no answer counts as a remaining writer.
func writersRemain(r *os.File) bool {
	conn, err := r.SyscallConn()
	if err != nil {
		return true
	}
	hup := false
	err = conn.Control(func(fd uintptr) {
		fds := []unix.PollFd{{Fd: int32(fd)}}
		for {
			_, err := unix.Poll(fds, 0)
			if err != unix.EINTR {
				hup = err == nil && fds[0].Revents&unix.POLLHUP != 0
				return
			}
		}
	})
	return err != nil || !hup
}

// drain keeps the bytes already in the pipe when capture stops waiting for its
// EOF. It reads only the count pending at that moment and never waits, so a
// writer that keeps writing cannot extend capture.
func (c *capture) drain(r io.Reader, w io.Writer, limit int64, buf []byte) {
	file, ok := r.(syscall.Conn)
	if !ok {
		return
	}
	conn, err := file.SyscallConn()
	if err != nil {
		c.err = errors.Join(c.err, err)
		return
	}
	var pending int
	var readErr error
	// TIOCINQ is Linux's FIONREAD: the bytes a read would return now.
	err = conn.Control(func(fd uintptr) { pending, readErr = unix.IoctlGetInt(int(fd), unix.TIOCINQ) })
	for err == nil && readErr == nil && pending > 0 {
		n := 0
		err = conn.Control(func(fd uintptr) { n, readErr = unix.Read(int(fd), buf[:min(len(buf), pending)]) })
		if n <= 0 {
			break
		}
		c.write(w, buf[:n], limit)
		pending -= n
	}
	if err = errors.Join(err, readErr); err != nil && !errors.Is(err, unix.EAGAIN) {
		c.err = errors.Join(c.err, err)
	}
}
