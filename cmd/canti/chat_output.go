package main

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const displayTimeout = 250 * time.Millisecond

type chatOutput struct {
	*os.File
	mu     sync.Mutex
	fd     int
	failed chan<- error
	err    error
}

func newChatOutput(w io.Writer, failed chan<- error) (io.Writer, func(), error) {
	f, ok := w.(*os.File)
	if !ok {
		return w, func() {}, nil
	}
	fd := int(f.Fd())
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		return nil, nil, err
	}
	if err = unix.SetNonblock(fd, true); err != nil {
		return nil, nil, err
	}
	return &chatOutput{File: f, fd: fd, failed: failed}, func() { _, _ = unix.FcntlInt(uintptr(fd), unix.F_SETFL, flags) }, nil
}

func (w *chatOutput) Fd() uintptr { return uintptr(w.fd) }

func (w *chatOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	deadline := time.Now().Add(displayTimeout)
	total := 0
	for len(p) > 0 {
		n, err := unix.Write(w.fd, p)
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
		remaining := time.Until(deadline)
		if remaining <= 0 {
			w.err = os.ErrDeadlineExceeded
			break
		}
		_, err = unix.Poll([]unix.PollFd{{Fd: int32(w.fd), Events: unix.POLLOUT}}, int(remaining.Milliseconds())+1)
		if err != nil && !errors.Is(err, unix.EINTR) {
			w.err = err
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
