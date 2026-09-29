package main

import (
	"errors"
	"fmt"
	"io"
	"syscall"

	"golang.org/x/sys/unix"
)

func flushInput(fd int) error {
	return unix.IoctlSetInt(fd, unix.TCFLSH, unix.TCIFLUSH)
}

func writePrompt(out io.Writer, p []byte, inputFD int, closing <-chan struct{}) error {
	conn, ok := out.(syscall.Conn)
	if !ok {
		return errors.New("terminal prompt requires a file descriptor")
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var writeErr error
	err = raw.Control(func(fd uintptr) { writeErr = writePromptFD(int(fd), p, inputFD, closing) })
	return errors.Join(err, writeErr)
}

func writePromptFD(fd int, p []byte, inputFD int, closing <-chan struct{}) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return err
	}
	kind := stat.Mode & unix.S_IFMT
	switch kind {
	case unix.S_IFIFO, unix.S_IFCHR:
		// Reopen rather than dup: O_NONBLOCK must not change the inherited
		// open file description shared with the shell or another stream.
		promptFD, err := unix.Open(fmt.Sprintf("/proc/self/fd/%d", fd), unix.O_WRONLY|unix.O_NONBLOCK|unix.O_CLOEXEC|unix.O_NOCTTY, 0)
		if err != nil {
			return fmt.Errorf("opening nonblocking prompt output: %w", err)
		}
		defer unix.Close(promptFD)
		fd = promptFD
	case unix.S_IFREG, unix.S_IFSOCK:
	default:
		return errors.New("unsupported terminal prompt output")
	}
	poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}
	for len(p) > 0 {
		select {
		case <-closing:
			return io.ErrClosedPipe
		default:
		}
		if _, err := unix.Poll(poll, 50); errors.Is(err, unix.EINTR) {
			continue
		} else if err != nil {
			return err
		}
		if poll[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
			return io.ErrClosedPipe
		}
		if poll[0].Revents&unix.POLLOUT == 0 {
			continue
		}
		if err := flushInput(inputFD); err != nil && !errors.Is(err, unix.ENOTTY) {
			return fmt.Errorf("discarding busy terminal input: %w", err)
		}
		// No flush follows a successful publication: fresh input may already
		// have arrived before the writer's acknowledgement is consumed.
		var n int
		var err error
		if kind == unix.S_IFSOCK {
			n, err = unix.SendmsgN(fd, p, nil, nil, unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL)
		} else {
			n, err = unix.Write(fd, p)
		}
		if n > 0 {
			p = p[n:]
		}
		if errors.Is(err, unix.EINTR) || errors.Is(err, unix.EAGAIN) {
			continue
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
