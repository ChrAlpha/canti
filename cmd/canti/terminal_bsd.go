//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package main

import (
	"errors"
	"io"

	"golang.org/x/sys/unix"
)

func flushInput(fd int) error {
	const fRead = 1
	return unix.IoctlSetPointerInt(fd, unix.TIOCFLUSH, fRead)
}

func writePrompt(out io.Writer, p []byte, inputFD int, _ <-chan struct{}) error {
	if err := flushInput(inputFD); err != nil && !errors.Is(err, unix.ENOTTY) {
		return err
	}
	_, err := out.Write(p)
	return err
}
