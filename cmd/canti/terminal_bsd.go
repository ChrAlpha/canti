//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package main

import "golang.org/x/sys/unix"

func flushInput(fd int) error {
	const fRead = 1
	return unix.IoctlSetPointerInt(fd, unix.TIOCFLUSH, fRead)
}
