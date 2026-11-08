package main

import (
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestChatOutputKeepsAnUnreopenablePipeBlocking(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reopens a pipe whatever its mode")
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	fd := int(w.Fd())
	if err := unix.Fchmod(fd, 0); err != nil {
		t.Fatal(err)
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		t.Fatal(err)
	}
	out, restore, err := newChatOutput(w, make(chan error, 1))
	if err != nil {
		t.Fatal(err)
	}
	o, ok := out.(*chatOutput)
	if !ok || o.fd != fd {
		t.Fatalf("unreopenable pipe: %#v", out)
	}
	if _, err := io.WriteString(out, "kept"); err != nil {
		t.Fatal(err)
	}
	restore()
	if after, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0); err != nil || after != flags {
		t.Fatalf("output flags changed from %#x to %#x: %v", flags, after, err)
	}
	w.Close()
	if got, err := io.ReadAll(r); err != nil || string(got) != "kept" {
		t.Fatalf("read %q: %v", got, err)
	}
}

func TestChatOutputBackpressureIsBounded(t *testing.T) {
	for _, socket := range []bool{false, true} {
		t.Run(map[bool]string{false: "pipe", true: "socket"}[socket], func(t *testing.T) {
			var r, w *os.File
			if socket {
				fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
				if err != nil {
					t.Fatal(err)
				}
				r, w = os.NewFile(uintptr(fds[0]), "reader"), os.NewFile(uintptr(fds[1]), "writer")
			} else {
				var err error
				if r, w, err = os.Pipe(); err != nil {
					t.Fatal(err)
				}
			}
			defer r.Close()
			defer w.Close()
			fillPipe(t, w)
			fd := w.Fd()
			flags, err := unix.FcntlInt(fd, unix.F_GETFL, 0)
			if err != nil {
				t.Fatal(err)
			}
			failed := make(chan error, 1)
			out, restore, err := newChatOutput(w, failed)
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if after, err := unix.FcntlInt(fd, unix.F_GETFL, 0); err != nil || after != flags {
				t.Fatalf("output flags changed from %#x to %#x: %v", flags, after, err)
			}
			start := time.Now()
			if _, err := io.WriteString(out, "blocked"); err == nil {
				t.Fatal("blocked output succeeded")
			}
			if time.Since(start) > time.Second {
				t.Fatal("display blocked shutdown")
			}
			select {
			case <-failed:
			default:
				t.Fatal("missing failure notification")
			}
			start = time.Now()
			if _, err := out.Write([]byte("again")); !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("sticky failure: %v", err)
			}
			if time.Since(start) > 50*time.Millisecond {
				t.Fatal("failed display retried")
			}
		})
	}
}
