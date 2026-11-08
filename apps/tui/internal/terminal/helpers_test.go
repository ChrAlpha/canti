package terminal

import (
	"os"
	"syscall"
	"testing"
)

func fillPipe(t *testing.T, w *os.File) {
	t.Helper()
	fd := int(w.Fd())
	if err := syscall.SetNonblock(fd, true); err != nil {
		t.Fatal(err)
	}
	for _, chunk := range [][]byte{make([]byte, 4096), {0}} {
		for {
			if _, err := syscall.Write(fd, chunk); err == syscall.EAGAIN {
				break
			} else if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := syscall.SetNonblock(fd, false); err != nil {
		t.Fatal(err)
	}
}
