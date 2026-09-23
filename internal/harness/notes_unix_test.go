//go:build unix

package harness

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestReadNotesRejectsFIFO(t *testing.T) {
	ws := t.TempDir()
	name := filepath.Join(ws, notesName)
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(name, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := ReadNotes(ws, notesName, 64)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ReadNotes() on a FIFO succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReadNotes() blocked on a FIFO")
	}
}
