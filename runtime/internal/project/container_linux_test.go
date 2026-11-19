//go:build linux

package project

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestContainerReadDoesNotBlockOnFIFOEntries(t *testing.T) {
	root := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(root, "AGENTS.md"), 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := ReadContainer(context.Background(), ContainerRequest{Root: root, Workdir: root})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("accepted a FIFO instruction")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FIFO instruction blocked admission")
	}

	if err := os.MkdirAll(filepath.Join(root, ".agents/skills"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(root, ".agents/skills/fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	done = make(chan error, 1)
	go func() {
		_, err := ReadContainer(context.Background(), ContainerRequest{Root: root, Workdir: root})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("FIFO skill directory failed admission: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FIFO skill directory blocked admission")
	}
}
