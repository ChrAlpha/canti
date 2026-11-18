package docker

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func waitForDestinationTemp(t *testing.T, dir string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, _ := os.ReadDir(dir)
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".tmp-") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no destination temp file in %s", dir)
		}
		time.Sleep(time.Millisecond)
	}
}

func writeLargeCapture(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(64 << 20); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCancelledCaptureDestinationENOENTKeepsCaptureForRetry(t *testing.T) {
	f := newFake(t)
	f.x.cfg.OutputLimit = 64 << 20
	id := "destination-enoent"
	capture := f.x.capture(id)
	writeLargeCapture(t, filepath.Join(capture, "output"))
	root, err := os.OpenRoot(capture)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	destination := filepath.Join(f.x.artifacts(), id)
	if err := os.MkdirAll(destination, 0755); err != nil {
		t.Fatal(err)
	}
	type result struct {
		job harness.Job
		err error
	}
	done := make(chan result, 1)
	go func() {
		job, err := f.x.cancelledCapture(root, id, harness.Job{})
		done <- result{job, err}
	}()
	waitForDestinationTemp(t, destination)
	moved := destination + ".moved"
	if err := os.Rename(destination, moved); err != nil {
		t.Fatal(err)
	}
	first := <-done
	if first.err == nil || !errors.Is(first.err, os.ErrNotExist) {
		t.Fatalf("destination ENOENT = %+v, %v; want a retryable error", first.job, first.err)
	}
	if _, err := os.Stat(f.x.record(id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination ENOENT wrote a receipt: %v", err)
	}
	if _, err := os.Stat(filepath.Join(capture, "output")); err != nil {
		t.Fatalf("destination ENOENT removed the capture: %v", err)
	}
	if err := os.Rename(moved, destination); err != nil {
		t.Fatal(err)
	}
	job, err := f.x.cancelledCapture(root, id, harness.Job{})
	if err != nil {
		t.Fatalf("retry after restoring destination = %+v, %v", job, err)
	}
	if _, err := os.Stat(filepath.Join(destination, "output")); err != nil {
		t.Fatalf("retry published no artifact: %v", err)
	}
}

func TestAttachedNoCompletionDestinationENOENTKeepsCaptureForRetry(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	x.cfg.OutputLimit = 64 << 20
	e := effectFor(x, "attached-destination", "true")
	if _, err := x.Drive(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	r, found, err := x.readJob(e)
	if err != nil || !found {
		t.Fatalf("read attached job: %+v, %v, %v", r, found, err)
	}
	r.Fenced, r.CancelAt = true, time.Now().UTC()
	writeLargeCapture(t, filepath.Join(f.dir, "spool", e.ID, "output"))
	_, s, err := readEnvironment(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(x.cfg.State, "artifacts", e.ID)
	if err := os.MkdirAll(destination, 0755); err != nil {
		t.Fatal(err)
	}
	type result struct {
		job harness.Job
		err error
	}
	done := make(chan result, 1)
	go func() {
		job, err := x.publishCessation(&s, &r, harness.CessationStopped)
		done <- result{job, err}
	}()
	waitForDestinationTemp(t, destination)
	moved := destination + ".moved"
	if err := os.Rename(destination, moved); err != nil {
		t.Fatal(err)
	}
	first := <-done
	if first.err == nil || !errors.Is(first.err, os.ErrNotExist) {
		t.Fatalf("attached destination ENOENT = %+v, %v; want a retryable error", first.job, first.err)
	}
	if again, found, err := x.readJob(e); err != nil || !found || again.Terminal != nil {
		t.Fatalf("attached destination ENOENT wrote a receipt: %+v, %v, %v", again, found, err)
	}
	if _, err := os.Stat(filepath.Join(f.dir, "spool", e.ID, "output")); err != nil {
		t.Fatalf("attached destination ENOENT removed the capture: %v", err)
	}
	if err := os.Rename(moved, destination); err != nil {
		t.Fatal(err)
	}
	job, err := x.publishCessation(&s, &r, harness.CessationStopped)
	if err != nil {
		t.Fatalf("attached retry after restoring destination = %+v, %v", job, err)
	}
}
