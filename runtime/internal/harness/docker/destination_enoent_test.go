package docker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
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

type destinationResult struct {
	job harness.Job
	err error
}

func waitForDestinationTempOrDone(t *testing.T, dir string, done chan destinationResult) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		entries, _ := os.ReadDir(dir)
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".tmp-") {
				return
			}
		}
		select {
		case got := <-done:
			t.Fatalf("cancel finished before destination copy: %+v", got)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("no destination temp file in %s", dir)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAttachedTerminationDestinationENOENTKeepsCaptureForRetry(t *testing.T) {
	for _, size := range []int64{32 << 20, 64 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			f := newAttachedFake(t)
			x := f.executor(t)
			x.cfg.OutputLimit = size
			e := effectFor(x, "attached-termination-destination", "true")
			if _, err := x.Drive(t.Context(), e); err != nil {
				t.Fatal(err)
			}
			record, found, err := x.readJob(e)
			if err != nil || !found {
				t.Fatalf("read attached job: %+v, %v, %v", record, found, err)
			}
			output := filepath.Join(f.dir, "spool", e.ID, "output")
			writeLargeCapture(t, output)
			if err := os.Truncate(output, size); err != nil {
				t.Fatal(err)
			}
			h := sha256.New()
			_, _ = h.Write(make([]byte, size))
			code := 0
			now := time.Now().UTC()
			rec := jobrunner.Record{Protocol: jobrunner.Protocol, EffectID: e.ID, Spec: e.Spec, Generation: record.Generation, StartedAt: now.Add(-time.Second), FinishedAt: now, ExitCode: &code, Bytes: size, Stored: size, Digest: hex.EncodeToString(h.Sum(nil))}
			data, err := json.Marshal(rec)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(f.dir, "spool", e.ID, "completion.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			record.Fenced, record.CancelAt = true, now
			record.Termination = &terminationProof{Protocol: jobrunner.Protocol, EffectID: e.ID, Spec: e.Spec, OwnerID: record.ExecID, Generation: record.Generation, ConfirmedAt: now}
			if err := x.saveJob(record); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			delete(f.execs, record.ExecID)
			f.mu.Unlock()
			destination := filepath.Join(x.cfg.State, "artifacts", e.ID)
			if err := os.MkdirAll(destination, 0755); err != nil {
				t.Fatal(err)
			}
			done := make(chan destinationResult, 1)
			go func() {
				job, err := x.Cancel(t.Context(), e)
				done <- destinationResult{job, err}
			}()
			waitForDestinationTempOrDone(t, destination, done)
			moved := destination + ".moved"
			if err := os.Rename(destination, moved); err != nil {
				t.Fatal(err)
			}
			first := <-done
			if first.err == nil || !errors.Is(first.err, os.ErrNotExist) {
				t.Fatalf("termination destination ENOENT = %+v, %v; want a retryable error", first.job, first.err)
			}
			if again, found, err := x.readJob(e); err != nil || !found || again.Terminal != nil {
				t.Fatalf("termination destination ENOENT wrote a receipt: %+v, %v, %v", again, found, err)
			}
			if _, err := os.Stat(filepath.Join(f.dir, "spool", e.ID, "output")); err != nil {
				t.Fatalf("termination destination ENOENT removed the capture: %v", err)
			}
			if err := os.Rename(moved, destination); err != nil {
				t.Fatal(err)
			}
			rebuilt, err := NewAttached(x.cfg)
			if err != nil {
				t.Fatal(err)
			}
			job, err := rebuilt.Cancel(t.Context(), e)
			if err != nil || job.Status != harness.JobCancelled || job.Cessation == nil {
				t.Fatalf("retry after restoring destination = %+v, %v", job, err)
			}
		})
	}
}

func TestAttachedDestinationObstructionKeepsCaptureForRetry(t *testing.T) {
	for _, mode := range []string{"file", "read-only"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "read-only" && os.Geteuid() == 0 {
				t.Skip("root can create files in a read-only directory")
			}
			f := newAttachedFake(t)
			x := f.executor(t)
			e := effectFor(x, "attached-destination-obstructed", "true")
			if _, err := x.Drive(t.Context(), e); err != nil {
				t.Fatal(err)
			}
			f.complete(t, x, e, 0, "kept")
			record, found, err := x.readJob(e)
			if err != nil || !found {
				t.Fatalf("read attached job: %+v, %v, %v", record, found, err)
			}
			now := time.Now().UTC()
			record.Fenced, record.CancelAt = true, now
			record.Termination = &terminationProof{Protocol: jobrunner.Protocol, EffectID: e.ID, Spec: e.Spec, OwnerID: record.ExecID, Generation: record.Generation, ConfirmedAt: now}
			if err := x.saveJob(record); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			delete(f.execs, record.ExecID)
			f.mu.Unlock()
			destination := filepath.Join(x.cfg.State, "artifacts", e.ID)
			if mode == "file" {
				if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(destination, []byte("obstruction"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.MkdirAll(destination, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(destination, 0555); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(destination, 0755) })
			}
			if _, err := x.Cancel(t.Context(), e); err == nil {
				t.Fatal("obstructed destination did not fail")
			}
			if again, found, err := x.readJob(e); err != nil || !found || again.Terminal != nil {
				t.Fatalf("obstructed destination wrote a receipt: %+v, %v, %v", again, found, err)
			}
			if _, err := os.Stat(filepath.Join(f.dir, "spool", e.ID, "output")); err != nil {
				t.Fatalf("obstructed destination removed the capture: %v", err)
			}
			if mode == "file" {
				if err := os.Remove(destination); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Chmod(destination, 0755); err != nil {
				t.Fatal(err)
			}
			rebuilt, err := NewAttached(x.cfg)
			if err != nil {
				t.Fatal(err)
			}
			job, err := rebuilt.Cancel(t.Context(), e)
			if err != nil || job.Status != harness.JobCancelled || !job.Output.Available || job.Output.Bytes != int64(len("kept")) {
				t.Fatalf("retry after obstruction = %+v, %v", job, err)
			}
		})
	}
}
