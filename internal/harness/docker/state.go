package docker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
)

const (
	latchStart  = "start"
	latchCancel = "cancel"
)

type container struct {
	ID     string
	Config struct {
		Labels map[string]string
	}
	State struct {
		Status     string
		ExitCode   int
		StartedAt  time.Time
		FinishedAt time.Time
	}
}

func derive(latch string, c container, marked bool) harness.Job {
	job := harness.Job{ID: c.ID, Status: harness.JobUnknown, StartedAt: c.State.StartedAt, FinishedAt: c.State.FinishedAt}
	switch c.State.Status {
	case "created":
		switch latch {
		case "":
			job.Status = harness.JobPending
		case latchCancel:
			job.Status = harness.JobCancelled
		}
	case "running", "paused", "restarting":
		if latch == latchStart {
			job.Status = harness.JobRunning
		}
	case "exited", "dead":
		if latch != latchStart {
			break
		}
		code := c.State.ExitCode
		job.ExitCode = &code
		switch {
		case marked:
			job.Status = harness.JobCancelled
		case code == 0:
			job.Status = harness.JobSucceeded
		default:
			job.Status = harness.JobFailed
		}
	}
	return job
}

func terminal(status harness.JobStatus) bool {
	return status == harness.JobSucceeded || status == harness.JobFailed || status == harness.JobCancelled
}

func checkID(id string) error {
	ok := strings.Trim(id, ".") != ""
	for _, r := range id {
		ok = ok && (r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.')
	}
	if !ok {
		return fmt.Errorf("docker: invalid effect ID %q", id)
	}
	return nil
}

func flock(ctx context.Context, path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { f.Close() }, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EINTR) {
			f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func readLatch(dir string) (string, error) {
	b, err := os.ReadFile(filepath.Join(dir, "latch"))
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if s := string(b); err == nil && s != latchStart && s != latchCancel {
		err = fmt.Errorf("docker: corrupt latch %q in %s", s, dir)
	}
	return string(b), err
}

func setLatch(dir, value string) error {
	if err := publish(filepath.Join(dir, "latch"), []byte(value), true); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dir))
}

func mark(dir string) error {
	err := publish(filepath.Join(dir, "cancel"), []byte(time.Now().UTC().Format(time.RFC3339Nano)), true)
	if errors.Is(err, fs.ErrExist) {
		return nil
	}
	return err
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func publish(path string, data []byte, exclusive bool) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	return install(f, path, exclusive, err)
}

func install(f *os.File, path string, exclusive bool, err error) error {
	defer os.Remove(f.Name())
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if exclusive {
		err = os.Link(f.Name(), path)
	} else {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}
