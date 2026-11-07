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
		Error      string
		OOMKilled  bool
		StartedAt  time.Time
		FinishedAt time.Time
	}
}

// A container labelled exitLabel runs wrapper, which writes the exit status
// of the job's command to exitFile just before it exits. Only that record,
// read as recorded, makes such a job's exit final.
const (
	exitLabel  = "canti.exit"
	exitFile   = "/tmp/.canti-exit"
	unrecorded = "no exit record at " + exitFile + ": the process was killed before it could record, or the record could not be written"
)

func derive(latch string, c container, marked bool, recorded *int) harness.Job {
	job := harness.Job{ID: c.ID, Status: harness.JobUnknown, StartedAt: c.State.StartedAt, FinishedAt: c.State.FinishedAt}
	switch s := c.State.Status; {
	case s == "created":
		switch {
		case latch == "":
			job.Status = harness.JobPending
		case latch == latchCancel:
			job.Status = harness.JobCancelled
		case rejected(c):
			job.Status = harness.JobFailedToStart
			job.Output.Detail = "job never started: " + c.State.Error
		}
	case running(c):
		if latch == latchStart {
			job.Status = harness.JobRunning
		}
	case s == "exited" || s == "dead":
		if latch != latchStart {
			break
		}
		code := c.State.ExitCode
		if !marked {
			switch {
			case recorded == nil:
				job.Output.Detail = unrecorded
				if c.State.OOMKilled {
					job.Output.Detail += "; Docker observed an out-of-memory kill in the container"
				}
				return job
			case *recorded != code:
				job.Output.Detail = fmt.Sprintf("recorded exit status %d differs from the container's exit code %d", *recorded, code)
				return job
			}
		}
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
	if job.Status == harness.JobUnknown && job.Output.Detail == "" {
		job.Output.Detail = fmt.Sprintf("container state %q with launch latch %q does not establish an execution outcome", c.State.Status, latch)
	}
	return job
}

// rejected reports a container whose start the daemon refused before its
// process began.
func rejected(c container) bool {
	return c.State.Status == "created" && c.State.Error != "" && c.State.StartedAt.IsZero()
}

func running(c container) bool {
	s := c.State.Status
	return s == "running" || s == "paused" || s == "restarting"
}

func terminal(status harness.JobStatus) bool {
	switch status {
	case harness.JobSucceeded, harness.JobFailed, harness.JobCancelled, harness.JobFailedToStart:
		return true
	}
	return false
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
