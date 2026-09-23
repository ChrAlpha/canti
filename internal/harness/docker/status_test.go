package docker

import (
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
)

func TestDeriveStatus(t *testing.T) {
	for _, tt := range []struct {
		name   string
		latch  string
		state  string
		exit   int
		marked bool
		want   harness.JobStatus
		code   bool
	}{
		{"bound", "", "created", 0, false, harness.JobPending, false},
		{"fenced before start", latchCancel, "created", 0, false, harness.JobCancelled, false},
		{"launch uncertain", latchStart, "created", 0, false, harness.JobUnknown, false},
		{"launch uncertain while cancelling", latchStart, "created", 0, true, harness.JobUnknown, false},
		{"running", latchStart, "running", 0, false, harness.JobRunning, false},
		{"running while cancelling", latchStart, "running", 0, true, harness.JobRunning, false},
		{"paused", latchStart, "paused", 0, false, harness.JobRunning, false},
		{"restarting", latchStart, "restarting", 0, false, harness.JobRunning, false},
		{"succeeded", latchStart, "exited", 0, false, harness.JobSucceeded, true},
		{"failed", latchStart, "exited", 3, false, harness.JobFailed, true},
		{"dead", latchStart, "dead", 137, false, harness.JobFailed, true},
		{"cancelled", latchStart, "exited", 143, true, harness.JobCancelled, true},
		{"cancelled after a clean exit", latchStart, "exited", 0, true, harness.JobCancelled, true},
		{"running without latch", "", "running", 0, false, harness.JobUnknown, false},
		{"running after fence", latchCancel, "running", 0, false, harness.JobUnknown, false},
		{"exited without latch", "", "exited", 0, false, harness.JobUnknown, false},
		{"exited after fence", latchCancel, "exited", 0, true, harness.JobUnknown, false},
		{"removing", latchStart, "removing", 0, false, harness.JobUnknown, false},
		{"unrecognized", "", "", 0, false, harness.JobUnknown, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var c container
			c.ID = "c1"
			c.State.Status = tt.state
			c.State.ExitCode = tt.exit
			c.State.StartedAt = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			c.State.FinishedAt = c.State.StartedAt.Add(time.Second)
			job := derive(tt.latch, c, tt.marked)
			if job.ID != "c1" || job.Status != tt.want {
				t.Fatalf("derive() = %q %q, want c1 %q", job.ID, job.Status, tt.want)
			}
			if !job.StartedAt.Equal(c.State.StartedAt) || !job.FinishedAt.Equal(c.State.FinishedAt) {
				t.Fatalf("times = %v %v, want docker's", job.StartedAt, job.FinishedAt)
			}
			if tt.code != (job.ExitCode != nil) || tt.code && *job.ExitCode != tt.exit {
				t.Fatalf("exit code = %v, want set %v to %d", job.ExitCode, tt.code, tt.exit)
			}
		})
	}
}

func TestDeriveFailedStart(t *testing.T) {
	for _, tt := range []struct {
		name    string
		latch   string
		failure string
		started bool
		marked  bool
		want    harness.JobStatus
	}{
		{"rejected start", latchStart, "unable to find user nosuchuser", false, false, harness.JobFailedToStart},
		{"rejected start while cancelling", latchStart, "unable to find user nosuchuser", false, true, harness.JobFailedToStart},
		{"launch uncertain", latchStart, "", false, false, harness.JobUnknown},
		{"error after a start", latchStart, "boom", true, false, harness.JobUnknown},
		{"error without latch", "", "boom", false, false, harness.JobPending},
		{"error after fence", latchCancel, "boom", false, false, harness.JobCancelled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var c container
			c.State.Status, c.State.ExitCode, c.State.Error = "created", 128, tt.failure
			if tt.started {
				c.State.StartedAt = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			}
			if job := derive(tt.latch, c, tt.marked); job.Status != tt.want || job.ExitCode != nil {
				t.Fatalf("derive() = %q with exit code %v, want %q without one", job.Status, job.ExitCode, tt.want)
			}
		})
	}
}

func TestDeriveKeepsZeroTimes(t *testing.T) {
	var c container
	c.State.Status = "created"
	job := derive("", c, false)
	if !job.StartedAt.IsZero() || !job.FinishedAt.IsZero() {
		t.Fatalf("times = %v %v, want zero", job.StartedAt, job.FinishedAt)
	}
}
