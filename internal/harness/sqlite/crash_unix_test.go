//go:build unix

package sqlite

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

const (
	crashEnv = "CANTI_SQLITE_CRASH_DB"
	crashRun = "run-crash"
)

var crashLimits = core.Limits{MaxSteps: 1000, MaxAttempts: 1}

// crashChild commits continuously, printing each acknowledged sequence, and
// kills its own process mid-run without closing the store.
func crashChild(path string) {
	ctx := context.Background()
	fail := func(err error) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	s, err := Open(ctx, path)
	if err != nil {
		fail(err)
	}
	initial, err := core.NewState(crashRun, crashLimits)
	if err != nil {
		fail(err)
	}
	if err := s.CreateRun(ctx, testRun(crashRun), initial); err != nil {
		fail(err)
	}
	var acked atomic.Int64
	runner, err := harness.NewRunner(storeFunc(func(ctx context.Context, tr harness.Transition) error {
		if err := s.Commit(ctx, tr); err != nil {
			return err
		}
		fmt.Printf("ack %d\n", tr.Seq)
		acked.Store(tr.Seq)
		return nil
	}), executor{})
	if err != nil {
		fail(err)
	}
	go func() {
		snap := harness.Snapshot{State: initial}
		for {
			event, facts := shortcut(snap.State)
			next, err := runner.Commit(ctx, snap, event, facts)
			if err != nil {
				fail(err)
			}
			snap = next
		}
	}()
	for acked.Load() < 200 {
		time.Sleep(time.Millisecond)
	}
	syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}

func TestCrashKeepsAcknowledgedCommits(t *testing.T) {
	if path := os.Getenv(crashEnv); path != "" {
		crashChild(path)
	}
	path := tempPath(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashKeepsAcknowledgedCommits$", "-test.count=1")
	cmd.Env = append(os.Environ(), crashEnv+"="+path)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("child error = %v, want it killed; stderr:\n%s", err, stderr.String())
	}
	if status, ok := exit.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child exited with %v, want SIGKILL; stderr:\n%s", exit, stderr.String())
	}
	var acked int64
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		if seq, ok := strings.CutPrefix(scanner.Text(), "ack "); ok {
			n, err := strconv.ParseInt(seq, 10, 64)
			if err != nil || n != acked+1 {
				t.Fatalf("acknowledgement %q follows seq %d", scanner.Text(), acked)
			}
			acked = n
		}
	}
	if acked < 200 {
		t.Fatalf("child acknowledged %d commits, want at least 200", acked)
	}

	s := open(t, path)
	stored, err := s.Transitions(context.Background(), crashRun, 0, int(acked)+10)
	if err != nil {
		t.Fatal(err)
	}
	if n := int64(len(stored)); n != acked && n != acked+1 {
		t.Fatalf("recovered %d transitions after %d acknowledgements, want every acknowledged one and at most the one in flight", n, acked)
	}
	final := replay(t, fresh(t, crashRun, crashLimits), stored)
	if got := latestOf(t, s, crashRun); got.Seq != int64(len(stored)) || got.State != final {
		t.Fatalf("Latest() = %#v, want seq %d with the replayed state", got, len(stored))
	}
	if got, want := totalsOf(t, s, crashRun), sums(stored); got != want {
		t.Fatalf("Totals() = %#v, want the sums of the recovered transitions %#v", got, want)
	}
	t.Logf("recovered %d transitions after %d acknowledgements", len(stored), acked)
}
