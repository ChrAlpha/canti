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

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
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
	}))
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

const admitCrashEnv = "CANTI_SQLITE_ADMIT_CRASH_DB"

// admitCrashChild admits run after run, stopping each before the next, and
// prints each admitted input position; it then kills itself mid-run.
func admitCrashChild(path string) {
	ctx := context.Background()
	fail := func(err error) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	s, err := Open(ctx, path)
	if err != nil {
		fail(err)
	}
	runner, err := harness.NewRunner(s)
	if err != nil {
		fail(err)
	}
	for i := 0; ; i++ {
		id := fmt.Sprintf("run-admit-%06d", i)
		initial, err := core.NewState(id, crashLimits)
		if err != nil {
			fail(err)
		}
		admission, err := s.Admit(ctx, harness.InputSubmission{InputID: "input-" + id, Authority: harness.AuthorityPrincipal,
			Text: "task " + id, Run: testRun(id), Initial: initial, Now: time.Now()})
		if err != nil {
			fail(err)
		}
		snap, err := s.Latest(ctx, id)
		if err != nil {
			fail(err)
		}
		if _, err := runner.Commit(ctx, snap, core.StopRequested{Reason: core.StopCancelled}, harness.Facts{}); err != nil {
			fail(err)
		}
		fmt.Printf("ack %d\n", admission.Input.Pos)
		if i == 300 {
			syscall.Kill(os.Getpid(), syscall.SIGKILL)
			select {}
		}
	}
}

// TestCrashAdmissionCommitsInputAndRunTogether requires that a process killed
// during admission leaves every acknowledged input and run, and no partial
// pair: each run's input position names its principal opening input, every
// input's run exists, and the agent-log counter is the latest position.
func TestCrashAdmissionCommitsInputAndRunTogether(t *testing.T) {
	if path := os.Getenv(admitCrashEnv); path != "" {
		admitCrashChild(path)
	}
	path := tempPath(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashAdmissionCommitsInputAndRunTogether$", "-test.count=1")
	cmd.Env = append(os.Environ(), admitCrashEnv+"="+path)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("child error = %v, want it killed; stderr:\n%s", err, stderr.String())
	}
	var acked int64
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if _, ok := strings.CutPrefix(line, "ack "); ok {
			acked++
		}
	}
	if acked < 200 {
		t.Fatalf("child acknowledged %d admissions, want at least 200", acked)
	}
	s := open(t, path)
	defer s.Close()
	var runs, inputs int
	if err := s.db.QueryRow(`SELECT count(*) FROM runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM inputs`).Scan(&inputs); err != nil {
		t.Fatal(err)
	}
	if int64(runs) < acked || int64(runs) > acked+1 || inputs != runs {
		t.Fatalf("%d runs and %d inputs after %d admissions", runs, inputs, acked)
	}
	rows, err := s.db.Query(`SELECT id, input_pos, fresh FROM runs ORDER BY n`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var pos int64
		var fresh bool
		if err := rows.Scan(&id, &pos, &fresh); err != nil {
			t.Fatal(err)
		}
		if pos < 1 {
			t.Fatalf("run %q has no input position", id)
		}
		var authority, disposition, runID, text string
		if err := s.db.QueryRow(`SELECT authority, disposition, COALESCE(run_id, ''), text FROM inputs WHERE pos = ?`, pos).
			Scan(&authority, &disposition, &runID, &text); err != nil {
			t.Fatalf("run %q input %d: %v", id, pos, err)
		}
		if authority != string(harness.AuthorityPrincipal) || disposition != string(harness.DispositionOpened) || runID != id || text != "task "+id {
			t.Fatalf("run %q input %d = %s %s %s %q", id, pos, authority, disposition, runID, text)
		}
		if snap, err := s.Latest(context.Background(), id); err != nil {
			t.Fatalf("run %q: %v", id, err)
		} else if snap.State.RunID != id {
			t.Fatalf("run %q reads as %q", id, snap.State.RunID)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	var counter, maxPos int64
	if err := s.db.QueryRow(`SELECT pos FROM agent_log WHERE n = 1`).Scan(&counter); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT max(p) FROM (SELECT pos AS p FROM transitions UNION ALL SELECT pos FROM inputs)`).Scan(&maxPos); err != nil {
		t.Fatal(err)
	}
	if counter != maxPos {
		t.Fatalf("agent log counter %d, latest position %d", counter, maxPos)
	}
}
