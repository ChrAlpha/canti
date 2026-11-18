package jobrunner

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func waitUntil(t *testing.T, limit time.Duration, why string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !ok() {
		if !time.Now().Before(deadline) {
			t.Fatalf("timed out waiting for %s", why)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func readPid(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	return pid
}

func processRunning(pid int) bool {
	if pid <= 0 {
		return false
	}
	info, err := readProcessInfoAt("/proc", pid)
	return err == nil && !info.zombie()
}

func startGroupProcess(t *testing.T, name string, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd
}

type runResult struct {
	rec Record
	err error
}

func startRun(t *testing.T, c Config) chan runResult {
	t.Helper()
	result := make(chan runResult, 1)
	go func() {
		rec, err := Run(c)
		result <- runResult{rec, err}
	}()
	return result
}

func TestRunTerminationStopsTheGroupAndKeepsANewSessionService(t *testing.T) {
	c := config(t, `setsid sh -c 'echo $$ > service.pid; exec sleep 300' </dev/null >/dev/null 2>&1 &
echo $$ > shell.pid
sleep 300 & echo $! > child.pid
sleep 300`)
	result := startRun(t, c)
	shellPath := filepath.Join(c.Workdir, "shell.pid")
	childPath := filepath.Join(c.Workdir, "child.pid")
	servicePath := filepath.Join(c.Workdir, "service.pid")
	waitUntil(t, 5*time.Second, "job processes", func() bool {
		return readPid(t, shellPath) > 0 && readPid(t, childPath) > 0 && readPid(t, servicePath) > 0
	})
	shell, child, service := readPid(t, shellPath), readPid(t, childPath), readPid(t, servicePath)
	info, err := readProcessInfoAt("/proc", shell)
	if err != nil || info.pgid != shell {
		t.Fatalf("shell PGID = %d, pid = %d: %v", info.pgid, shell, err)
	}
	t.Cleanup(func() { _ = syscall.Kill(service, syscall.SIGKILL) })
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	got := <-result
	if got.err != nil || !got.rec.Terminated || got.rec.ExitCode == nil || (*got.rec.ExitCode != 143 && *got.rec.ExitCode != 137) {
		t.Fatalf("run after termination = %+v: %v", got.rec, got.err)
	}
	if processRunning(shell) || processRunning(child) {
		t.Fatal("job process group survived termination")
	}
	if !processRunning(service) {
		t.Fatal("service in a new session was terminated with the job")
	}
}

func TestRunTerminationEscalatesToKillAfterTheGrace(t *testing.T) {
	c := config(t, "trap '' TERM; echo $$ > shell.pid; while :; do sleep 1; done")
	c.Grace = 300 * time.Millisecond
	result := startRun(t, c)
	waitUntil(t, 5*time.Second, "shell identity", func() bool {
		return readPid(t, filepath.Join(c.Workdir, "shell.pid")) > 0
	})
	start := time.Now()
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	got := <-result
	elapsed := time.Since(start)
	if got.err != nil || !got.rec.Terminated || got.rec.ExitCode == nil || *got.rec.ExitCode != 137 {
		t.Fatalf("run after KILL = %+v: %v", got.rec, got.err)
	}
	if elapsed < c.Grace {
		t.Fatalf("KILL was sent after %v, before the %v grace", elapsed, c.Grace)
	}
}

func TestTerminateChildKillsForksDuringTheGrace(t *testing.T) {
	cmd := startGroupProcess(t, "/bin/sh", "-c", "trap '' TERM; while :; do sleep 30 & sleep 0.02; done")
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	})
	waitUntil(t, 5*time.Second, "child group", func() bool {
		live, err := groupHasLiveTasks(cmd.Process.Pid)
		return err == nil && live
	})
	if err := terminateChild(cmd.Process.Pid, cmd.Process.Pid, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	live, err := groupHasLiveTasks(cmd.Process.Pid)
	if err != nil || live {
		t.Fatalf("group after KILL live=%t: %v", live, err)
	}
}

func TestTerminateChildDoesNotSignalAnotherGroup(t *testing.T) {
	job := startGroupProcess(t, "sleep", "300")
	service := startGroupProcess(t, "sleep", "300")
	t.Cleanup(func() {
		_ = syscall.Kill(-job.Process.Pid, syscall.SIGKILL)
		_ = syscall.Kill(-service.Process.Pid, syscall.SIGKILL)
		_ = job.Wait()
		_ = service.Wait()
	})
	if err := terminateChild(job.Process.Pid, job.Process.Pid, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if !processRunning(service.Process.Pid) {
		t.Fatal("termination signalled another process group")
	}
}

func TestGroupScanReadsEveryThread(t *testing.T) {
	root := t.TempDir()
	writeProcStat(t, root, 100, 900, 100, "Z")
	writeProcTask(t, root, 100, 100, "Z")
	writeProcTask(t, root, 100, 101, "S")
	live, err := groupHasLiveTasksAt(root, 100)
	if err != nil || !live {
		t.Fatalf("live worker thread behind zombie leader = %t: %v", live, err)
	}
	writeProcTask(t, root, 100, 101, "Z")
	live, err = groupHasLiveTasksAt(root, 100)
	if err != nil || live {
		t.Fatalf("all-zombie group = %t: %v", live, err)
	}
	if err := os.MkdirAll(filepath.Join(root, "200", "task", "200"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "200", "stat"), []byte("200 (helper) S 0 x 0 0 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "200", "task", "200", "stat"), []byte("200 (thread) S 200 x 0 0 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	live, err = groupHasLiveTasksAt(root, 100)
	if live || !errors.Is(err, errProcessStat) {
		t.Fatalf("unreadable group field = %t: %v, want uncertain", live, err)
	}
	if err := os.WriteFile(filepath.Join(root, "200", "stat"), []byte("200 (helper) Z 0 200 0 0 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "200", "task", "200", "stat"), []byte("200 (thread) Z 200 200 0 0 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	live, err = groupHasLiveTasksAt(root, 100)
	if err != nil || live {
		t.Fatalf("resolved group field = %t: %v", live, err)
	}
}

func TestTerminateChildRejectsAnInvalidTarget(t *testing.T) {
	cmd := startGroupProcess(t, "sleep", "300")
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	})
	if err := terminateChild(cmd.Process.Pid, 1, 100*time.Millisecond); err == nil {
		t.Fatal("accepted the container init process group")
	}
	if err := terminateChild(cmd.Process.Pid, cmd.Process.Pid+1, 100*time.Millisecond); err == nil {
		t.Fatal("accepted a target whose PGID differs from its PID")
	}
	if !processRunning(cmd.Process.Pid) {
		t.Fatal("an invalid target was signalled")
	}
}

func TestWaitNoReapKeepsTheGroupLeaderReserved(t *testing.T) {
	cmd := startGroupProcess(t, "sleep", "0.1")
	defer func() { _ = cmd.Wait() }()
	if err := waitNoReap(cmd.Process.Pid); err != nil {
		t.Fatal(err)
	}
	info, err := readProcessInfoAt("/proc", cmd.Process.Pid)
	if err != nil || !info.zombie() {
		t.Fatalf("leader was reaped before the group proof: %+v: %v", info, err)
	}
}

func TestProcessGoneTreatsKernelDisappearanceAsNotLive(t *testing.T) {
	for _, err := range []error{os.ErrNotExist, syscall.ENOENT, syscall.ESRCH} {
		if !processGone(err) {
			t.Fatalf("%v was treated as a live process", err)
		}
	}
	if processGone(syscall.EACCES) || processGone(errProcessStat) {
		t.Fatal("an uncertain or permission failure was treated as a disappeared process")
	}
}

func TestTerminateScannedRetriesUncertaintyButNotAsProof(t *testing.T) {
	var scans int
	var signals []unix.Signal
	scan := func(int) (bool, error) {
		scans++
		switch scans {
		case 1:
			return false, errProcessStat
		case 2:
			return false, os.ErrNotExist
		default:
			return false, nil
		}
	}
	signal := func(sig unix.Signal) error { signals = append(signals, sig); return nil }
	if err := terminateScanned(10, 10, 100*time.Millisecond, scan, signal); err != nil {
		t.Fatalf("uncertainty did not resolve: %v", err)
	}
	if scans < 3 || !slices.Contains(signals, unix.SIGKILL) {
		t.Fatalf("scans=%d signals=%v", scans, signals)
	}
	alwaysUncertain := func(int) (bool, error) { return false, errProcessStat }
	if err := terminateScanned(10, 10, 50*time.Millisecond, alwaysUncertain, signal); err == nil || !errors.Is(err, errProcessStat) {
		t.Fatalf("persistent uncertainty = %v, want unproven", err)
	}
}

func TestTerminateChildTreatsZombiesAsGone(t *testing.T) {
	cmd := startGroupProcess(t, "sleep", "0.1")
	defer func() { _ = cmd.Wait() }()
	waitUntil(t, 5*time.Second, "zombie child", func() bool {
		info, err := readProcessInfoAt("/proc", cmd.Process.Pid)
		return err == nil && info.zombie()
	})
	if err := terminateChild(cmd.Process.Pid, cmd.Process.Pid, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

func TestTerminateControlFindsOnlyItsGeneration(t *testing.T) {
	root := t.TempDir()
	self := "/canti/bin/helper"
	args := []string{self, "-effect", "effect", "-spec", strings.Repeat("a", 64), "-generation", "3", "-epoch", "epoch", "-directory", "/spool/effect"}
	writeProcStat(t, root, 10, 0, 10, "S")
	writeProcCmdline(t, root, 10, args)
	writeProcStat(t, root, 11, 0, 11, "S")
	writeProcCmdline(t, root, 11, []string{self, "-effect", "effect", "-spec", strings.Repeat("a", 64), "-generation", "4", "-epoch", "epoch"})
	pid, err := findHelperAt(root, self, controlQuery{EffectID: "effect", Spec: strings.Repeat("a", 64), Generation: 3, Epoch: "epoch"})
	if err != nil || pid != 10 {
		t.Fatalf("find helper = %d: %v", pid, err)
	}
	if _, err := findHelperAt(root, self, controlQuery{EffectID: "effect", Spec: strings.Repeat("a", 64), Generation: 3, Epoch: "epoch", ExpectedPID: 11}); err == nil {
		t.Fatal("accepted the wrong expected helper PID")
	}
	if _, err := findHelperAt(root, self, controlQuery{EffectID: "effect", Spec: strings.Repeat("a", 64), Generation: 5, Epoch: "epoch"}); err == nil {
		t.Fatal("accepted another generation")
	}
}

func TestTerminateControlRejectsDuplicateHelpers(t *testing.T) {
	root := t.TempDir()
	self := "/canti/bin/helper"
	args := []string{self, "-effect", "effect", "-spec", strings.Repeat("a", 64), "-generation", "3", "-epoch", "epoch"}
	for _, pid := range []int{10, 11} {
		writeProcStat(t, root, pid, 0, pid, "S")
		writeProcCmdline(t, root, pid, args)
	}
	if _, err := findHelperAt(root, self, controlQuery{EffectID: "effect", Spec: strings.Repeat("a", 64), Generation: 3, Epoch: "epoch"}); err == nil {
		t.Fatal("accepted duplicate helpers")
	}
}

func TestExitStatusRemapsANatural255(t *testing.T) {
	c := config(t, "exit 255")
	rec, err := Run(c)
	if err != nil || rec.Terminated || rec.ExitCode == nil || *rec.ExitCode != 255 || ExitStatus(rec, true) != RemappedCommandExit {
		t.Fatalf("natural exit 255 = %+v: %v, status=%d", rec, err, ExitStatus(rec, true))
	}
	c = config(t, "exit 254")
	rec, err = Run(c)
	if err != nil || rec.Terminated || rec.ExitCode == nil || *rec.ExitCode != 254 || ExitStatus(rec, true) != RemappedCommandExit254 {
		t.Fatalf("natural exit 254 = %+v: %v, status=%d", rec, err, ExitStatus(rec, true))
	}
}

func TestExitStatusLeavesEphemeralNaturalStatusesAlone(t *testing.T) {
	for _, code := range []int{253, 254, 255} {
		rec := Record{ExitCode: &code}
		if got := ExitStatus(rec, false); got != code {
			t.Errorf("ephemeral natural exit %d = %d", code, got)
		}
	}
}

func TestHardenProcessRefusesSameUserPtrace(t *testing.T) {
	if os.Getenv("CANTI_PTRACE_CHILD") != "" {
		_ = unix.Prctl(unix.PR_SET_DUMPABLE, 1, 0, 0, 0)
		if err := HardenProcess(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		// Ask Yama to permit this parent; PR_SET_DUMPABLE 0 must still refuse
		// the attach. The job child inherits the same non-dumpable state.
		_ = unix.Prctl(unix.PR_SET_PTRACER, uintptr(os.Getppid()), 0, 0, 0)
		child := exec.Command("sleep", "30")
		if err := child.Start(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Printf("ready %d %d\n", os.Getpid(), child.Process.Pid)
		time.Sleep(30 * time.Second)
		_ = child.Process.Kill()
		_ = child.Wait()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestHardenProcessRefusesSameUserPtrace$")
	cmd.Env = append(os.Environ(), "CANTI_PTRACE_CHILD=1")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		t.Fatalf("helper readiness: %q: %v", line, err)
	}
	var helperPID, childPID int
	if _, err := fmt.Sscan(line, new(string), &helperPID, &childPID); err != nil || helperPID <= 0 || childPID <= 0 {
		t.Fatalf("helper readiness line = %q: %v", line, err)
	}
	t.Cleanup(func() { _ = syscall.Kill(childPID, syscall.SIGKILL) })
	err = unix.PtraceAttach(helperPID)
	if err == nil {
		_ = unix.PtraceDetach(helperPID)
		t.Fatalf("same-user ptrace attach to the hardened helper %d was allowed", helperPID)
	}
	if !errors.Is(err, syscall.EPERM) {
		t.Fatalf("ptrace attach to %d error = %v, want EPERM", helperPID, err)
	}
	_ = childPID
}

func writeProcStat(t *testing.T, root string, pid, ppid, pgid int, state string) {
	t.Helper()
	dir := filepath.Join(root, strconv.Itoa(pid))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data := fmt.Sprintf("%d (helper) %s %d %d 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n", pid, state, ppid, pgid)
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeProcTask(t *testing.T, root string, pid, tid int, state string) {
	t.Helper()
	dir := filepath.Join(root, strconv.Itoa(pid), "task", strconv.Itoa(tid))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data := fmt.Sprintf("%d (thread) %s %d %d 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n", pid, state, pid, pid)
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeProcCmdline(t *testing.T, root string, pid int, args []string) {
	t.Helper()
	dir := filepath.Join(root, strconv.Itoa(pid))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	var data []byte
	for _, arg := range args {
		data = append(data, arg...)
		data = append(data, 0)
	}
	if err := os.WriteFile(filepath.Join(dir, "cmdline"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestParseProcessInfoAcceptsOnlyOneDocumentedStateByte(t *testing.T) {
	for _, state := range []string{"R", "S", "D", "T", "t", "Z", "X", "x", "K", "W", "P", "I"} {
		info, err := parseProcessInfo([]byte("123 (sh) "+state+" 1 2 0"), 123)
		if err != nil || info.state != state[0] {
			t.Errorf("state %q = %+v, %v", state, info, err)
		}
	}
	for _, state := range []string{"ZZ", "Q", "", "\xff", "\xc3\xa9"} {
		_, err := parseProcessInfo([]byte("123 (sh) "+state+" 1 2 0"), 123)
		if !errors.Is(err, errProcessStat) {
			t.Errorf("state %q error = %v, want errProcessStat", state, err)
		}
	}
}
