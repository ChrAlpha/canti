package jobrunner

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	// TerminationStatus is the helper exit status that only the proven
	// termination path produces. The host uses it as a durable proof, separate
	// from the command's own exit code.
	TerminationStatus = 255
	// RemappedCommandExit keeps a command's natural exit 255 distinct from the
	// termination proof. Natural 254 is remapped once more so daemon status 254
	// means exactly natural 255; the completion carries the command's real exit.
	RemappedCommandExit    = 254
	RemappedCommandExit254 = 253
)

// TerminateControl is the canti-job -terminate mode. It finds the live capture
// helper of exactly one effect, spec, generation and epoch, pins it with pidfd,
// verifies the identity again through the pinned process, signals it, and only
// exits successfully after the helper has exited and its child process group
// has no live task. It never reads the job-writable spool.
func TerminateControl(effect, spec string, generation uint64, epoch string, expectedPID int, grace time.Duration) error {
	if effect == "" || len(effect) > 128 || generation == 0 || epoch == "" || grace <= 0 {
		return errors.New("job runner: invalid termination control request")
	}
	if !canonicalSpec(spec) {
		return errors.New("job runner: invalid termination specification")
	}
	query := controlQuery{EffectID: effect, Spec: spec, Generation: generation, Epoch: epoch, ExpectedPID: expectedPID}
	helperPID, err := findHelperAt("/proc", os.Args[0], query)
	if err != nil {
		return err
	}
	helperFD, err := unix.PidfdOpen(helperPID, 0)
	if err != nil {
		return fmt.Errorf("job runner: pin helper: %w", err)
	}
	defer unix.Close(helperFD)
	if err = verifyPinnedHelper("/proc", helperPID, query); err != nil {
		return err
	}
	childPID, err := findChildAt("/proc", helperPID)
	if err != nil {
		return err
	}
	childFD, err := unix.PidfdOpen(childPID, 0)
	if err != nil {
		return fmt.Errorf("job runner: pin job shell: %w", err)
	}
	defer unix.Close(childFD)
	if err = verifyChild("/proc", helperPID, childPID); err != nil {
		return err
	}
	if err = unix.PidfdSendSignal(helperFD, unix.SIGUSR1, nil, 0); err != nil {
		return fmt.Errorf("job runner: signal helper: %w", err)
	}
	deadline := time.Now().Add(2*grace + 5*time.Second)
	for {
		alive, err := pidfdAlive(helperFD)
		if err != nil {
			return err
		}
		live, err := groupHasLiveTasks(childPID)
		if err != nil {
			return err
		}
		switch {
		case !alive && live:
			return errors.New("job runner: helper exited before its process group did")
		case !alive && !live:
			return nil
		case !time.Now().Before(deadline):
			return errors.New("job runner: termination control timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// HardenProcess refuses same-user ptrace attachment to the helper or control
// exec. It is called after the shell fork in Run so the job keeps its normal
// debugger access; the control exec calls it before looking for a helper.
func HardenProcess() error {
	return unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0)
}

type controlQuery struct {
	EffectID, Spec, Epoch string
	Generation            uint64
	ExpectedPID           int
}

func findHelperAt(root, argvSelf string, q controlQuery) (int, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, err
	}
	var found []int
	for _, entry := range entries {
		if !entry.IsDir() || !decimal(entry.Name()) {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || q.ExpectedPID > 0 && pid != q.ExpectedPID {
			continue
		}
		match, err := helperCandidateAt(root, pid, argvSelf, q)
		if err != nil {
			if processGone(err) {
				continue
			}
			return 0, err
		}
		if match {
			found = append(found, pid)
		}
	}
	switch len(found) {
	case 0:
		return 0, errors.New("job runner: no live helper of this effect and generation")
	case 1:
		return found[0], nil
	default:
		return 0, errors.New("job runner: more than one live helper of this effect and generation")
	}
}

func helperCandidateAt(root string, pid int, argvSelf string, q controlQuery) (bool, error) {
	info, err := readProcessInfoAt(root, pid)
	if err != nil {
		return false, err
	}
	if info.ppid != 0 {
		return false, nil
	}
	args, err := readArgvAt(root, pid)
	if err != nil {
		return false, err
	}
	return helperArgsMatch(args, argvSelf, q), nil
}

func helperArgsMatch(args []string, argvSelf string, q controlQuery) bool {
	if len(args) == 0 || args[0] != argvSelf || hasFlag(args, "-terminate") {
		return false
	}
	if argValue(args, "-effect") != q.EffectID || argValue(args, "-spec") != q.Spec || argValue(args, "-epoch") != q.Epoch {
		return false
	}
	value, err := strconv.ParseUint(argValue(args, "-generation"), 10, 64)
	return err == nil && value == q.Generation
}

func verifyPinnedHelper(root string, pid int, q controlQuery) error {
	info, err := readProcessInfoAt(root, pid)
	if err != nil || info.ppid != 0 {
		return errors.New("job runner: pinned helper lost its PPid 0 identity")
	}
	args, err := readArgvAt(root, pid)
	if err != nil || len(args) == 0 || args[0] != os.Args[0] || hasFlag(args, "-terminate") || argValue(args, "-effect") != q.EffectID || argValue(args, "-spec") != q.Spec || argValue(args, "-epoch") != q.Epoch {
		return errors.New("job runner: pinned helper argv changed")
	}
	if value, err := strconv.ParseUint(argValue(args, "-generation"), 10, 64); err != nil || value != q.Generation {
		return errors.New("job runner: pinned helper generation changed")
	}
	// PR_SET_DUMPABLE 0 intentionally makes /proc/<pid>/exe unreadable to the
	// same user, so executable identity is the read-only argv[0] path together
	// with PPid 0 and the optional Docker PID cross-check. A job without Docker
	// control cannot create a PPid 0 process that names this path.
	return nil
}

func findChildAt(root string, helperPID int) (int, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, err
	}
	var found []int
	for _, entry := range entries {
		if !entry.IsDir() || !decimal(entry.Name()) {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		info, err := readProcessInfoAt(root, pid)
		if err != nil {
			if processGone(err) {
				continue
			}
			return 0, err
		}
		if info.ppid == helperPID && info.pgid == pid {
			found = append(found, pid)
		}
	}
	switch len(found) {
	case 0:
		return 0, errors.New("job runner: live helper has no unreaped job shell")
	case 1:
		return found[0], nil
	default:
		return 0, errors.New("job runner: live helper has more than one job shell")
	}
}

func verifyChild(root string, helperPID, childPID int) error {
	info, err := readProcessInfoAt(root, childPID)
	if err != nil || info.ppid != helperPID || info.pgid != childPID {
		return errors.New("job runner: pinned job shell changed")
	}
	return nil
}

func pidfdAlive(fd int) (bool, error) {
	err := unix.PidfdSendSignal(fd, 0, nil, 0)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, unix.ESRCH):
		return false, nil
	default:
		return false, err
	}
}

func readArgvAt(root string, pid int) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(root, strconv.Itoa(pid), "cmdline"))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) > 256<<10 {
		return nil, errors.New("job runner: helper argv is too large")
	}
	parts := bytes.Split(data, []byte{0})
	if len(parts) > 0 && len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}
	args := make([]string, len(parts))
	for i, part := range parts {
		args[i] = string(part)
	}
	return args, nil
}

func argValue(args []string, name string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == name {
			return args[i+1]
		}
	}
	return ""
}

func hasFlag(args []string, name string) bool {
	for _, arg := range args {
		if arg == name {
			return true
		}
	}
	return false
}

func canonicalSpec(spec string) bool {
	data, err := hex.DecodeString(spec)
	return err == nil && len(data) == 32 && hex.EncodeToString(data) == spec
}

func terminateChild(pid, pgid int, grace time.Duration) error {
	if pid <= 0 || pgid <= 1 || pid != pgid || grace <= 0 || !targetValid(pid, pgid) {
		return errors.New("job runner: invalid child termination request")
	}
	return terminateScanned(pid, pgid, grace, groupHasLiveTasks, func(signal unix.Signal) error {
		if !targetValid(pid, pgid) {
			return errors.New("job runner: the job process group is no longer the launch group")
		}
		if err := unix.Kill(-pgid, signal); err != nil && !errors.Is(err, unix.ESRCH) {
			return err
		}
		return nil
	})
}

// terminateScanned is the TERM/KILL escalation over an injectable scanner and
// signal delivery. A scanner error is uncertain, not proof of death: it is
// retried until the deadline, and the result is unproven if it remains
// uncertain. A KILL is always delivered (or ESRCH observed) before the final
// scan that can return success.
func terminateScanned(pid, pgid int, grace time.Duration, scan func(int) (bool, error), signal func(unix.Signal) error) error {
	if err := signal(unix.SIGTERM); err != nil {
		return err
	}
	deadline := time.Now().Add(grace)
	var lastErr error
	for {
		live, err := scan(pgid)
		lastErr = err
		if err == nil && !live {
			break
		}
		if !time.Now().Before(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	deadline = time.Now().Add(grace)
	for {
		if err := signal(unix.SIGKILL); err != nil {
			return err
		}
		live, err := scan(pgid)
		lastErr = err
		if err == nil && !live {
			return nil
		}
		if !time.Now().Before(deadline) {
			if lastErr != nil {
				return fmt.Errorf("job runner: process group status is uncertain: %w", lastErr)
			}
			return errors.New("job runner: the job process group still has a live process after KILL")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// groupHasLiveTasks reports whether any task of any process in pgid is not a
// zombie or dead entry. Reading every task, not only the thread-group leader,
// keeps a worker thread behind a zombie leader visible. The helper calls it
// with its child unreaped, so the PGID cannot be recycled while it signals.
func groupHasLiveTasks(pgid int) (bool, error) {
	return groupHasLiveTasksAt("/proc", pgid)
}

func groupHasLiveTasksAt(root string, pgid int) (bool, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !decimal(entry.Name()) {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		info, err := readProcessInfoAt(root, pid)
		if err != nil {
			if processGone(err) {
				continue
			}
			return false, err
		}
		if info.pgid != pgid {
			continue
		}
		live, err := processHasLiveTask(root, pid, info)
		if err != nil {
			return false, err
		}
		if live {
			return true, nil
		}
	}
	return false, nil
}

func processHasLiveTask(root string, pid int, info processInfo) (bool, error) {
	tasks, err := os.ReadDir(filepath.Join(root, strconv.Itoa(pid), "task"))
	if err != nil {
		if processGone(err) {
			return false, nil
		}
		return false, err
	}
	if len(tasks) == 0 {
		return !info.zombie(), nil
	}
	for _, task := range tasks {
		if !task.IsDir() || !decimal(task.Name()) {
			continue
		}
		tid, err := strconv.Atoi(task.Name())
		if err != nil {
			continue
		}
		state, err := readTaskStateAt(root, pid, tid)
		if err != nil {
			if processGone(err) {
				continue
			}
			return false, err
		}
		if !zombieState(state) {
			return true, nil
		}
	}
	return false, nil
}

type processInfo struct {
	pid   int
	ppid  int
	pgid  int
	state byte
}

func (p processInfo) zombie() bool { return zombieState(p.state) }

func zombieState(state byte) bool { return state == 'Z' || state == 'X' || state == 'x' }

// processGone reports the kernel's two disappearing-entry errors. /proc reads
// use ENOENT and may use ESRCH when the task exits between the directory and
// stat open; neither is an inspection failure.
func targetValid(pid, pgid int) bool {
	current, err := syscall.Getpgid(pid)
	return err == nil && current == pgid
}

func processGone(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ESRCH)
}

var errProcessStat = errors.New("job runner: transient or unreadable process status")

func readProcessInfoAt(root string, pid int) (processInfo, error) {
	data, err := os.ReadFile(filepath.Join(root, strconv.Itoa(pid), "stat"))
	if err != nil {
		return processInfo{}, err
	}
	return parseProcessInfo(data, pid)
}

func readTaskStateAt(root string, pid, tid int) (byte, error) {
	data, err := os.ReadFile(filepath.Join(root, strconv.Itoa(pid), "task", strconv.Itoa(tid), "stat"))
	if err != nil {
		return 0, err
	}
	info, err := parseProcessInfo(data, pid)
	if err != nil {
		return 0, err
	}
	return info.state, nil
}

// parseProcessInfo reads the fields of /proc/<pid>/stat after the parenthesized
// comm: state (field 3), ppid (4), and pgrp (5).
func parseProcessInfo(data []byte, pid int) (processInfo, error) {
	end := bytes.LastIndexByte(data, ')')
	if end < 0 {
		return processInfo{}, errors.New("job runner: malformed process status")
	}
	fields := bytes.Fields(data[end+1:])
	if len(fields) < 3 || len(fields[0]) != 1 || !documentedTaskState(fields[0][0]) {
		return processInfo{}, fmt.Errorf("%w: truncated or invalid state", errProcessStat)
	}
	ppid, err := strconv.Atoi(string(fields[1]))
	if err != nil || ppid < 0 {
		return processInfo{}, fmt.Errorf("%w: invalid parent", errProcessStat)
	}
	pgid, err := strconv.Atoi(string(fields[2]))
	if err != nil || pgid < 0 {
		return processInfo{}, fmt.Errorf("%w: invalid process group", errProcessStat)
	}
	return processInfo{pid: pid, ppid: ppid, pgid: pgid, state: fields[0][0]}, nil
}

// documentedTaskState accepts exactly the one-byte states /proc/<pid>/stat
// documents. A longer token (for example "ZZ") or an unknown letter is an
// uncertain read, never evidence that a task is gone.
func documentedTaskState(state byte) bool {
	switch state {
	case 'R', 'S', 'D', 'T', 't', 'Z', 'X', 'x', 'K', 'W', 'P', 'I':
		return true
	}
	return false
}

func waitNoReap(pid int) error {
	var info unix.Siginfo
	for {
		err := unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if err == unix.EINTR {
			continue
		}
		return err
	}
}

func decimal(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
