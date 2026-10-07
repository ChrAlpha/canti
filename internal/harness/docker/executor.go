// Package docker runs harness effects as Docker containers through the docker
// CLI.
//
// A finished job's container, with its Docker log, is removed once its result
// is recorded, even when its output could not be captured, so host disk holds
// at most Config.OutputLimit bytes of retained output per job, plus the Docker
// log of each job still running or awaiting capture, bounded only by its
// runtime.
package docker

import (
	"archive/tar"
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ChrAlpha/canti/internal/archive"
	"github.com/ChrAlpha/canti/internal/harness"
)

// WorkDir is the working directory of every job, where Config.Workspace is
// mounted read-write.
const WorkDir = "/workspace"

// Config is the execution environment shared by every job of an executor.
type Config struct {
	// Docker is the CLI path; "docker" by default.
	Docker string
	// Image runs every job; required.
	Image string
	// Workspace is an absolute host directory mounted read-write at WorkDir.
	Workspace string
	// State is an existing absolute host directory owned by the executor,
	// holding per-effect locks, latches, and artifacts. Only its artifacts
	// subdirectory and any Mounts inside it are mounted, read-only; it must
	// not overlap Workspace.
	State string
	// User is the job's "uid:gid"; the current process's by default.
	User string
	// Network is the job's network; "none" by default.
	Network string
	// Memory and CPUs set --memory and --cpus when not empty.
	Memory, CPUs string
	// PidsLimit sets --pids-limit when positive.
	PidsLimit int
	// StopGrace is how long Cancel lets a job exit before it is killed;
	// 5s by default, rounded up to whole seconds.
	StopGrace time.Duration
	// OutputLimit bounds the bytes retained per job artifact; 8 MiB by
	// default.
	OutputLimit int64
	// PreviewLimit bounds the preview bytes; 4 KiB by default, at least 64.
	PreviewLimit int
	// Mounts are mounted read-only into every job, in order.
	Mounts []Mount
}

// Mount mounts Source, an existing absolute host directory that neither
// overlaps the workspace nor contains the state directory, read-only at
// Target, a clean absolute container path that overlaps neither WorkDir nor
// archive.ArtifactsRoot, where the executor's artifact root is mounted
// read-only so a job can read the retained output of earlier jobs.
type Mount struct{ Source, Target string }

// Executor runs each effect as one container named canti-<effect ID>,
// labelled with the hash of its full execution spec. The start latch, cancel
// marker, and final record live under Config.State and every operation holds
// the effect's file lock, so callers in any goroutine or process never
// observe a half-done start. A container that exits without the exit status
// its shell records, as when it is killed outside canti, reports JobUnknown.
// Only Inspect reads that record and captures and records the job of a
// finished container; once recorded, a job is reported from its record
// alone. Host environment variables never reach a job.
type Executor struct {
	cfg      Config
	timeouts timeouts
}

// Every docker CLI call has its own deadline, so a hung daemon cannot hold an
// effect's lock forever: callTimeout, extended by Config.StopGrace for a
// stop, or transferTimeout for a create, which may pull the image, and to
// read a job's whole Docker log.
const (
	callTimeout     = 30 * time.Second
	transferTimeout = 10 * time.Minute
)

// timeouts are the deadlines of docker CLI calls; tests shorten them.
type timeouts struct{ call, transfer time.Duration }

var _ harness.Executor = (*Executor)(nil)

// New validates cfg, applies its defaults, and creates the state layout. It
// does not contact the Docker daemon.
func New(cfg Config) (*Executor, error) {
	if cfg.Image == "" {
		return nil, errors.New("docker: image is required")
	}
	if cfg.OutputLimit < 0 || cfg.PreviewLimit < 0 || cfg.StopGrace < 0 {
		return nil, errors.New("docker: limits must not be negative")
	}
	cfg.Docker = cmp.Or(cfg.Docker, "docker")
	cfg.User = cmp.Or(cfg.User, fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()))
	cfg.Network = cmp.Or(cfg.Network, "none")
	cfg.StopGrace = cmp.Or(cfg.StopGrace, 5*time.Second)
	cfg.OutputLimit = cmp.Or(cfg.OutputLimit, 8<<20)
	cfg.PreviewLimit = cmp.Or(cfg.PreviewLimit, 4<<10)
	if cfg.PreviewLimit < 64 {
		return nil, errors.New("docker: preview limit must be at least 64 bytes")
	}
	ws, err := resolve(cfg.Workspace)
	if err != nil {
		return nil, err
	}
	state, err := resolve(cfg.State)
	if err != nil {
		return nil, err
	}
	if within(ws, state) || within(state, ws) {
		return nil, fmt.Errorf("docker: workspace %s and state %s overlap", ws, state)
	}
	mounts := make([]Mount, len(cfg.Mounts))
	for i, m := range cfg.Mounts {
		src, err := resolve(m.Source)
		if err != nil {
			return nil, err
		}
		t := m.Target
		switch {
		case within(ws, src) || within(src, ws) || within(src, state):
			return nil, fmt.Errorf("docker: mount %s overlaps workspace %s or contains state %s", src, ws, state)
		case !path.IsAbs(t) || path.Clean(t) != t || strings.Contains(t, ":") || within(t, WorkDir) || within(WorkDir, t) || within(t, archive.ArtifactsRoot) || within(archive.ArtifactsRoot, t):
			return nil, fmt.Errorf("docker: mount target %q is not a clean absolute path apart from %s and %s", t, WorkDir, archive.ArtifactsRoot)
		}
		mounts[i] = Mount{src, t}
	}
	for _, dir := range []string{"jobs", "artifacts"} {
		if err := os.MkdirAll(filepath.Join(state, dir), 0o755); err != nil {
			return nil, err
		}
	}
	if err := syncDir(state); err != nil {
		return nil, err
	}
	cfg.Workspace, cfg.State, cfg.Mounts = ws, state, mounts
	return &Executor{cfg: cfg, timeouts: timeouts{callTimeout, transferTimeout}}, nil
}

// CheckLayout refuses a workspace and a state directory that overlap, as New
// does, before the state directory exists and without touching the
// filesystem: the state, a clean absolute path, resolves as its nearest
// existing ancestor followed by the missing components. A state below a
// file or a dangling link is refused.
func CheckLayout(workspace, state string) error {
	ws, err := resolve(workspace)
	if err != nil {
		return err
	}
	if !filepath.IsAbs(state) || filepath.Clean(state) != state {
		return fmt.Errorf("docker: state %q is not a clean absolute path", state)
	}
	dir, rest := state, ""
	p, err := filepath.EvalSymlinks(dir)
	for err != nil {
		if _, missing := os.Lstat(dir); !errors.Is(err, fs.ErrNotExist) || !errors.Is(missing, fs.ErrNotExist) || filepath.Dir(dir) == dir {
			return fmt.Errorf("docker: state %s: %w", state, err)
		}
		dir, rest = filepath.Dir(dir), filepath.Join(filepath.Base(dir), rest)
		p, err = filepath.EvalSymlinks(dir)
	}
	if p = filepath.Join(p, rest); within(ws, p) || within(p, ws) {
		return fmt.Errorf("docker: workspace %s and state %s overlap", ws, p)
	}
	return nil
}

func resolve(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("docker: %q is not an absolute path", path)
	}
	p, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	if fi, err := os.Stat(p); err != nil || !fi.IsDir() || strings.Contains(p, ":") {
		return "", fmt.Errorf("docker: %s is not a directory that can be mounted", p)
	}
	return p, nil
}

func within(dir, path string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func (x *Executor) runArgs(command string) []string {
	c := x.cfg
	args := []string{"--init", "--entrypoint", "/bin/sh", "--workdir", WorkDir, "--user", c.User,
		"--network", c.Network, "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
		"--log-driver", "json-file", "--log-opt", "max-size=1p", "--log-opt", "mode=blocking"}
	if c.Memory != "" {
		args = append(args, "--memory", c.Memory)
	}
	if c.CPUs != "" {
		args = append(args, "--cpus", c.CPUs)
	}
	if c.PidsLimit > 0 {
		args = append(args, "--pids-limit", strconv.Itoa(c.PidsLimit))
	}
	for _, k := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "FTP_PROXY", "ALL_PROXY"} {
		args = append(args, "-e", k+"=", "-e", strings.ToLower(k)+"=")
	}
	args = append(args, "-v", c.Workspace+":"+WorkDir, "-v", x.artifacts()+":"+archive.ArtifactsRoot+":ro")
	for _, m := range c.Mounts {
		args = append(args, "-v", m.Source+":"+m.Target+":ro")
	}
	return append(args, c.Image, "-c", "exec 2>&1\n"+command)
}

// wrapper runs the job's script in a child shell exactly as an earlier
// revision ran it and records the child's exit status in the container's
// writable layer, which outlives a container killed outside canti. It
// discards its own diagnostics, survives the stop signals that tini sends the
// whole process group, and unsets the variables it adds to the environment.
const wrapper = `exec 3>&2 2>/dev/null
unset TINI_KILL_PROCESS_GROUP SHLVL
trap : HUP INT QUIT TERM USR1 USR2
(exec /bin/sh -c "$1" 2>&3 3>&-)
s=$?
rm -f ` + exitFile + `
set -C
echo $s >` + exitFile + `
exit $s`

func specHash(run []string) string {
	h := sha256.New()
	for _, a := range append([]string{"canti.docker/v1"}, run...) {
		fmt.Fprintf(h, "%d:%s", len(a), a)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// createArgs creates the job of run under wrapper. The spec hash covers run
// alone, so a container of an earlier revision, which ran run directly and
// has no exitLabel, still binds and is derived from Docker's exit code.
func (x *Executor) createArgs(id string, run []string) []string {
	args := []string{"create", "--name", "canti-" + id, "--label", "canti.effect=" + id, "--label", "canti.state=" + x.cfg.State,
		"--label", "canti.spec=" + specHash(run), "--label", exitLabel + "=" + exitFile, "-e", "TINI_KILL_PROCESS_GROUP=1"}
	n := len(run) - 1
	return append(append(args, run[:n]...), wrapper, "canti", run[n])
}

func (x *Executor) dir(id string) string { return filepath.Join(x.cfg.State, "jobs", id) }

func (x *Executor) artifacts() string { return filepath.Join(x.cfg.State, "artifacts") }

func (x *Executor) lock(ctx context.Context, id string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkID(id); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(x.dir(id), 0o700); err != nil {
		return nil, err
	}
	return flock(ctx, filepath.Join(x.dir(id), "lock"))
}

var errMissing = errors.New("no such container")

func (x *Executor) command(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, x.cfg.Docker, args...)
	cmd.WaitDelay = time.Second
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd
}

// docker runs one CLI call under its own deadline. Once ctx is done it
// returns ctx's error; a call past only its own deadline fails with an
// ordinary error, never mistaken for the caller's shutdown.
func (x *Executor) docker(ctx context.Context, args ...string) ([]byte, error) {
	limit := x.timeouts.call
	switch args[0] {
	case "create":
		limit = x.timeouts.transfer
	case "stop":
		limit += x.cfg.StopGrace
	}
	call, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	cmd := x.command(call, args...)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if args[0] == "cp" {
		cmd.Stdout = capped{&out}
	}
	err := cmd.Run()
	if err == nil {
		return out.Bytes(), nil
	}
	if cerr := ctx.Err(); cerr != nil {
		return nil, cerr
	}
	if call.Err() != nil {
		return nil, fmt.Errorf("docker %s: no response within %v", args[0], limit)
	}
	stderr := strings.TrimSpace(errOut.String())
	if s := strings.ToLower(stderr); strings.Contains(s, "no such container") || strings.Contains(s, "no such object") {
		err = errMissing
	}
	return nil, fmt.Errorf("docker %s: %w: %s", args[0], err, stderr)
}

// capped bounds the output of docker cp, which a job controls.
type capped struct{ b *bytes.Buffer }

func (c capped) Write(p []byte) (int, error) {
	if c.b.Len()+len(p) > 64<<10 {
		return 0, errors.New("output exceeds 64 KiB")
	}
	return c.b.Write(p)
}

// exitStatus reads the exit status that wrapper wrote to file in the
// container; it is nil when the file holds none.
func (x *Executor) exitStatus(ctx context.Context, containerID, file string) (*int, error) {
	out, err := x.docker(ctx, "cp", containerID+":"+file, "-")
	if err != nil {
		if s := strings.ToLower(err.Error()); strings.Contains(s, "could not find the file") || strings.Contains(s, "no such container:path") {
			return nil, nil
		}
		return nil, err
	}
	r := tar.NewReader(bytes.NewReader(out))
	if h, err := r.Next(); err != nil || h.Typeflag != tar.TypeReg || h.Size > 4 {
		return nil, nil
	}
	b, err := io.ReadAll(r)
	s, ok := strings.CutSuffix(string(b), "\n")
	code, cerr := strconv.Atoi(s)
	if err != nil || !ok || cerr != nil || strconv.Itoa(code) != s || code < 0 || code > 255 {
		return nil, nil
	}
	return &code, nil
}

func (x *Executor) inspect(ctx context.Context, id string) (container, bool, error) {
	out, err := x.docker(ctx, "container", "inspect", "canti-"+id)
	if errors.Is(err, errMissing) {
		return container{}, false, nil
	}
	if err != nil {
		return container{}, false, err
	}
	var cs []container
	if err := json.Unmarshal(out, &cs); err != nil || len(cs) != 1 {
		return container{}, false, fmt.Errorf("docker: unexpected inspect output for effect %s: %v", id, err)
	}
	if l := cs[0].Config.Labels; l["canti.effect"] != id || l["canti.state"] != x.cfg.State {
		return container{}, false, fmt.Errorf("docker: container %s is not owned by effect %s under %s", cs[0].ID, id, x.cfg.State)
	}
	return cs[0], true, nil
}

type observation struct {
	latch  string
	marked bool
	c      container
}

// observe reads the latch and cancel marker of an effect and inspects its
// container. The container of a started effect is never reported missing:
// its removal before a record leaves the outcome unknown.
func (x *Executor) observe(ctx context.Context, id string) (o observation, found bool, err error) {
	if o.latch, err = readLatch(x.dir(id)); err != nil {
		return o, false, err
	}
	if o.marked, err = exists(filepath.Join(x.dir(id), "cancel")); err != nil {
		return o, false, err
	}
	if o.c, found, err = x.inspect(ctx, id); err == nil && !found && o.latch == latchStart {
		err = fmt.Errorf("%w: container of effect %s is gone", harness.ErrJobLost, id)
	}
	return o, found, err
}

func (x *Executor) bound(ctx context.Context, id string) (observation, error) {
	o, found, err := x.observe(ctx, id)
	if err == nil && !found {
		err = fmt.Errorf("docker: no job is bound to effect %s", id)
	}
	return o, err
}

// exitRecord is the file holding the exit record that decides the result of
// an exited job, or "" when its result needs none.
func (o observation) exitRecord() string {
	if o.latch != latchStart || o.marked || o.c.State.Status != "exited" && o.c.State.Status != "dead" {
		return ""
	}
	return o.c.Config.Labels[exitLabel]
}

// snapshot is the job derived without reading its exit record or output: an
// exited job whose result needs the record is unknown until Inspect reads it.
func (o observation) snapshot() harness.Job {
	job := derive(o.latch, o.c, o.marked, nil)
	if o.exitRecord() != "" {
		job.Output = harness.ArtifactRef{}
	}
	return job
}

// finalize derives the job, reading its exit record when its result needs
// one, and records it once it is finished; recording a job then sweeps the
// state directory.
func (x *Executor) finalize(ctx context.Context, id string, o observation) (harness.Job, error) {
	var recorded *int
	if file := o.exitRecord(); file != "" {
		var err error
		if recorded, err = x.exitStatus(ctx, o.c.ID, file); err != nil {
			return harness.Job{}, err
		}
	}
	job := derive(o.latch, o.c, o.marked, recorded)
	if !terminal(job.Status) {
		return job, nil
	}
	job, err := x.finish(ctx, id, job, o.c.Config.Labels["canti.spec"])
	if err == nil {
		x.sweep(ctx)
	}
	return job, err
}

// Bind creates the effect's container without starting it, or reports the
// existing job if it was created for the same execution spec; like Start,
// it reports a snapshot of the job. An effect fenced before it had a
// container is recorded cancelled without one.
func (x *Executor) Bind(ctx context.Context, effect harness.Effect) (harness.Job, error) {
	unlock, err := x.lock(ctx, effect.ID)
	if err != nil {
		return harness.Job{}, err
	}
	defer unlock()
	run := x.runArgs(effect.Command)
	rec, done, err := x.recorded(ctx, effect.ID)
	o, spec := observation{}, rec.Spec
	if err == nil && !done {
		var found bool
		if o, found, err = x.observe(ctx, effect.ID); err == nil && !found {
			if o.latch == latchCancel {
				return x.finish(ctx, effect.ID, harness.Job{ID: fenced(effect.ID), Status: harness.JobCancelled}, specHash(run))
			}
			if _, err = x.docker(ctx, x.createArgs(effect.ID, run)...); err == nil {
				o, err = x.bound(ctx, effect.ID)
			}
		}
		spec = o.c.Config.Labels["canti.spec"]
	}
	if err != nil {
		return harness.Job{}, err
	}
	if spec != specHash(run) {
		return harness.Job{}, fmt.Errorf("docker: effect %s is bound to a different execution spec", effect.ID)
	}
	if done {
		return rec.Job, nil
	}
	return o.snapshot(), nil
}

// Start sets the effect's latch and starts its never-started container,
// unless the latch was already set. Once the latch is set the launch
// finishes even if ctx is done, each of its calls within its own deadline.
// Start reports a snapshot of the job, so a job that already exited is
// unknown until Inspect reads its exit record. A start the daemon rejected
// before the job's process began reports the job, failed to start, rather
// than the error.
func (x *Executor) Start(ctx context.Context, effectID string) (harness.Job, error) {
	unlock, err := x.lock(ctx, effectID)
	if err != nil {
		return harness.Job{}, err
	}
	defer unlock()
	if rec, done, err := x.recorded(ctx, effectID); err != nil || done {
		return rec.Job, err
	}
	o, err := x.bound(ctx, effectID)
	if err != nil {
		return harness.Job{}, err
	}
	if o.latch == "" {
		if o.c.State.Status != "created" {
			return harness.Job{}, fmt.Errorf("docker: container %s of effect %s is %s without a start latch", o.c.ID, effectID, o.c.State.Status)
		}
		if err := ctx.Err(); err != nil {
			return harness.Job{}, err
		}
		if err := setLatch(x.dir(effectID), latchStart); err != nil {
			return harness.Job{}, err
		}
		launch := context.WithoutCancel(ctx)
		_, started := x.docker(launch, "start", o.c.ID)
		o, err = x.bound(launch, effectID)
		if started != nil && (err != nil || !rejected(o.c)) {
			return harness.Job{}, started
		}
		if err != nil {
			return harness.Job{}, err
		}
	}
	return o.snapshot(), nil
}

// Inspect reports the effect's job; found is false only when Docker reports
// that no container exists for an effect that never started.
func (x *Executor) Inspect(ctx context.Context, effectID string) (harness.Job, bool, error) {
	unlock, err := x.lock(ctx, effectID)
	if err != nil {
		return harness.Job{}, false, err
	}
	defer unlock()
	if rec, done, err := x.recorded(ctx, effectID); err != nil || done {
		return rec.Job, done, err
	}
	o, found, err := x.observe(ctx, effectID)
	if err != nil || !found {
		return harness.Job{}, false, err
	}
	job, err := x.finalize(ctx, effectID, o)
	return job, err == nil, err
}

// Cancel fences an unstarted effect or stops its running container, waiting
// at most StopGrace before Docker kills the container's process tree.
func (x *Executor) Cancel(ctx context.Context, effectID string) error {
	unlock, err := x.lock(ctx, effectID)
	if err != nil {
		return err
	}
	defer unlock()
	dir := x.dir(effectID)
	latch, err := readLatch(dir)
	if err != nil || latch == latchCancel {
		return err
	}
	if latch == "" {
		return setLatch(dir, latchCancel)
	}
	if done, err := exists(x.record(effectID)); err != nil || done {
		return err
	}
	c, found, err := x.inspect(ctx, effectID)
	if err != nil || !found || !running(c) {
		return err
	}
	if err := mark(dir); err != nil {
		return err
	}
	grace := (x.cfg.StopGrace + time.Second - 1) / time.Second
	_, err = x.docker(ctx, "stop", "-t", strconv.Itoa(int(grace)), c.ID)
	return err
}
