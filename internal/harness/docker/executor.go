// Package docker runs harness effects as Docker containers through the docker
// CLI.
package docker

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
)

// WorkDir is the working directory of every job, where Config.Workspace is
// mounted read-write.
const WorkDir = "/workspace"

// ArtifactDir is where the executor's artifact root is mounted read-only, so
// a job can read the retained output of earlier jobs.
const ArtifactDir = "/canti/artifacts"

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
	// subdirectory is mounted, read-only; it must not overlap Workspace.
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
}

// Executor runs each effect as one container named canti-<effect ID>,
// labelled with the hash of its full execution spec. The start latch, cancel
// marker, and output metadata live under Config.State and every operation
// holds the effect's file lock, so callers in any goroutine or process never
// observe a half-done start. Host environment variables never reach a job.
type Executor struct {
	cfg Config
}

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
	for _, dir := range []string{"jobs", "artifacts"} {
		if err := os.MkdirAll(filepath.Join(state, dir), 0o755); err != nil {
			return nil, err
		}
	}
	if err := syncDir(state); err != nil {
		return nil, err
	}
	cfg.Workspace, cfg.State = ws, state
	return &Executor{cfg: cfg}, nil
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
	return append(args, "-v", c.Workspace+":"+WorkDir, "-v", x.artifacts()+":"+ArtifactDir+":ro",
		c.Image, "-c", "exec 2>&1\n"+command)
}

func specHash(run []string) string {
	h := sha256.New()
	for _, a := range append([]string{"canti.docker/v1"}, run...) {
		fmt.Fprintf(h, "%d:%s", len(a), a)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (x *Executor) createArgs(id string, run []string) []string {
	return append([]string{"create", "--name", "canti-" + id, "--label", "canti.effect=" + id,
		"--label", "canti.state=" + x.cfg.State, "--label", "canti.spec=" + specHash(run)}, run...)
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

const launchTimeout = 30 * time.Second

func (x *Executor) command(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, x.cfg.Docker, args...)
	cmd.WaitDelay = time.Second
	return cmd
}

func (x *Executor) docker(ctx context.Context, args ...string) ([]byte, error) {
	out, err := x.command(ctx, args...).Output()
	if err == nil {
		return out, nil
	}
	if cerr := ctx.Err(); cerr != nil {
		return nil, cerr
	}
	var stderr string
	if ee := (*exec.ExitError)(nil); errors.As(err, &ee) {
		stderr = strings.TrimSpace(string(ee.Stderr))
	}
	if s := strings.ToLower(stderr); strings.Contains(s, "no such container") || strings.Contains(s, "no such object") {
		err = errMissing
	}
	return nil, fmt.Errorf("docker %s: %w: %s", args[0], err, stderr)
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

// find is inspect, except that the container of a started effect is never
// reported missing: its removal leaves the outcome unknown.
func (x *Executor) find(ctx context.Context, id string) (container, bool, error) {
	c, found, err := x.inspect(ctx, id)
	if err == nil && !found {
		var latch string
		if latch, err = readLatch(x.dir(id)); err == nil && latch == latchStart {
			err = fmt.Errorf("%w: container of effect %s is gone", harness.ErrJobLost, id)
		}
	}
	return c, found, err
}

func (x *Executor) bound(ctx context.Context, id string) (container, error) {
	c, found, err := x.find(ctx, id)
	if err == nil && !found {
		err = fmt.Errorf("docker: no job is bound to effect %s", id)
	}
	return c, err
}

func (x *Executor) report(ctx context.Context, id string, c container) (harness.Job, error) {
	latch, err := readLatch(x.dir(id))
	if err != nil {
		return harness.Job{}, err
	}
	marked, err := exists(filepath.Join(x.dir(id), "cancel"))
	if err != nil {
		return harness.Job{}, err
	}
	job := derive(latch, c, marked)
	if terminal(job.Status) {
		if job.Preview, job.Output, err = x.output(ctx, id, c.ID, job.Status); err != nil {
			return harness.Job{}, err
		}
	}
	return job, nil
}

// Bind creates the effect's container without starting it, or reports the
// existing one if it was created for the same execution spec.
func (x *Executor) Bind(ctx context.Context, effect harness.Effect) (harness.Job, error) {
	unlock, err := x.lock(ctx, effect.ID)
	if err != nil {
		return harness.Job{}, err
	}
	defer unlock()
	run := x.runArgs(effect.Command)
	c, found, err := x.find(ctx, effect.ID)
	if err == nil && !found {
		if _, err = x.docker(ctx, x.createArgs(effect.ID, run)...); err == nil {
			c, err = x.bound(ctx, effect.ID)
		}
	}
	if err != nil {
		return harness.Job{}, err
	}
	if c.Config.Labels["canti.spec"] != specHash(run) {
		return harness.Job{}, fmt.Errorf("docker: effect %s is bound to a different execution spec", effect.ID)
	}
	return x.report(ctx, effect.ID, c)
}

// Start sets the effect's latch and starts its never-started container,
// unless the latch was already set, in which case it only reports the job.
// Once the latch is set the launch finishes even if ctx is done, within a
// fixed timeout.
func (x *Executor) Start(ctx context.Context, effectID string) (harness.Job, error) {
	unlock, err := x.lock(ctx, effectID)
	if err != nil {
		return harness.Job{}, err
	}
	defer unlock()
	c, err := x.bound(ctx, effectID)
	if err != nil {
		return harness.Job{}, err
	}
	latch, err := readLatch(x.dir(effectID))
	if err != nil {
		return harness.Job{}, err
	}
	if latch == "" {
		if c.State.Status != "created" {
			return harness.Job{}, fmt.Errorf("docker: container %s of effect %s is %s without a start latch", c.ID, effectID, c.State.Status)
		}
		if err := ctx.Err(); err != nil {
			return harness.Job{}, err
		}
		if err := setLatch(x.dir(effectID), latchStart); err != nil {
			return harness.Job{}, err
		}
		launch, cancel := context.WithTimeout(context.WithoutCancel(ctx), launchTimeout)
		defer cancel()
		if _, err := x.docker(launch, "start", c.ID); err != nil {
			return harness.Job{}, err
		}
		if c, err = x.bound(launch, effectID); err != nil {
			return harness.Job{}, err
		}
	}
	return x.report(ctx, effectID, c)
}

// Inspect reports the effect's job; found is false only when Docker reports
// that no container exists for an effect that never started.
func (x *Executor) Inspect(ctx context.Context, effectID string) (harness.Job, bool, error) {
	unlock, err := x.lock(ctx, effectID)
	if err != nil {
		return harness.Job{}, false, err
	}
	defer unlock()
	c, found, err := x.find(ctx, effectID)
	if err != nil || !found {
		return harness.Job{}, false, err
	}
	job, err := x.report(ctx, effectID, c)
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
	c, found, err := x.inspect(ctx, effectID)
	if err != nil || !found || derive(latch, c, false).Status != harness.JobRunning {
		return err
	}
	if err := mark(dir); err != nil {
		return err
	}
	grace := (x.cfg.StopGrace + time.Second - 1) / time.Second
	_, err = x.docker(ctx, "stop", "-t", strconv.Itoa(int(grace)), c.ID)
	return err
}
