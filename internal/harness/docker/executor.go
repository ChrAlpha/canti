// Package docker runs harness effects as Docker containers through the
// Docker Engine API, over connections that the docker CLI opens.
//
// A finished job's container, with its Docker log, is removed once its result
// and output are durably recorded, so host disk holds
// at most Config.OutputLimit bytes of retained output per job, plus the Docker
// log of each job still running or awaiting capture, bounded only by its
// runtime.
package docker

import (
	"archive/tar"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
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
// marker, and final record live under Config.State. Drive, Inspect and Cancel
// hold the effect's file lock; advisory Wait holds none. Callers never
// observe a half-done start. A container that exits without the exit status
// its shell records, as when it is killed outside canti, reports JobUnknown.
// Every operation that finds a finished job captures and records it.
// Once recorded, a job is reported
// from its record alone. Host environment variables never reach a job.
type Executor struct {
	cfg      Config
	timeouts timeouts
	engine   *engine
}

// Every daemon call has its own deadline, so a hung daemon cannot hold an
// effect's lock forever: callTimeout, extended by Config.StopGrace for a
// stop, or transferTimeout to create a container, which can copy the image,
// to pull the image, to read a job's whole Docker log, and to wait for a job
// to stop.
const (
	callTimeout     = 30 * time.Second
	transferTimeout = 10 * time.Minute
)

// timeouts are the deadlines of daemon calls; tests shorten them.
type timeouts struct{ call, transfer time.Duration }

var (
	_ harness.Executor = (*Executor)(nil)
	_ harness.Waiter   = (*Executor)(nil)
)

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
	// Validate numeric runtime settings before accepting a configuration whose
	// pure specification must describe a constructible request.
	if _, err := (&Executor{cfg: cfg}).request(harness.Effect{}, ""); err != nil {
		return nil, fmt.Errorf("docker: execution configuration: %w", err)
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
	return &Executor{cfg: cfg, timeouts: timeouts{callTimeout, transferTimeout}, engine: cliEngine(cfg.Docker)}, nil
}

// CheckLayout refuses, as New does, a workspace and a state directory that
// overlap and a state whose resolved path has a colon, which cannot be
// mounted, before the state directory exists and without touching the
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
	if strings.Contains(p, ":") {
		return fmt.Errorf("docker: state %s is not a directory that can be mounted", p)
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

// proxies are the variables the docker CLI's configuration could set in a
// job; every job sets them empty.
var proxies = []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "FTP_PROXY", "ALL_PROXY"}

func (x *Executor) binds() []string {
	b := []string{x.cfg.Workspace + ":" + WorkDir, x.artifacts() + ":" + archive.ArtifactsRoot + ":ro"}
	for _, m := range x.cfg.Mounts {
		b = append(b, m.Source+":"+m.Target+":ro")
	}
	return b
}

func script(command string) string { return "exec 2>&1\n" + command }

// wrapper runs the job's script in a child shell and records the child's
// exit status in the container's
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

// Specification hashes the actual create request, exact normalized config and
// retention rules. Its identity labels are blank to avoid self-reference.
// Image contents and daemon defaults remain external environment.
func (x *Executor) Specification(command string) string {
	request, _ := x.request(harness.Effect{Command: command}, "")
	canonical, _ := json.Marshal(struct {
		Protocol string
		Config   Config
		Request  createRequest
	}{"canti.docker/v2", x.cfg, request})
	return harness.ConfigDigest(canonical)
}

func (x *Executor) checkEffect(effect harness.Effect) error {
	if err := checkID(effect.ID); err != nil {
		return err
	}
	if effect.Spec != x.Specification(effect.Command) {
		return fmt.Errorf("docker: effect %s has a different execution specification", effect.ID)
	}
	return nil
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
	acquisition, cancel := context.WithTimeout(ctx, x.timeouts.call)
	defer cancel()
	return flock(acquisition, filepath.Join(x.dir(id), "lock"))
}

var errMissing = errors.New("no such container")

// capped bounds the archive of the exit record, which a job controls.
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
	var out bytes.Buffer
	err := x.api(ctx, "cp", http.MethodGet, "/containers/"+containerID+"/archive?path="+url.QueryEscape(file), nil, func(r *http.Response) error {
		_, err := io.Copy(capped{&out}, r.Body)
		return err
	})
	if status(err) == http.StatusNotFound && !errors.Is(err, errMissing) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r := tar.NewReader(&out)
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
	var c container
	err := x.api(ctx, "inspect", http.MethodGet, "/containers/canti-"+id+"/json", nil, decode(&c))
	if errors.Is(err, errMissing) {
		return container{}, false, nil
	}
	if err != nil {
		return container{}, false, err
	}
	if c.ID == "" {
		return container{}, false, fmt.Errorf("docker: unexpected inspect output for effect %s", id)
	}
	if l := c.Config.Labels; l["canti.effect"] != id || l["canti.state"] != x.cfg.State {
		return container{}, false, fmt.Errorf("docker: container %s is not owned by effect %s under %s", c.ID, id, x.cfg.State)
	}
	return c, true, nil
}

func decode(v any) func(*http.Response) error {
	return func(r *http.Response) error {
		d := json.NewDecoder(r.Body)
		if err := d.Decode(v); err != nil {
			return err
		}
		if _, err := d.Token(); err != io.EOF {
			return errors.New("trailing data")
		}
		return nil
	}
}
