// Package docker runs harness effects in per-effect containers or as distinct
// execs in a prepared environment, through the Docker Engine API over
// connections that the docker CLI opens.
//
// An ephemeral job's container, with its Docker log, is removed once its result
// and output are durably recorded, so host disk holds
// at most Config.OutputLimit bytes of retained output per job, plus the Docker
// log of each job still running or awaiting capture, bounded only by its
// runtime.
package docker

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
)

// WorkDir is the working directory of every job, where Config.Workspace is
// mounted read-write.
const WorkDir = "/workspace"

// Config is the execution environment shared by every job of an executor.
type Config struct {
	// Docker is the CLI path; "docker" by default.
	Docker string
	// Helper is the canti-job executable mounted into every ephemeral
	// container. It defaults to the canti-job beside this executable.
	Helper string `json:"-"`
	// HelperDigest pins the helper content in the execution specification;
	// HelperProtocol pins its record contract.
	HelperDigest   string
	HelperProtocol int
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
	// PreviewLimit bounds the preview bytes; 50 KiB by default, at least 4 KiB.
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
	cfg Config
	client
}

// Every daemon call has its own deadline: callTimeout, extended by the stop
// grace for a stop, or transferTimeout to create a container, which can copy
// the image, to pull the image, to read a job's whole Docker log, and to wait
// for a job to stop.
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

// prepareConfig validates execution settings and applies defaults without I/O.
func prepareConfig(cfg Config) (Config, error) {
	if cfg.Image == "" {
		return Config{}, errors.New("docker: image is required")
	}
	if cfg.OutputLimit < 0 || cfg.PreviewLimit < 0 || cfg.StopGrace < 0 {
		return Config{}, errors.New("docker: limits must not be negative")
	}
	cfg.Docker = cmp.Or(cfg.Docker, "docker")
	if cfg.Helper == "" {
		cfg.Helper = os.Getenv("CANTI_JOB_HELPER")
		if cfg.Helper == "" {
			exe, err := os.Executable()
			if err != nil {
				return Config{}, fmt.Errorf("docker: locate canti-job: %w", err)
			}
			cfg.Helper = filepath.Join(filepath.Dir(exe), "canti-job")
		}
	}
	if !filepath.IsAbs(cfg.Helper) {
		return Config{}, errors.New("docker: helper must be an absolute path")
	}
	if cfg.HelperProtocol == 0 {
		cfg.HelperProtocol = jobrunner.Protocol
	}
	if cfg.HelperProtocol != jobrunner.Protocol {
		return Config{}, fmt.Errorf("docker: helper protocol %d is not supported; want %d", cfg.HelperProtocol, jobrunner.Protocol)
	}
	if digest, err := jobrunner.HelperDigest(cfg.Helper); err == nil {
		if cfg.HelperDigest == "" {
			cfg.HelperDigest = digest
		}
	} else if cfg.HelperDigest == "" {
		return Config{}, fmt.Errorf("docker: helper cannot be verified: %w", err)
	}
	cfg.User = cmp.Or(cfg.User, fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()))
	cfg.Network = cmp.Or(cfg.Network, "none")
	cfg.StopGrace = cmp.Or(cfg.StopGrace, 5*time.Second)
	cfg.OutputLimit = cmp.Or(cfg.OutputLimit, 8<<20)
	cfg.PreviewLimit = cmp.Or(cfg.PreviewLimit, jobrunner.MaxPreviewBytes)
	if cfg.PreviewLimit < jobrunner.MinPreviewBytes || cfg.PreviewLimit > jobrunner.MaxPreviewBytes {
		return Config{}, fmt.Errorf("docker: preview limit must be %d..%d bytes", jobrunner.MinPreviewBytes, jobrunner.MaxPreviewBytes)
	}
	// Validate numeric runtime settings before accepting a configuration whose
	// pure specification must describe a constructible request.
	if _, err := (&Executor{cfg: cfg}).request(harness.Effect{}, ""); err != nil {
		return Config{}, fmt.Errorf("docker: execution configuration: %w", err)
	}
	return cfg, nil
}

// CheckConfig validates execution limits without creating state or contacting Docker.
func CheckConfig(cfg Config) error { _, err := prepareConfig(cfg); return err }

// New validates cfg, applies its defaults, and creates the state layout. It
// does not contact the Docker daemon.
func New(cfg Config) (*Executor, error) {
	cfg, err := prepareConfig(cfg)
	if err != nil {
		return nil, err
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
	cfg, err = pinHelper(cfg)
	if err != nil {
		return nil, err
	}
	return &Executor{cfg: cfg, client: newClient(cliEngine(cfg.Docker))}, nil
}

// CheckLayout refuses, as New does, a workspace and a state directory that
// overlap and a state whose resolved path has a colon, which cannot be
// mounted, before the state directory exists and without touching the
// filesystem: the state, a clean absolute path, resolves as its nearest
// existing ancestor followed by the missing components. A state that is a
// file, or is below a file or a dangling link, is refused.
func CheckLayout(workspace, state string) error {
	return checkLayout(workspace, state, os.Lstat)
}

// ResolveState returns the canonical prospective State directory checked by
// CheckLayout, without creating it. Existing symlink ancestors are resolved;
// missing components are appended only after a real directory is established.
func ResolveState(workspace, state string) (string, error) {
	return resolveState(workspace, state, os.Lstat)
}

func checkLayout(workspace, state string, lstat func(string) (os.FileInfo, error)) error {
	_, err := resolveState(workspace, state, lstat)
	return err
}

func resolveState(workspace, state string, lstat func(string) (os.FileInfo, error)) (string, error) {
	ws, err := resolve(workspace)
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(state) || filepath.Clean(state) != state {
		return "", fmt.Errorf("docker: state %q is not a clean absolute path", state)
	}
	dir, rest := state, ""
	for {
		_, err := lstat(dir)
		if err == nil {
			break
		}
		if !errors.Is(err, fs.ErrNotExist) || filepath.Dir(dir) == dir {
			return "", fmt.Errorf("docker: state %s: %w", state, err)
		}
		dir, rest = filepath.Dir(dir), filepath.Join(filepath.Base(dir), rest)
	}
	// Resolve the entry actually found, including any link target. A sibling
	// may have created it while we walked the missing parents; an earlier
	// EvalSymlinks ENOENT must not override that successful observation.
	p, err := resolve(dir)
	if err != nil {
		return "", fmt.Errorf("docker: state %s: %w", state, err)
	}
	if p = filepath.Join(p, rest); within(ws, p) || within(p, ws) {
		return "", fmt.Errorf("docker: workspace %s and state %s overlap", ws, p)
	}
	if strings.Contains(p, ":") {
		return "", fmt.Errorf("docker: state %s is not a directory that can be mounted", p)
	}
	return p, nil
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

func (x *Executor) binds(effectID string) []string {
	b := []string{x.cfg.Workspace + ":" + WorkDir, x.artifacts() + ":" + archive.ArtifactsRoot + ":ro",
		x.cfg.Helper + ":/canti-job:ro", x.capture(effectID) + ":/canti/capture:rw"}
	for _, m := range x.cfg.Mounts {
		b = append(b, m.Source+":"+m.Target+":ro")
	}
	return b
}

func (x *Executor) capture(id string) string { return filepath.Join(x.dir(id), "capture") }

func (x *Executor) ensureCapture(id string) error {
	parent := x.dir(id)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return err
	}
	if err := os.Chmod(parent, 0700); err != nil {
		return err
	}
	path := x.capture(id)
	if err := os.MkdirAll(path, 0777); err != nil {
		return err
	}
	return os.Chmod(path, 0777)
}

// Specification hashes the actual create request, exact normalized config and
// retention rules. Its identity labels are blank to avoid self-reference.
// Image contents and daemon defaults remain external environment.
func (x *Executor) Specification(command string) string {
	request, _ := x.request(harness.Effect{Command: command}, "")
	for i, bind := range request.HostConfig.Binds {
		if bind == x.cfg.Helper+":/canti-job:ro" {
			request.HostConfig.Binds[i] = "/canti-job:/canti-job:ro"
		}
	}
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
