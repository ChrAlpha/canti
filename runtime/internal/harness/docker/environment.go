package docker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
)

const environmentFormat = 1
const controlLabel = "io.canti.control"
const helperRoot = "/canti"
const spoolRoot = "/var/lib/canti/jobs"

// Attachment is immutable host wiring. Its directory is the one authority for
// this container across all run state directories; no writable job mounts it.
type Attachment struct {
	Format                                       int
	Directory, Endpoint, DaemonID, ContainerID   string
	Incarnation                                  uint64
	Helper, HelperDigest, User, Home, Path, Init string
	AdmissionEpoch                               string
}

type environmentManifest struct {
	Format, HelperProtocol int
	Helper, Digest         string
}
type environmentState struct {
	Format                      int
	Gate                        string
	Generation, StartGeneration uint64
	Active                      string
	Requests                    []string
	Pending                     string
	LastStop                    string
}

type preparedContainer struct {
	ID     string
	Config struct {
		User, WorkingDir string
		Env              []string
		Labels           map[string]string
	}
	State struct {
		Status                      string
		Running, Paused, Restarting bool
		StartedAt                   time.Time
	}
	HostConfig struct {
		AutoRemove    bool
		RestartPolicy struct{ Name string }
	}
	Mounts []struct {
		Type, Source, Destination string
		RW                        bool
	}
}

// InitEnvironment prepares attachment metadata and a read-only versioned helper.
// Existing initialized directories are never reset or assigned a new identity.
func InitEnvironment(directory, helper string) error {
	dir, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return err
	}
	for _, name := range []string{"authority", "authority/jobs", "public", "spool"} {
		mode := os.FileMode(0700)
		if name == "public" {
			mode = 0755
		}
		if err = os.MkdirAll(filepath.Join(dir, name), mode); err != nil {
			return err
		}
	}
	unlock, err := flock(context.Background(), filepath.Join(dir, "authority", "lock"))
	if err != nil {
		return err
	}
	defer unlock()
	var old environmentManifest
	exists, err := readJSON(filepath.Join(dir, "authority", "manifest.json"), &old)
	if err != nil {
		return err
	}
	if exists {
		return validateManifest(old)
	}
	if _, err := os.Stat(filepath.Join(dir, "authority", "environment.json")); err == nil {
		return errors.New("docker: missing manifest of an existing attachment; authority cannot be reset")
	}
	if err = checkPreparedHelper(helper); err != nil {
		return err
	}
	f, err := os.Open(helper)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 128<<20+1))
	if err != nil {
		return err
	}
	if len(data) > 128<<20 {
		return errors.New("docker: helper exceeds 128 MiB")
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	relative := filepath.Join("bin", digest, "canti-job")
	dest := filepath.Join(dir, "public", relative)
	if err = os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return err
	}
	if err = publish(dest, data, false); err != nil {
		return err
	}
	if err = os.Chmod(dest, 0555); err != nil {
		return err
	}
	if err = syncDir(filepath.Dir(dest)); err != nil {
		return err
	}
	manifest := environmentManifest{environmentFormat, jobrunner.Protocol, filepath.ToSlash(relative), digest}
	if err = saveJSON(filepath.Join(dir, "authority", "state.json"), environmentState{Format: environmentFormat, Gate: "unattached"}, false); err != nil {
		return err
	}
	return saveJSON(filepath.Join(dir, "authority", "manifest.json"), manifest, true)
}
func validateManifest(m environmentManifest) error {
	if m.Format != environmentFormat || m.HelperProtocol != jobrunner.Protocol || m.Helper != "bin/"+m.Digest+"/canti-job" || len(m.Digest) != 64 {
		return errors.New("docker: unsupported environment manifest")
	}
	return nil
}
func saveJSON(path string, value any, exclusive bool) error {
	data, err := marshal(value)
	if err != nil {
		return err
	}
	return publish(path, data, exclusive)
}

// ReadAttachment loads the pinned identity without contacting Docker. Run input
// retries and retained receipt replay must not depend on daemon availability.
func ReadAttachment(directory string) (Attachment, error) {
	var a Attachment
	dir, err := resolve(directory)
	if err != nil {
		return a, err
	}
	found, err := readJSON(filepath.Join(dir, "authority", "environment.json"), &a)
	if err != nil {
		return a, err
	}
	if !found {
		return a, errors.New("docker: environment is not attached")
	}
	if a.Directory != dir {
		return a, errors.New("docker: attachment directory identity changed")
	}
	return a, a.validate()
}
func (a Attachment) validate() error {
	u, err := url.Parse(a.Endpoint)
	if err != nil || u.Scheme != "unix" || u.Host != "" || !filepath.IsAbs(u.Path) || u.RawQuery != "" || u.Fragment != "" || a.Format != environmentFormat || !canonicalDigest(a.HelperDigest) || !canonicalDigest(a.ContainerID) || filepath.Clean(a.Directory) != a.Directory || !filepath.IsAbs(a.Directory) || a.DaemonID == "" || len(a.ContainerID) != 64 || a.Incarnation == 0 || a.User == "" || !filepath.IsAbs(a.Home) || filepath.Clean(a.Home) != a.Home || a.Path == "" || a.Helper != "/canti/bin/"+a.HelperDigest+"/canti-job" {
		return errors.New("docker: invalid pinned environment attachment")
	}
	return nil
}
func environmentEngine(endpoint string) *Executor {
	return &Executor{cfg: Config{Docker: "docker", StopGrace: 5 * time.Second}, timeouts: timeouts{callTimeout, transferTimeout}, engine: endpointEngine("docker", endpoint)}
}
func (x *Executor) inspectPrepared(ctx context.Context, id string) (preparedContainer, error) {
	var c preparedContainer
	err := x.api(ctx, "inspect", http.MethodGet, "/containers/"+url.PathEscape(id)+"/json", nil, decodeResponse(&c))
	return c, err
}
func (x *Executor) daemon(ctx context.Context) (string, error) {
	var info struct{ ID, OSType, Architecture string }
	if err := x.api(ctx, "info", http.MethodGet, "/info", nil, decodeResponse(&info)); err != nil {
		return "", err
	}
	arch := info.Architecture
	if arch == "x86_64" {
		arch = "amd64"
	}
	if arch == "aarch64" {
		arch = "arm64"
	}
	if info.ID == "" || info.OSType != "linux" || runtime.GOOS != "linux" || arch != runtime.GOARCH {
		return "", errors.New("docker: attachment requires a local Linux daemon matching the helper architecture")
	}
	return info.ID, nil
}

// AttachEnvironment pins an explicitly prepared, running container. Its immutable
// control label is the single choice of authority; a second directory is refused.
func AttachEnvironment(ctx context.Context, directory, endpoint, selector, init string) (Attachment, error) {
	dir, err := resolve(directory)
	if err != nil {
		return Attachment{}, err
	}
	if selector == "" || strings.ContainsAny(selector, "/\\\x00") {
		return Attachment{}, errors.New("docker: container selector is required")
	}
	if init != "" && (!filepath.IsAbs(init) || filepath.Clean(init) != init) {
		return Attachment{}, errors.New("docker: init path must be clean and absolute")
	}
	u, parseErr := url.Parse(endpoint)
	if parseErr != nil || u.Scheme != "unix" || u.Host != "" || !filepath.IsAbs(u.Path) || u.RawQuery != "" || u.Fragment != "" {
		return Attachment{}, errors.New("docker: attachment requires a local Unix Docker endpoint")
	}
	x := environmentEngine(endpoint)
	unlock, err := flock(ctx, filepath.Join(dir, "authority", "lock"))
	if err != nil {
		return Attachment{}, err
	}
	defer unlock()
	m, state, err := readEnvironment(dir)
	if err != nil {
		return Attachment{}, err
	}
	if state.Active != "" {
		return Attachment{}, errors.New("docker: environment has unreconciled work")
	}
	backend, err := x.daemon(ctx)
	if err != nil {
		return Attachment{}, err
	}
	c, err := x.inspectPrepared(ctx, selector)
	if err != nil {
		return Attachment{}, err
	}
	if err = checkPrepared(dir, c); err != nil {
		return Attachment{}, err
	}
	a := Attachment{Format: environmentFormat, Directory: dir, Endpoint: endpoint, DaemonID: backend, ContainerID: c.ID, Incarnation: 1, Helper: "/canti/" + m.Helper, HelperDigest: m.Digest, User: c.Config.User, Init: init, AdmissionEpoch: c.State.StartedAt.Format(time.RFC3339Nano)}
	for _, v := range c.Config.Env {
		key, value, _ := strings.Cut(v, "=")
		switch key {
		case "HOME":
			a.Home = value
		case "PATH":
			a.Path = value
		}
	}
	var unchanged bool
	a, unchanged, err = attachmentIncarnation(dir, a, state)
	if err != nil {
		return Attachment{}, err
	}
	if unchanged {
		if state.Gate == "unattached" {
			state.Gate = "open"
			if err = saveEnvironmentState(dir, state); err != nil {
				return Attachment{}, err
			}
		}
		return a, a.validate()
	}

	if err = a.validate(); err != nil {
		return Attachment{}, err
	}
	if err = x.mountChallenge(ctx, a); err != nil {
		return Attachment{}, err
	}
	if err = saveJSON(filepath.Join(dir, "authority", "environment.json"), a, false); err != nil {
		return Attachment{}, err
	}
	state.Gate = "open"
	if err = saveEnvironmentState(dir, state); err != nil {
		return Attachment{}, err
	}
	return a, nil
}
func checkPrepared(dir string, c preparedContainer) error {
	if c.Config.Labels[controlLabel] != dir || !c.State.Running || c.State.Paused || c.State.Restarting || c.HostConfig.AutoRemove || c.HostConfig.RestartPolicy.Name != "" && c.HostConfig.RestartPolicy.Name != "no" {
		return errors.New("docker: prepared environment needs matching control label, running state, no auto-remove and no automatic restart")
	}
	public, spool := false, false
	for _, m := range c.Mounts {
		switch m.Destination {
		case helperRoot:
			public = m.Type == "bind" && m.Source == filepath.Join(dir, "public") && !m.RW
		case spoolRoot:
			spool = m.Type == "bind" && m.Source == filepath.Join(dir, "spool") && m.RW
		default:
			if within(m.Source, dir) || within(dir, m.Source) {
				return errors.New("docker: environment mounts trusted control state")
			}
		}
	}
	if !public || !spool {
		return errors.New("docker: prepared environment needs separate read-only /canti helper and writable /var/lib/canti/jobs bind mounts")
	}
	return nil
}
func readEnvironment(dir string) (environmentManifest, environmentState, error) {
	var m environmentManifest
	var s environmentState
	for _, entry := range []struct {
		name  string
		value any
	}{{"manifest.json", &m}, {"state.json", &s}} {
		found, err := readJSON(filepath.Join(dir, "authority", entry.name), entry.value)
		if err != nil {
			return m, s, err
		}
		if !found {
			return m, s, errors.New("docker: missing environment authority; initialize a new attachment explicitly")
		}
	}
	if err := validateManifest(m); err != nil {
		return m, s, err
	}
	if s.Format != environmentFormat || s.Gate != "unattached" && s.Gate != "open" && s.Gate != "stopped" && s.Gate != "stopping" && s.Gate != "starting" && s.Gate != "detached" && s.Gate != "quarantined" && s.Gate != "retiring" && s.Gate != "retired" {
		return m, s, errors.New("docker: invalid environment control gate")
	}
	return m, s, nil
}
func saveEnvironmentState(dir string, s environmentState) error {
	return saveJSON(filepath.Join(dir, "authority", "state.json"), s, false)
}
func (x *Executor) mountChallenge(ctx context.Context, a Attachment) error {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	name := ".attach-" + hex.EncodeToString(nonce[:])
	file := filepath.Join(a.Directory, "public", name)
	data := []byte(name)
	if err := os.WriteFile(file, data, 0444); err != nil {
		return err
	}
	defer os.Remove(file)
	got, err := x.archiveFile(ctx, a.ContainerID, "/canti/"+name, 4096)
	if err != nil {
		return err
	}
	if string(got) != string(data) {
		return errors.New("docker: prepared helper mount is not the local attachment directory")
	}
	return nil
}

// EnvironmentWorkdir returns the unique writable bind mapping of this host
// workspace. It also excludes the current journal from all environment mounts.
func EnvironmentWorkdir(ctx context.Context, a Attachment, workspace, state, requested string) (string, error) {
	x := environmentEngine(a.Endpoint)
	if err := x.checkAttachment(ctx, a); err != nil {
		return "", err
	}
	c, err := x.inspectPrepared(ctx, a.ContainerID)
	if err != nil {
		return "", err
	}
	var candidates []string
	for _, m := range c.Mounts {
		if m.Source != "" && (within(m.Source, state) || within(state, m.Source)) {
			return "", errors.New("docker: environment mount exposes run state")
		}
		if m.Type != "bind" {
			continue
		}
		if within(m.Source, state) || within(state, m.Source) {
			return "", errors.New("docker: environment mount exposes run state")
		}
		if m.RW && within(m.Source, workspace) {
			rel, _ := filepath.Rel(m.Source, workspace)
			candidates = append(candidates, filepath.Join(m.Destination, rel))
		}
	}
	if len(candidates) != 1 {
		return "", errors.New("docker: workspace requires one unambiguous writable environment bind mapping")
	}
	cwd := candidates[0]
	if requested != "" {
		if !filepath.IsAbs(requested) || filepath.Clean(requested) != requested || !within(cwd, requested) {
			return "", errors.New("docker: environment workdir must be inside the mapped workspace")
		}
		cwd = requested
	}
	return cwd, nil
}
func (x *Executor) checkAttachment(ctx context.Context, a Attachment) error {
	if err := a.validate(); err != nil {
		return err
	}
	current, err := ReadAttachment(a.Directory)
	if err != nil {
		return err
	}
	if current.ContainerID != a.ContainerID || current.DaemonID != a.DaemonID || current.Incarnation != a.Incarnation || current.Endpoint != a.Endpoint {
		return fmt.Errorf("%w: environment attachment changed", harness.ErrJobLost)
	}
	id, err := x.daemon(ctx)
	if err != nil {
		return err
	}
	if id != a.DaemonID {
		return fmt.Errorf("%w: Docker daemon identity changed", harness.ErrJobLost)
	}
	c, err := x.inspectPrepared(ctx, a.ContainerID)
	if err != nil {
		return err
	}
	if c.ID != a.ContainerID || c.Config.Labels[controlLabel] != a.Directory {
		return fmt.Errorf("%w: environment identity changed", harness.ErrJobLost)
	}
	return nil
}

func attachmentIncarnation(dir string, a Attachment, state environmentState) (Attachment, bool, error) {
	var old Attachment
	found, err := readJSON(filepath.Join(dir, "authority", "environment.json"), &old)
	if err != nil {
		return a, false, err
	}
	if !found {
		return a, false, nil
	}
	if old.ContainerID == a.ContainerID && old.DaemonID == a.DaemonID && old.Init == a.Init && old.Endpoint == a.Endpoint && old.HelperDigest == a.HelperDigest && state.Gate != "detached" && state.Gate != "retired" {
		return old, true, nil
	}
	if state.Gate != "detached" && state.Gate != "retired" {
		return a, false, errors.New("docker: detach or retire the previous attachment before changing its identity")
	}
	if state.Gate == "retired" && old.ContainerID == a.ContainerID {
		return a, false, errors.New("docker: retired attachment requires a genuinely new container ID")
	}
	a.Incarnation = old.Incarnation + 1
	if a.Incarnation == 0 {
		return a, false, errors.New("docker: attachment incarnation exhausted")
	}
	return a, false, nil
}

func checkPreparedHelper(helper string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, helper, "-version")
	command.Env = []string{"PATH=/usr/bin:/bin", "HOME=/nonexistent"}
	command.WaitDelay = time.Second
	out := &tail{}
	command.Stdout, command.Stderr = out, io.Discard
	if err := command.Run(); err != nil {
		return fmt.Errorf("docker: check canti-job helper: %w", err)
	}
	if strings.TrimSpace(out.String()) != fmt.Sprint(jobrunner.Protocol) {
		return errors.New("docker: unsupported canti-job helper protocol")
	}
	return nil
}

func canonicalDigest(value string) bool {
	data, err := hex.DecodeString(value)
	return err == nil && len(data) == sha256.Size && hex.EncodeToString(data) == value
}
