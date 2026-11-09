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
const authorityLabel = "io.canti.authority"
const helperRoot = "/canti"
const spoolRoot = "/var/lib/canti/jobs"

// Attachment is immutable host wiring. Its directory is the one authority for
// this container across all run state directories; no writable job mounts it.
type Attachment struct {
	Format                                       int
	Directory, Endpoint, DaemonID, ContainerID   string
	AuthorityID                                  string
	Mounts                                       []EnvironmentMount
	Incarnation                                  uint64
	Helper, HelperDigest, User, Home, Path, Init string
	AdmissionEpoch                               string
}

// EnvironmentMount is immutable Docker mount metadata, retained so a resumed
// executor can reject an exposed journal before trusting local receipt files.
// Bind sources are canonical host paths, verified on observation; rebuilding
// stored configuration uses this snapshot without filesystem access.
type EnvironmentMount struct {
	Type, Source, Target string
	Writable             bool
}

func mountProjection(c preparedContainer) []EnvironmentMount {
	out := make([]EnvironmentMount, 0, len(c.Mounts))
	for _, m := range c.Mounts {
		out = append(out, EnvironmentMount{m.Type, m.Source, m.Destination, m.RW})
	}
	return out
}

type environmentManifest struct {
	Format, HelperProtocol int
	Helper, Digest         string
	AuthorityID            string
}
type environmentState struct {
	Format                               int
	Gate                                 string
	Generation, StartGeneration          uint64
	Active                               string
	Requests                             []string
	Pending                              string
	LastStop                             string
	AuthorityID, ActiveSpec, ActiveState string
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
		_, _, err := readEnvironment(dir)
		return err
	}
	if err = emptyEnvironmentInitialization(dir); err != nil {
		return err
	}
	var identity [32]byte
	if _, err = rand.Read(identity[:]); err != nil {
		return err
	}
	authorityID := hex.EncodeToString(identity[:])

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
	manifest := environmentManifest{Format: environmentFormat, HelperProtocol: jobrunner.Protocol, Helper: filepath.ToSlash(relative), Digest: digest, AuthorityID: authorityID}
	if err = saveJSON(filepath.Join(dir, "authority", "state.json"), environmentState{Format: environmentFormat, Gate: "unattached", AuthorityID: authorityID}, false); err != nil {
		return err
	}
	for _, dependency := range []string{filepath.Dir(filepath.Dir(dest)), filepath.Join(dir, "public"), dir} {
		if err = syncDir(dependency); err != nil {
			return err
		}
	}
	return saveJSON(filepath.Join(dir, "authority", "manifest.json"), manifest, true)
}
func validateManifest(m environmentManifest) error {
	if !canonicalDigest(m.AuthorityID) || m.Format != environmentFormat || m.HelperProtocol != jobrunner.Protocol || m.Helper != "bin/"+m.Digest+"/canti-job" || !canonicalDigest(m.Digest) {
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
	m, _, err := readEnvironment(dir)
	if err != nil {
		return a, err
	}
	if m.AuthorityID != a.AuthorityID {
		return a, errors.New("docker: attachment authority identity changed")
	}
	return a, a.validate()
}
func (a Attachment) validate() error {
	u, err := url.Parse(a.Endpoint)
	if err != nil || u.Scheme != "unix" || u.Host != "" || u.User != nil || !filepath.IsAbs(u.Path) || u.RawQuery != "" || u.Fragment != "" || !canonicalDigest(a.AuthorityID) || a.Format != environmentFormat || !canonicalDigest(a.HelperDigest) || !canonicalDigest(a.ContainerID) || filepath.Clean(a.Directory) != a.Directory || !filepath.IsAbs(a.Directory) || a.DaemonID == "" || len(a.ContainerID) != 64 || a.Incarnation == 0 || a.User == "" || !filepath.IsAbs(a.Home) || filepath.Clean(a.Home) != a.Home || a.Path == "" || a.Helper != "/canti/bin/"+a.HelperDigest+"/canti-job" {
		return errors.New("docker: invalid pinned environment attachment")
	}
	return validatePreparedMounts(a.Directory, a.Mounts)
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
	if parseErr != nil || u.Scheme != "unix" || u.Host != "" || u.User != nil || !filepath.IsAbs(u.Path) || u.RawQuery != "" || u.Fragment != "" {
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
	if err = checkPrepared(dir, m.AuthorityID, c); err != nil {
		return Attachment{}, err
	}
	if err = x.solePreparedContainer(ctx, dir, c.ID); err != nil {
		return Attachment{}, err
	}
	a := Attachment{Mounts: mountProjection(c), Format: environmentFormat, AuthorityID: m.AuthorityID, Directory: dir, Endpoint: endpoint, DaemonID: backend, ContainerID: c.ID, Incarnation: 1, Helper: "/canti/" + m.Helper, HelperDigest: m.Digest, User: c.Config.User, Init: init, AdmissionEpoch: c.State.StartedAt.Format(time.RFC3339Nano)}
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
	if err = x.spoolChallenge(ctx, a); err != nil {
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
func checkPrepared(dir, authorityID string, c preparedContainer) error {
	if c.Config.Labels[authorityLabel] != authorityID || c.Config.Labels[controlLabel] != dir || !c.State.Running || c.State.Paused || c.State.Restarting || c.HostConfig.AutoRemove || c.HostConfig.RestartPolicy.Name != "" && c.HostConfig.RestartPolicy.Name != "no" {
		return errors.New("docker: prepared environment needs matching control label, running state, no auto-remove and no automatic restart")
	}
	return checkEnvironmentMounts(dir, mountProjection(c))
}
func validatePreparedMounts(dir string, mounts []EnvironmentMount) error {
	public, spool := false, false
	for _, m := range mounts {
		if m.Type == "bind" && (!filepath.IsAbs(m.Source) || filepath.Clean(m.Source) != m.Source) {
			return errors.New("docker: bind sources must be clean absolute host paths")
		}
		switch m.Target {
		case helperRoot:
			public = m.Type == "bind" && m.Source == filepath.Join(dir, "public") && !m.Writable
		case spoolRoot:
			spool = m.Type == "bind" && m.Source == filepath.Join(dir, "spool") && m.Writable
		default:
			if reservedMountOverlap(m.Target) {
				return errors.New("docker: environment mount shadows reserved helper or spool paths")
			}
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

// checkEnvironmentMounts checks live bind sources separately from the pure
// snapshot validator. Docker may retain a symlink spelling in Mounts.Source.
func checkEnvironmentMounts(dir string, mounts []EnvironmentMount) error {
	if err := validatePreparedMounts(dir, mounts); err != nil {
		return err
	}
	for _, m := range mounts {
		if m.Type != "bind" {
			continue
		}
		canonical, err := filepath.EvalSymlinks(m.Source)
		if err != nil {
			return fmt.Errorf("docker: cannot verify local bind source %q: %w", m.Source, err)
		}
		if canonical != m.Source {
			return fmt.Errorf("docker: bind source %q uses a symlink; recreate the prepared container with canonical host paths", m.Source)
		}
	}
	return nil
}
func reservedMountOverlap(target string) bool {
	return within(helperRoot, target) || within(target, helperRoot) || within(spoolRoot, target) || within(target, spoolRoot)
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
	if s.AuthorityID != m.AuthorityID {
		return m, s, errors.New("docker: environment state has another authority identity")
	}
	if len(s.Requests) > 64 {
		return m, s, errors.New("docker: too many unsettled request references")
	}
	seen := map[string]bool{}
	for _, ref := range s.Requests {
		if len(ref) != 32 || strings.Trim(ref, "0123456789abcdef") != "" || seen[ref] {
			return m, s, errors.New("docker: invalid unsettled request references")
		}
		seen[ref] = true
	}
	if s.Active == "" && (s.ActiveSpec != "" || s.ActiveState != "") || s.Active != "" && (!canonicalDigest(s.ActiveSpec) || !filepath.IsAbs(s.ActiveState)) {
		return m, s, errors.New("docker: incomplete active command origin")
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

// spoolChallenge proves, before an attachment is committed, that its pinned
// user can create evidence in a command directory prepared as dispatch prepares
// one: private to the host user that owns the spool. A container user whose
// UID differs, or is remapped by a user namespace, is refused here instead of
// failing every command before its completion can be written.
func (x *Executor) spoolChallenge(ctx context.Context, a Attachment) error {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	name := ".attach-" + hex.EncodeToString(nonce[:])
	dir := filepath.Join(a.Directory, "spool", name)
	if err := os.Mkdir(dir, 0700); err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	request := execRequest{User: a.User, WorkingDir: "/", Cmd: []string{"/bin/sh", "-c", `: > "$1"`, "canti-attach", spoolRoot + "/" + name + "/probe"}, Env: commandEnv(a)}
	var created struct{ ID string }
	if err := x.api(ctx, "exec-create", http.MethodPost, "/containers/"+a.ContainerID+"/exec", request, decodeResponse(&created)); err != nil {
		return err
	}
	if len(created.ID) != 64 {
		return errors.New("docker: exec creation returned an invalid physical ID")
	}
	if err := x.api(ctx, "start", http.MethodPost, "/exec/"+created.ID+"/start", struct{ Detach, Tty bool }{Detach: true}, nil); err != nil {
		return err
	}
	call, cancel := context.WithTimeout(ctx, x.timeouts.call)
	defer cancel()
	for {
		var m execMetadata
		if err := x.api(call, "exec-inspect", http.MethodGet, "/exec/"+created.ID+"/json", nil, decodeResponse(&m)); err != nil {
			return err
		}
		if !m.Running && m.ExitCode != nil {
			probe, err := os.Lstat(filepath.Join(dir, "probe"))
			if *m.ExitCode != 0 || err != nil || !probe.Mode().IsRegular() {
				return fmt.Errorf("docker: environment user %q cannot write the host-owned command spool; run the prepared container as host UID %d, or as root without user-namespace remapping", a.User, os.Getuid())
			}
			return nil
		}
		select {
		case <-call.Done():
			return fmt.Errorf("docker: spool write challenge did not finish within %v", x.timeouts.call)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// EnvironmentWorkdir validates a read-only admission snapshot and returns the
// unique writable workspace bind mapping. It never recovers control state,
// acquires the foreground slot, or creates the run state directory. Reserving
// the slot rechecks gate, readiness and mapping because admission reserves nothing.
func EnvironmentWorkdir(ctx context.Context, a Attachment, workspace, state, requested string) (string, error) {
	cwd, c, err := observedEnvironmentWorkdir(ctx, a, workspace, state, requested)
	if err != nil {
		return "", err
	}
	_, control, err := readEnvironment(a.Directory)
	if err != nil {
		return "", err
	}
	if control.Gate != "open" {
		return "", gateError(control.Gate)
	}
	if control.Pending != "" {
		return "", gateError("quarantined")
	}
	// Check exact acknowledgements in memory; read-only admission must not prune
	// their durable references or reopen a closed/quarantined gate.
	owner := &Attached{cfg: AttachedConfig{Attachment: a}}
	if err = owner.pruneRequests(&control); err != nil {
		return "", err
	}
	if len(control.Requests) > 0 {
		return "", gateError("quarantined")
	}
	if !c.State.Running || c.State.Paused || c.State.Restarting {
		return "", errors.New("docker: environment must be running and ready before admission; inspect or start it explicitly")
	}
	return cwd, nil
}

// observedEnvironmentWorkdir is the admission check of a new configuration. It
// resolves the workspace and prospective State on the host before observing
// the attachment, so a missing workspace or a State alias into a mount is
// refused before the canonical paths are committed with the run.
func observedEnvironmentWorkdir(ctx context.Context, a Attachment, workspace, state, requested string) (string, preparedContainer, error) {
	state, err := ResolveState(workspace, state)
	if err != nil {
		return "", preparedContainer{}, err
	}
	c, err := environmentEngine(a.Endpoint).checkAttachment(ctx, a)
	if err != nil {
		return "", c, err
	}
	cwd, err := observedMapping(c, workspace, state, requested)
	return cwd, c, err
}

// observedMapping checks the run's mapping against the mounts of a container
// already observed in this operation. It is lexical over canonical paths, so
// observing and cancelling bound work keep working, even in a stopped or
// quarantined environment, after its command removes its own workspace.
func observedMapping(c preparedContainer, workspace, state, requested string) (string, error) {
	return pinnedEnvironmentWorkdir(Attachment{Mounts: mountProjection(c)}, workspace, state, requested)
}
func pinnedEnvironmentWorkdir(a Attachment, workspace, state, requested string) (string, error) {
	var candidates []string
	for _, m := range a.Mounts {
		if m.Source != "" && (within(m.Source, state) || within(state, m.Source)) {
			return "", errors.New("docker: environment mount exposes run state")
		}
		if m.Type != "bind" {
			continue
		}
		if within(m.Source, state) || within(state, m.Source) {
			return "", errors.New("docker: environment mount exposes run state")
		}
		if m.Writable && within(m.Source, workspace) {
			rel, _ := filepath.Rel(m.Source, workspace)
			candidates = append(candidates, filepath.Join(m.Target, rel))
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

// checkAttachment observes, once per operation, that a still names the current
// attachment, daemon and labelled container with verified bind sources, and
// returns that container observation for the operation's later checks.
func (x *Executor) checkAttachment(ctx context.Context, a Attachment) (preparedContainer, error) {
	if err := a.validate(); err != nil {
		return preparedContainer{}, err
	}
	current, err := ReadAttachment(a.Directory)
	if err != nil {
		return preparedContainer{}, err
	}
	if current.AuthorityID != a.AuthorityID || current.ContainerID != a.ContainerID || current.DaemonID != a.DaemonID || current.Incarnation != a.Incarnation || current.Endpoint != a.Endpoint {
		return preparedContainer{}, fmt.Errorf("%w: environment attachment changed", harness.ErrJobLost)
	}
	id, err := x.daemon(ctx)
	if err != nil {
		return preparedContainer{}, err
	}
	if id != a.DaemonID {
		return preparedContainer{}, fmt.Errorf("%w: Docker daemon identity changed", harness.ErrJobLost)
	}
	c, err := x.inspectPrepared(ctx, a.ContainerID)
	if err != nil {
		return c, err
	}
	if c.Config.Labels[authorityLabel] != a.AuthorityID || c.ID != a.ContainerID || c.Config.Labels[controlLabel] != a.Directory {
		return c, fmt.Errorf("%w: environment identity changed", harness.ErrJobLost)
	}
	return c, checkEnvironmentMounts(a.Directory, mountProjection(c))
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
	if old.AuthorityID != a.AuthorityID {
		return a, false, errors.New("docker: stored attachment belongs to another authority")
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

func emptyEnvironmentInitialization(dir string) error {
	entries, err := os.ReadDir(filepath.Join(dir, "authority"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == "lock" {
			continue
		}
		if entry.Name() == "jobs" && entry.IsDir() {
			children, err := os.ReadDir(filepath.Join(dir, "authority", "jobs"))
			if err == nil && len(children) == 0 {
				continue
			}
		}
		return errors.New("docker: incomplete or damaged authority cannot be reset; initialize a new attachment directory and provision a new container")
	}
	for _, name := range []string{"public", "spool"} {
		entries, err := os.ReadDir(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			return errors.New("docker: retained environment data prevents fresh authority initialization")
		}
	}
	return nil
}

// EnvironmentIdentity is the initialization token provisioning must place in
// the container's immutable io.canti.authority label. It is not a filesystem hash.
func EnvironmentIdentity(directory string) (string, error) {
	dir, err := resolve(directory)
	if err != nil {
		return "", err
	}
	m, _, err := readEnvironment(dir)
	return m.AuthorityID, err
}

func (x *Executor) solePreparedContainer(ctx context.Context, directory, id string) error {
	filters, _ := marshal(map[string][]string{"label": {controlLabel + "=" + directory}})
	var containers []struct{ ID string }
	err := x.api(ctx, "environment-claims", http.MethodGet, "/containers/json?all=true&filters="+url.QueryEscape(string(filters)), nil, decodeResponse(&containers))
	if err != nil {
		return err
	}
	if len(containers) != 1 || containers[0].ID != id {
		return errors.New("docker: another prepared container claims this control directory; remove the exact old container before attachment")
	}
	return nil
}
