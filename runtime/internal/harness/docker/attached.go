package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// AttachedConfig rebuilds a command executor from immutable run wiring. Its
// attachment pins identity; package installation does not change its spec.
type AttachedConfig struct {
	Attachment                Attachment
	Workspace, State, Workdir string
	OutputLimit               int64
	PreviewLimit              int
	StopGrace                 time.Duration
}

// Attached runs distinct Docker execs inside one explicitly prepared environment.
// All physical authority is outside the writable environment and run journals.
type Attached struct {
	cfg     AttachedConfig
	backend *Executor
}

var _ harness.Executor = (*Attached)(nil)

func NewAttached(c AttachedConfig) (*Attached, error) {
	c, err := prepareAttachedConfig(c)
	if err != nil {
		return nil, err
	}
	x := environmentEngine(c.Attachment.Endpoint)
	x.cfg.StopGrace = c.StopGrace
	return &Attached{cfg: c, backend: x}, nil
}
func prepareAttachedConfig(c AttachedConfig) (AttachedConfig, error) {
	if err := c.Attachment.validate(); err != nil {
		return c, err
	}
	if !filepath.IsAbs(c.State) || filepath.Clean(c.State) != c.State || !filepath.IsAbs(c.Workspace) || filepath.Clean(c.Workspace) != c.Workspace || !filepath.IsAbs(c.Workdir) || filepath.Clean(c.Workdir) != c.Workdir || c.OutputLimit < 1 || c.PreviewLimit < 64 || c.PreviewLimit > 4096 || c.StopGrace < 0 {
		return c, errors.New("docker: invalid attached execution configuration")
	}
	for _, other := range []string{c.Workspace, c.Attachment.Directory} {
		if within(other, c.State) || within(c.State, other) {
			return c, errors.New("docker: run state overlaps workspace or environment authority")
		}
	}
	if within(c.Workspace, c.Attachment.Directory) || within(c.Attachment.Directory, c.Workspace) {
		return c, errors.New("docker: workspace overlaps environment authority")
	}
	if _, err := pinnedEnvironmentWorkdir(c.Attachment, c.Workspace, c.State, c.Workdir); err != nil {
		return c, err
	}
	if c.StopGrace == 0 {
		c.StopGrace = 5 * time.Second
	}
	return c, nil
}
func attachedSpecification(c AttachedConfig, command string) string {
	data, _ := json.Marshal(struct {
		Protocol string
		Config   AttachedConfig
		Command  string
	}{"canti.docker.attach/v1", c, command})
	return harness.ConfigDigest(data)
}

func (x *Attached) Specification(command string) string { return attachedSpecification(x.cfg, command) }

func (x *Attached) checkEffect(e harness.Effect) error {
	if err := checkID(e.ID); err != nil {
		return err
	}
	if e.Spec != x.Specification(e.Command) {
		return errors.New("docker: accepted attached specification changed")
	}
	return nil
}
func (x *Attached) authority() string       { return filepath.Join(x.cfg.Attachment.Directory, "authority") }
func (x *Attached) jobDir(id string) string { return filepath.Join(x.authority(), "jobs", id) }
func (x *Attached) cachePath(id string) string {
	return filepath.Join(x.cfg.State, "jobs", id, "attached-terminal.json")
}

type attachedRecord struct {
	Format                int
	Config                AttachedConfig
	Effect                harness.Effect
	Generation            uint64
	Epoch, ExecID, Launch string
	DispatchedAt          time.Time
	Fenced                bool
	CancelIntent          string
	CancelAt              time.Time
	Terminal              *harness.Job
}

func (x *Attached) readJob(e harness.Effect) (attachedRecord, bool, error) {
	var r attachedRecord
	found, err := readJSON(filepath.Join(x.jobDir(e.ID), "command.json"), &r)
	if err != nil || !found {
		return r, found, err
	}
	validated, err := prepareAttachedConfig(r.Config)
	if err != nil {
		return r, false, err
	}
	if r.Format != 1 || r.Effect != e || attachedSpecification(validated, e.Command) != e.Spec || r.Config.Attachment.Directory != x.cfg.Attachment.Directory || r.Generation == 0 {
		return r, false, errors.New("docker: command authority contradicts accepted effect or origin")
	}
	if r.ExecID != "" && !canonicalDigest(r.ExecID) {
		return r, false, errors.New("docker: invalid attached exec identity")
	}
	if r.Terminal != nil {
		if err := validateAttachedTerminal(r); err != nil {
			return r, false, err
		}
	}
	if (r.Launch == "") != r.DispatchedAt.IsZero() {
		return r, false, errors.New("docker: incomplete command launch latch")
	}
	if r.ExecID != "" && r.Epoch == "" || !r.DispatchedAt.IsZero() && (r.ExecID == "" || r.Launch == "") {
		return r, false, errors.New("docker: invalid attached command publication chain")
	}
	return r, true, nil
}
func (x *Attached) saveJob(r attachedRecord) error {
	dir := x.jobDir(r.Effect.ID)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if err := saveJSON(filepath.Join(dir, "command.json"), r, false); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dir))
}
func (x *Attached) cached(e harness.Effect) (harness.Job, bool, error) {
	var r attachedRecord
	found, err := readJSON(x.cachePath(e.ID), &r)
	if err != nil || !found {
		return harness.Job{}, found, err
	}
	if r.Format != 1 || r.Effect != e || r.Terminal == nil || !terminal(r.Terminal.Status) || r.Config.State != x.cfg.State || r.Config.Attachment.ContainerID != x.cfg.Attachment.ContainerID || r.Config.Attachment.Incarnation != x.cfg.Attachment.Incarnation {
		return harness.Job{}, false, errors.New("docker: retained attached receipt contradicts its acceptance")
	}
	if err := validateAttachedTerminal(r); err != nil {
		return harness.Job{}, false, err
	}
	if err := syncDir(filepath.Dir(x.cachePath(e.ID))); err != nil {
		return harness.Job{}, false, err
	}
	return *r.Terminal, true, nil
}
func (x *Attached) mirror(r attachedRecord) error {
	if err := validateAttachedTerminal(r); err != nil {
		return err
	}
	path := x.cachePath(r.Effect.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	if prior, done, err := x.cached(r.Effect); err != nil || done {
		if err != nil {
			return err
		}
		before, _ := json.Marshal(prior)
		after, _ := json.Marshal(r.Terminal)
		if string(before) != string(after) {
			return errors.New("docker: conflicting retained terminal receipt")
		}
		return nil
	}
	if err := saveJSON(path, r, true); err != nil {
		return err
	}
	return syncDir(filepath.Dir(filepath.Dir(path)))
}
func (x *Attached) locked(ctx context.Context) (func(), environmentState, error) {
	_, s, err := readEnvironment(x.cfg.Attachment.Directory)
	if err != nil {
		return nil, s, err
	}
	call, cancel := context.WithTimeout(ctx, x.backend.timeouts.call)
	defer cancel()
	unlock, err := flock(call, filepath.Join(x.authority(), "lock"))
	if err != nil {
		return nil, s, err
	}
	_, s, err = readEnvironment(x.cfg.Attachment.Directory)
	if err == nil && s.AuthorityID != x.cfg.Attachment.AuthorityID {
		err = fmt.Errorf("%w: environment authority was replaced", harness.ErrJobLost)
	}
	if err != nil {
		unlock()
		return nil, s, err
	}
	return unlock, s, nil
}

func (x *Attached) Drive(ctx context.Context, e harness.Effect) (harness.Job, error) {
	return x.operate(ctx, e, "drive")
}
func (x *Attached) Inspect(ctx context.Context, e harness.Effect) (harness.Job, error) {
	return x.operate(ctx, e, "inspect")
}
func (x *Attached) Cancel(ctx context.Context, e harness.Effect) (harness.Job, error) {
	return x.operate(ctx, e, "cancel")
}

func (x *Attached) operate(ctx context.Context, e harness.Effect, op string) (harness.Job, error) {
	if err := x.checkEffect(e); err != nil {
		return harness.Job{}, err
	}
	if job, done, err := x.cached(e); err != nil || done {
		return job, err
	}
	unlock, s, err := x.locked(ctx)
	if err != nil {
		return harness.Job{}, err
	}
	defer unlock()
	r, found, err := x.readJob(e)
	if err != nil {
		return harness.Job{}, err
	}
	if job, done, loadedErr := x.loaded(e, r, found, &s, op); loadedErr != nil || done {
		return job, loadedErr
	}

	if err = x.prepareSlot(ctx, e, &s); err != nil {
		return harness.Job{}, err
	}

	if !found {
		var done bool
		var job harness.Job
		r, job, done, err = x.unbound(ctx, e, op, &s)
		if err != nil || done {
			return job, err
		}
	}

	if s.Active == "" {
		if s.Gate != "open" && op == "drive" {
			return harness.Job{}, gateError(s.Gate)
		}
		s.Active = e.ID
		s.ActiveSpec, s.ActiveState = e.Spec, x.cfg.State
		if err = saveEnvironmentState(x.cfg.Attachment.Directory, s); err != nil {
			return harness.Job{}, err
		}
	}
	if op == "cancel" {
		return x.cancel(ctx, &s, &r)
	}
	if op == "drive" && r.DispatchedAt.IsZero() && !r.Fenced {
		return x.dispatch(ctx, &s, &r)
	}
	return x.sample(ctx, &s, &r)
}
func gateError(gate string) error {
	return fmt.Errorf("docker: environment gate is %s; inspect it; quarantined work requires explicit retire/replacement with a new container ID", gate)
}
func (x *Attached) cancelAbsent(e harness.Effect) (harness.Job, error) {
	j := harness.Job{EffectID: e.ID, Spec: e.Spec, Status: harness.JobCancelled, Fenced: true}
	r := attachedRecord{Format: 1, Config: x.cfg, Effect: e, Fenced: true, Generation: 1, Terminal: &j}
	if err := x.saveJob(r); err != nil {
		return harness.Job{}, err
	}
	return j, x.mirror(r)
}
func (x *Attached) reserve(ctx context.Context, e harness.Effect, s *environmentState) (attachedRecord, error) {
	c, err := x.backend.inspectPrepared(ctx, x.cfg.Attachment.ContainerID)
	if err != nil {
		return attachedRecord{}, err
	}
	if !c.State.Running || c.State.Paused || c.State.Restarting {
		return attachedRecord{}, errors.New("docker: prepared environment is stopped; start it explicitly")
	}
	s.Generation++
	if s.Generation == 0 {
		return attachedRecord{}, errors.New("docker: environment command generation exhausted")
	}
	if err = saveEnvironmentState(x.cfg.Attachment.Directory, *s); err != nil {
		return attachedRecord{}, err
	}
	r := attachedRecord{Format: 1, Config: x.cfg, Effect: e, Generation: s.Generation, Epoch: c.State.StartedAt.Format(time.RFC3339Nano)}
	if err = x.saveJob(r); err != nil {
		return r, err
	}
	s.Active = e.ID
	s.ActiveSpec, s.ActiveState = e.Spec, x.cfg.State
	return r, saveEnvironmentState(x.cfg.Attachment.Directory, *s)
}
func (x *Attached) activeOwner(ctx context.Context, s environmentState) (*Attached, attachedRecord, error) {
	var r attachedRecord
	found, err := readJSON(filepath.Join(x.jobDir(s.Active), "command.json"), &r)
	if err != nil || !found {
		return nil, r, errors.Join(err, errors.New("docker: missing active command authority"))
	}
	if r.Effect.ID != s.Active || r.Effect.Spec != s.ActiveSpec || r.Config.State != s.ActiveState || r.Config.Attachment.AuthorityID != x.cfg.Attachment.AuthorityID || attachedSpecification(r.Config, r.Effect.Command) != s.ActiveSpec {
		return nil, r, errors.New("docker: active command origin contradicts its authority slot")
	}
	owner, err := NewAttached(r.Config)
	if err != nil {
		return nil, r, err
	}
	r, found, err = owner.readJob(r.Effect)
	if err != nil || !found {
		return nil, r, errors.Join(err, errors.New("docker: invalid active command authority"))
	}
	if _, _, err = observedEnvironmentWorkdir(ctx, r.Config.Attachment, r.Config.Workspace, r.Config.State, r.Config.Workdir); err != nil {
		return nil, r, err
	}
	return owner, r, nil
}
func (x *Attached) reconcileActive(ctx context.Context, s *environmentState) error {
	previous, r, err := x.activeOwner(ctx, *s)
	if err != nil {
		return err
	}
	if r.Terminal != nil {
		return previous.releaseSlot(s, r)
	}
	_, err = previous.sample(ctx, s, &r)
	return err
}
func (x *Attached) releaseSlot(s *environmentState, r attachedRecord) error {
	if r.Terminal == nil {
		return errors.New("docker: cannot release a command without its durable terminal receipt")
	}
	if err := x.mirror(r); err != nil {
		return err
	}
	if s.Active == r.Effect.ID {
		s.Active = ""
		s.ActiveSpec, s.ActiveState = "", ""
	}
	if err := x.pruneRequests(s); err != nil {
		return err
	}
	return saveEnvironmentState(x.cfg.Attachment.Directory, *s)
}

func (x *Attached) unbound(ctx context.Context, e harness.Effect, op string, s *environmentState) (attachedRecord, harness.Job, bool, error) {
	switch op {
	case "cancel":
		j, err := x.cancelAbsent(e)
		return attachedRecord{}, j, true, err
	case "inspect":
		return attachedRecord{}, harness.Job{EffectID: e.ID, Spec: e.Spec, Status: harness.JobAbsent}, true, nil
	}
	if s.Gate != "open" || len(s.Requests) != 0 {
		return attachedRecord{}, harness.Job{}, false, gateError(s.Gate)
	}
	r, err := x.reserve(ctx, e, s)
	return r, harness.Job{}, false, err
}

func validateAttachedTerminal(r attachedRecord) error {
	j := r.Terminal
	if j == nil || !terminal(j.Status) || j.ID != r.ExecID || j.Fenced != r.Fenced || !j.DispatchedAt.Equal(r.DispatchedAt) {
		return errors.New("docker: terminal receipt contradicts command authority")
	}
	if j.ID != "" {
		a := r.Config.Attachment
		expected := harness.EnvironmentBinding{AuthorityID: a.AuthorityID, BackendID: a.DaemonID, EnvironmentID: a.ContainerID, StartEpoch: r.Epoch, Incarnation: a.Incarnation, Generation: r.Generation}
		if j.Binding == nil || *j.Binding != expected {
			return errors.New("docker: terminal receipt has a different environment binding")
		}
	}
	if (j.Status == harness.JobSucceeded || j.Status == harness.JobFailed) && (r.Launch == "" || r.DispatchedAt.IsZero()) {
		return errors.New("docker: terminal execution lacks its launch latch")
	}
	cfg := r.Config
	if _, err := prepareAttachedConfig(cfg); err != nil {
		return err
	}
	if attachedSpecification(cfg, r.Effect.Command) != r.Effect.Spec {
		return errors.New("docker: retained receipt belongs to another configuration")
	}
	return j.Validate(r.Effect)
}

func (x *Attached) prepareSlot(ctx context.Context, e harness.Effect, s *environmentState) error {
	var err error
	if _, _, err = observedEnvironmentWorkdir(ctx, x.cfg.Attachment, x.cfg.Workspace, x.cfg.State, x.cfg.Workdir); err != nil {
		return err
	}
	if err = x.backend.checkAttachment(ctx, x.cfg.Attachment); err != nil {
		return err
	}
	if err = x.recoverControl(ctx, s); err != nil {
		return err
	}
	if s.Gate == "detached" || s.Gate == "retiring" || s.Gate == "retired" {
		return fmt.Errorf("%w: environment attachment is %s", harness.ErrJobLost, s.Gate)
	}
	if s.Active != "" && s.Active != e.ID {
		if err = x.reconcileActive(ctx, s); err != nil {
			return err
		}
		if s.Active != "" {
			return errors.New("docker: environment foreground command is busy; inspect or stop its owning run")
		}
	}
	return nil
}

func (x *Attached) loaded(e harness.Effect, r attachedRecord, found bool, s *environmentState, op string) (harness.Job, bool, error) {
	ref, err := x.readReference(e)
	if found && err == nil {
		err = x.validateLocalBinding(r)
	}
	if err != nil {
		return harness.Job{}, false, err
	}
	if !found && (ref || s.Active == e.ID) {
		return harness.Job{}, false, fmt.Errorf("%w: accepted command authority is missing", harness.ErrJobLost)
	}
	if !found && op == "cancel" {
		current, err := ReadAttachment(x.cfg.Attachment.Directory)
		if err != nil {
			return harness.Job{}, false, err
		}
		if !x.cfg.Attachment.sameIdentity(current) {
			return harness.Job{}, false, fmt.Errorf("%w: attachment changed", harness.ErrJobLost)
		}
		j, err := x.cancelAbsent(e)
		return j, true, err
	}
	if found && s.Active == e.ID && (s.ActiveSpec != e.Spec || s.ActiveState != r.Config.State) {
		return harness.Job{}, false, errors.New("docker: active slot contradicts accepted command origin")
	}
	if found && r.Terminal != nil {
		if s.Active == e.ID {
			return *r.Terminal, true, x.releaseSlot(s, r)
		}
		return *r.Terminal, true, x.mirror(r)
	}
	return harness.Job{}, false, nil
}
