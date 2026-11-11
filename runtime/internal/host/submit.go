package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// SpecError reports a submission whose task or configuration no run can be
// created from; its message is the reason.
type SpecError struct{ Err error }

func (e SpecError) Error() string { return e.Err.Error() }
func (e SpecError) Unwrap() error { return e.Err }

// ErrConflict reports an input ID that already names an input of other content.
var ErrConflict = errors.New("input ID already exists with different content")

// ErrHeld reports a principal input refused while a stopped run holds inputs.
var ErrHeld = errors.New("held inputs must be withdrawn first")

// Submission is one input for the agent: a principal instruction or an
// observation. An InputID makes it retryable: the same input always names the
// input first admitted for it. Delivery steers the open run or queues a
// follow-up; it is ignored when the agent is quiescent. Fresh starts a new
// line. A non-fresh principal input inherits the latest run's constraints
// unless ExplicitConstraints is set.
type Submission struct {
	InputID             string
	Goal, Constraints   string
	ExplicitConstraints bool
	Delivery            harness.InputDelivery
	Fresh               bool
	Spec                Spec
	// Binding names the provider the run stores, and its keys, when it has
	// keys for the run's API, are those that admission checks the run can be
	// driven with; without them, a credential the host holds for that API and
	// provider must exist. Admission stores no key.
	Binding Binding
	Now     time.Time
}

// Observation is evidence one source reports about one subject. It never
// starts or wakes a run, and it has no disposition.
type Observation struct {
	InputID         string
	Source, Subject string
	Text            string
	Now             time.Time
}

// Ack acknowledges an admitted input. Run is set when the input opened a run,
// now or earlier; Replayed reports the answer of the input's first admission,
// and Differs names, for a replay, the settings in which the retried
// submission differs from what was first admitted, which keeps its own.
type Ack struct {
	Run      harness.Run
	Config   RunConfig
	Input    harness.Input
	Snapshot harness.Snapshot
	Replayed bool
	Differs  []string
}

// Opened reports whether the acknowledged input is the goal of a run.
func (a Ack) Opened() bool { return a.Run.ID != "" }

// Submit admits one input. Under the host's admission lock it answers a retry,
// prepares the input the branch the latest run selects, and commits the input
// and, when it opens a run, the run in one transaction. Admission drives
// nothing: a steer or queued follow-up only acknowledges.
func (h *Host) Submit(ctx context.Context, s Submission) (Ack, error) {
	ctx, leave, err := h.Enter(ctx)
	if err != nil {
		return Ack{}, err
	}
	defer leave()
	if err := h.acquire(ctx, h.admission); err != nil {
		return Ack{}, err
	}
	defer h.releaseAdmission()
	if dir := s.Spec.Config.HistoryImports; dir != "" {
		if !filepath.IsAbs(dir) {
			return Ack{}, SpecError{fmt.Errorf("history imports %q must be an absolute directory", dir)}
		}
		if canonical, err := filepath.EvalSymlinks(dir); err == nil {
			s.Spec.Config.HistoryImports = canonical
		}
	}
	if s.InputID != "" {
		if previous, err := h.store.Input(ctx, s.InputID); err == nil {
			if !sameInputContent(previous, s) {
				return Ack{}, fmt.Errorf("%w: %s", ErrConflict, s.InputID)
			}
			return h.replay(ctx, s, previous)
		} else if !errors.Is(err, harness.ErrNotFound) {
			return Ack{}, err
		}
	}
	for range 3 {
		ack, err := h.admit(ctx, s)
		if errors.Is(err, harness.ErrAdmissionRace) {
			continue
		}
		return ack, err
	}
	return Ack{}, fmt.Errorf("the latest run changed during every attempt to prepare input; retry")
}

// admit prepares the input for the branch the latest run selects and commits
// it. A branch that changed after the read returns harness.ErrAdmissionRace.
func (h *Host) admit(ctx context.Context, s Submission) (Ack, error) {
	latest, snap, _, err := h.latest(ctx)
	if err != nil {
		return Ack{}, err
	}
	if latest != nil {
		if _, err := DecodeConfig(*latest, h.opts.Workspace); err != nil {
			return Ack{}, err
		}
	}
	open := latest == nil || snap.State.Closed()
	if open && latest != nil && !s.ExplicitConstraints && !s.Fresh {
		s.Constraints = latest.Constraints
	}
	var sub harness.InputSubmission
	if open {
		if latest != nil {
			if err := h.Closed(ctx); err != nil {
				return Ack{}, err
			}
		}
		sub, err = h.openingSubmission(ctx, s)
	} else {
		sub, err = h.workingSubmission(ctx, s)
	}
	if err != nil {
		return Ack{}, err
	}
	h.checkpoint(PointAdmitting)
	// A queued follow-up's binding and its row become visible together: the
	// start that opens the row takes this lock before it reads the queued
	// input or its binding.
	h.binding.Lock()
	admission, err := h.store.Admit(ctx, sub)
	if err == nil && admission.Disposition == harness.DispositionQueued {
		h.rememberBinding(admission.Input.Pos, s.Binding)
	}
	h.binding.Unlock()
	switch {
	case errors.Is(err, harness.ErrInputConflict):
		return Ack{}, fmt.Errorf("%w: %s", ErrConflict, s.InputID)
	case errors.Is(err, harness.ErrHeld):
		return Ack{}, fmt.Errorf("%w; withdraw the held inputs first", ErrHeld)
	case errors.Is(err, harness.ErrDeliveryRequired):
		return Ack{}, SpecError{fmt.Errorf("the agent is working: run with -delivery steer to steer it or -delivery follow_up to queue the input; run without -delivery is accepted only when the agent is quiescent")}
	case errors.Is(err, harness.ErrInputSpec):
		return Ack{}, SpecError{err}
	case err != nil:
		return Ack{}, err
	}
	h.checkpoint(PointSubmitted)
	ack := Ack{Input: admission.Input, Replayed: admission.Replayed, Snapshot: snap}
	if admission.Run != nil {
		run := *admission.Run
		stored, err := DecodeConfig(run, h.opts.Workspace)
		if err != nil {
			return Ack{}, err
		}
		ack.Run, ack.Config = run, stored
		ack.Snapshot = harness.Snapshot{State: sub.Initial}
		h.forgetBinding(admission.Input.Pos)
		h.opts.Logger.Info("opened run", "run_id", run.ID, "input_id", run.InputID, "input_pos", run.InputPos, "fresh", run.Fresh)
		return ack, nil
	}
	switch admission.Disposition {
	case harness.DispositionSteer:
		h.opts.Logger.Info("steered run", "run_id", admission.Input.RunID, "input_id", admission.Input.InputID, "input_pos", admission.Input.Pos)
	case harness.DispositionQueued:
		h.opts.Logger.Info("queued follow-up", "input_id", admission.Input.InputID, "input_pos", admission.Input.Pos)
	}
	return ack, nil
}

// openingSubmission prepares the run an input opens when the agent is
// quiescent: its resolved execution, provider, budget, and limits, and the
// check that a credential can drive it.
func (h *Host) openingSubmission(ctx context.Context, s Submission) (harness.InputSubmission, error) {
	spec := s.Spec
	resolved, err := ResolveExecution(ctx, spec.Config, h.opts.State)
	if err != nil {
		return harness.InputSubmission{}, SpecError{err}
	}
	spec.Config = resolved
	p, err := newProvider(spec.Config.API, s.Binding)
	if err != nil {
		return harness.InputSubmission{}, err
	}
	spec.Config.Provider = p
	run, initial, err := NewRun(s.Goal, s.Constraints, spec, s.Now)
	if err != nil {
		return harness.InputSubmission{}, SpecError{err}
	}
	run.Fresh = s.Fresh
	if err := h.drivable(spec.Config, p, s.Binding); err != nil {
		return harness.InputSubmission{}, err
	}
	data, err := json.Marshal(spec)
	if err != nil {
		return harness.InputSubmission{}, err
	}
	return harness.InputSubmission{InputID: s.InputID, Authority: harness.AuthorityPrincipal, Delivery: s.Delivery,
		Text: s.Goal, Constraints: s.Constraints, ExplicitConstraints: s.ExplicitConstraints, Fresh: s.Fresh,
		Spec: data, Run: run, Initial: initial, Now: s.Now}, nil
}

// workingSubmission prepares a steer or follow-up while the agent works. It
// resolves no execution and checks no credential: a follow-up stores the run
// spec it will open with, which the follow-up start resolves and checks when
// it opens the run.
func (h *Host) workingSubmission(ctx context.Context, s Submission) (harness.InputSubmission, error) {
	if s.Delivery == "" {
		return harness.InputSubmission{}, SpecError{fmt.Errorf("the agent is working: name a delivery")}
	}
	spec := s.Spec
	if s.Delivery == harness.DeliveryFollowUp {
		p, err := newProvider(spec.Config.API, s.Binding)
		if err != nil {
			return harness.InputSubmission{}, err
		}
		spec.Config.Provider = p
		if spec.Config.ContextVersion == 0 {
			spec.Config.ContextVersion = ContextVersion
		}
		if err := CheckExecutionMode(spec.Config); err != nil {
			return harness.InputSubmission{}, SpecError{err}
		}
		if _, _, err := NewRun(s.Goal, s.Constraints, spec, s.Now); err != nil {
			return harness.InputSubmission{}, SpecError{err}
		}
	}
	data, err := json.Marshal(spec)
	if err != nil {
		return harness.InputSubmission{}, err
	}
	return harness.InputSubmission{InputID: s.InputID, Authority: harness.AuthorityPrincipal, Delivery: s.Delivery,
		Text: s.Goal, Constraints: s.Constraints, ExplicitConstraints: s.ExplicitConstraints, Fresh: s.Fresh,
		Spec: data, Now: s.Now}, nil
}

// replay acknowledges a retry of the input that admitted previous.
func (h *Host) replay(ctx context.Context, s Submission, previous harness.Input) (Ack, error) {
	ack := Ack{Input: previous, Replayed: true}
	var storedSpec Spec
	haveSpec := len(previous.Spec) > 0
	if haveSpec {
		if err := strictSpec(previous.Spec, &storedSpec); err != nil {
			return Ack{}, err
		}
	}
	if previous.RunID != "" && previous.Disposition == harness.DispositionOpened {
		run, err := h.store.Run(ctx, previous.RunID)
		if err != nil {
			return Ack{}, err
		}
		c, err := DecodeConfig(run, h.opts.Workspace)
		if err != nil {
			return Ack{}, err
		}
		snap, err := h.store.Latest(ctx, run.ID)
		if err != nil {
			return Ack{}, err
		}
		submitted := s.Spec
		if p, err := newProvider(submitted.Config.API, s.Binding); err == nil {
			submitted.Config.Provider = p
		}
		submitted.Config.Attachment = c.Attachment
		if submitted.Config.Environment == c.Environment && submitted.Config.EnvironmentWorkdir == "" {
			submitted.Config.EnvironmentWorkdir = c.EnvironmentWorkdir
		}
		ack.Differs = fieldsDiffering("", c, submitted.Config)
		ack.Differs = append(ack.Differs, fieldsDiffering("budget.", run.Budget, s.Spec.Budget)...)
		ack.Differs = append(ack.Differs, fieldsDiffering("limits.", snap.State.Limits, s.Spec.Limits)...)
		ack.Run, ack.Config, ack.Snapshot = run, c, snap
		return ack, nil
	}
	if haveSpec {
		ack.Differs = fieldsDiffering("", storedSpec.Config, s.Spec.Config)
		ack.Differs = append(ack.Differs, fieldsDiffering("budget.", storedSpec.Budget, s.Spec.Budget)...)
		ack.Differs = append(ack.Differs, fieldsDiffering("limits.", storedSpec.Limits, s.Spec.Limits)...)
	}
	if previous.Disposition == harness.DispositionQueued {
		h.binding.Lock()
		h.rememberBinding(previous.Pos, s.Binding)
		h.binding.Unlock()
	}
	return ack, nil
}

func strictSpec(data []byte, s *Spec) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	return decoder.Decode(s)
}

func sameInputContent(previous harness.Input, s Submission) bool {
	return previous.Authority == harness.AuthorityPrincipal && previous.Text == s.Goal &&
		previous.Delivery == s.Delivery && previous.Fresh == s.Fresh &&
		(!s.ExplicitConstraints && !s.Fresh || previous.Constraints == s.Constraints)
}

// Observe admits an observation. It never touches a run: it appends the
// evidence with its own agent-log position and returns it.
func (h *Host) Observe(ctx context.Context, o Observation) (Ack, error) {
	ctx, leave, err := h.Enter(ctx)
	if err != nil {
		return Ack{}, err
	}
	defer leave()
	if err := h.acquire(ctx, h.admission); err != nil {
		return Ack{}, err
	}
	defer h.releaseAdmission()
	admission, err := h.store.Admit(ctx, harness.InputSubmission{InputID: o.InputID, Authority: harness.AuthorityObservation,
		Text: o.Text, Source: o.Source, Subject: o.Subject, Now: o.Now})
	if errors.Is(err, harness.ErrInputConflict) {
		return Ack{}, fmt.Errorf("%w: %s", ErrConflict, o.InputID)
	}
	if errors.Is(err, harness.ErrInputSpec) {
		return Ack{}, SpecError{err}
	}
	if err != nil {
		return Ack{}, err
	}
	return Ack{Input: admission.Input, Replayed: admission.Replayed}, nil
}

// Withdraw takes back the named queued follow-up or unseen steer of the
// stopped latest run, or with an empty input ID every such input, and returns
// them in position order.
func (h *Host) Withdraw(ctx context.Context, inputID string) ([]harness.Input, error) {
	ctx, leave, err := h.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer leave()
	if err := h.acquire(ctx, h.admission); err != nil {
		return nil, err
	}
	defer h.releaseAdmission()
	h.checkpoint(PointWithdrawing)
	withdrawn, err := h.store.Withdraw(ctx, inputID)
	switch {
	case errors.Is(err, harness.ErrInputSpec), errors.Is(err, harness.ErrNotWithdrawable):
		return nil, SpecError{err}
	case err != nil:
		return nil, err
	}
	for _, in := range withdrawn {
		h.forgetBinding(in.Pos)
	}
	return withdrawn, nil
}

// latest reads the latest run of the state directory, if any, with its
// snapshot and latest event.
func (h *Host) latest(ctx context.Context) (*harness.Run, harness.Snapshot, core.Event, error) {
	run, err := h.store.LastRun(ctx)
	if errors.Is(err, harness.ErrNotFound) {
		return nil, harness.Snapshot{}, nil, nil
	}
	if err != nil {
		return nil, harness.Snapshot{}, nil, err
	}
	snap, err := h.store.Latest(ctx, run.ID)
	if err != nil {
		return nil, harness.Snapshot{}, nil, err
	}
	last, err := h.store.LastEvent(ctx, run.ID)
	return &run, snap, last, err
}

// InheritedConstraints returns the constraints a new non-fresh principal
// input inherits: those of the latest run.
func (h *Host) InheritedConstraints(ctx context.Context) (string, error) {
	last, err := h.store.LastRun(ctx)
	if errors.Is(err, harness.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return last.Constraints, nil
}

// fieldsDiffering names the fields of the structs a and b, of one type, that
// differ, by their JSON names, or else their Go names in snake case, each
// after prefix, in field order.
func fieldsDiffering(prefix string, a, b any) []string {
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	var out []string
	for i := range va.NumField() {
		f := va.Type().Field(i)
		if reflect.DeepEqual(va.Field(i).Interface(), vb.Field(i).Interface()) {
			continue
		}
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "" || name == "-" {
			name = snake(f.Name)
		}
		out = append(out, prefix+name)
	}
	return out
}

func snake(s string) string {
	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}

// Closed fails unless the latest run of the state directory, if any, is
// closed, its driver has ended, and its step records are published: a new run
// is admitted only beside closed ones whose publication is quiescent.
func (h *Host) Closed(ctx context.Context) error {
	last, err := h.store.LastRun(ctx)
	if errors.Is(err, harness.ErrNotFound) {
		return nil
	}
	var snap harness.Snapshot
	var c RunConfig
	if err == nil {
		c, err = DecodeConfig(last, h.opts.Workspace)
	}
	if err == nil {
		snap, err = h.store.Latest(ctx, last.ID)
	}
	switch p := snap.State.Phase; {
	case err != nil:
		return err
	case snap.State.Closed():
		return h.publishClosed(ctx, last.ID)
	case p == core.PhaseNeedsResolution:
		return fmt.Errorf("run %s needs resolution: resolve it first", last.ID)
	}
	next := "resume or stop"
	if c.ContextVersion != ContextVersion {
		next = "stop"
	}
	return fmt.Errorf("run %s is %s: %s it first", last.ID, snap.State.Phase, next)
}

// publishClosed waits for the final publication of a closed run's driver
// without interrupting it. The run lock excludes a new drive, stop or resolution
// until admission's own publication finishes; the admission lock alone cannot
// exclude a driver that committed its terminal fact but is still publishing.
func (h *Host) publishClosed(ctx context.Context, id string) error {
	unlock, err := h.lockRun(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	h.mu.Lock()
	d := h.drivers[id]
	h.mu.Unlock()
	if d != nil {
		select {
		case <-d.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := h.store.CheckAdmission(ctx); err != nil {
		return err
	}
	if err := harness.PublishHistory(ctx, h.store, HistoryRoot(h.opts.State), id); err != nil {
		return fmt.Errorf("publishing the step records of run %s: %w", id, err)
	}
	return nil
}

// drivable fails unless a run of c, which stores provider p, can be driven
// with binding b: there are keys for its API and provider, from b or bound
// to the host, and the model and the executor can be built, so that no run
// is admitted that cannot be driven.
func (h *Host) drivable(c RunConfig, p string, b Binding) error {
	if b.Keys == (Keys{}) && len(h.Bindings()) == 0 {
		return ErrNoKeys
	}
	k, err := h.keysFor("", c, p, b)
	if err != nil {
		return err
	}
	if _, err := NewModel(c, p, k); err != nil {
		return err
	}
	_, err = NewExecutor(c, h.opts.State)
	return err
}
