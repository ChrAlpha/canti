package host

import (
	"context"
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

// ErrConflict reports an input ID that already names a run of other content.
var ErrConflict = errors.New("input ID already exists with different content")

// Submission is a request for a new run. An InputID makes it retryable: the
// same input, in SessionID or in no session when that is empty, always names
// the run first admitted for it. A session input inherits the constraints of
// the session's latest run unless ExplicitConstraints is set.
type Submission struct {
	SessionID, InputID  string
	Goal, Constraints   string
	ExplicitConstraints bool
	Spec                Spec
	// Binding names the provider the run stores, and its keys, when it has
	// keys for the run's API, are those that admission checks the run can be
	// driven with; without them, a credential the host holds for that API and
	// provider must exist. Admission stores no key.
	Binding Binding
	Now     time.Time
}

// Ack acknowledges a submission once its run is durably admitted, or names
// the run an earlier attempt of the same input admitted, with Replayed set
// and the snapshot it has reached. Admission drives nothing.
type Ack struct {
	Run      harness.Run
	Config   RunConfig
	Replayed bool
	// Differs names, for a replay, the settings in which the retried
	// submission differs from the run first admitted for its input, which
	// keeps its own: the stored config's fields by their JSON names, and
	// "budget.<field>" and "limits.<field>".
	Differs  []string
	Snapshot harness.Snapshot
}

// Submit admits s: under the host's admission lock it answers a retry of an
// input, checks that the latest run of the state directory is closed and its
// step records published, creates the run with the provider s names, and
// commits it in the transaction that admits it. Submit is an operation of
// the host: Shutdown awaits it, so its run is either admitted or not before
// the host releases the state directory, and a submission still waiting
// for admission when the shutdown begins is refused with ErrClosing.
func (h *Host) Submit(ctx context.Context, s Submission) (Ack, error) {
	ctx, leave, err := h.Enter(ctx)
	if err != nil {
		return Ack{}, err
	}
	defer leave()
	if err := h.acquire(ctx, h.admission); err != nil {
		return Ack{}, err
	}
	defer func() { <-h.admission }()
	if dir := s.Spec.Config.HistoryImports; dir != "" {
		// A relative directory is refused: only its client can resolve it.
		// An absolute one is compared and stored canonical; a retry is
		// answered whether or not its directory still exists, and only a
		// new run needs it to exist, which NewRun checks.
		if !filepath.IsAbs(dir) {
			return Ack{}, SpecError{fmt.Errorf("history imports %q must be an absolute directory", dir)}
		}
		if canonical, err := filepath.EvalSymlinks(dir); err == nil {
			s.Spec.Config.HistoryImports = canonical
		}
	}
	if s.InputID != "" {
		previous, err := h.store.SessionInput(ctx, s.SessionID, s.InputID)
		if err == nil {
			return h.replay(ctx, s, previous)
		}
		if !errors.Is(err, harness.ErrNotFound) {
			return Ack{}, err
		}
	}
	constraints := s.Constraints
	if s.SessionID != "" && !s.ExplicitConstraints {
		prior, err := h.store.SessionRuns(ctx, s.SessionID, 0, 1)
		if err != nil {
			return Ack{}, err
		}
		if len(prior) > 0 {
			constraints = prior[0].Constraints
		}
	}
	if err := h.Closed(ctx); err != nil {
		return Ack{}, err
	}
	spec := s.Spec
	p, err := newProvider(spec.Config.API, s.Binding)
	if err != nil {
		return Ack{}, err
	}
	spec.Config.Provider = p
	run, initial, err := NewRun(s.Goal, constraints, spec, s.Now)
	if err != nil {
		return Ack{}, SpecError{err}
	}
	if err := h.drivable(spec.Config, p, s.Binding); err != nil {
		return Ack{}, err
	}
	h.checkpoint(PointAdmitting)
	if s.InputID == "" {
		if err := h.store.CreateRun(ctx, run, initial); err != nil {
			return Ack{}, err
		}
	} else {
		run.SessionID, run.InputID = s.SessionID, s.InputID
		if run, err = h.store.Submit(ctx, run, initial); err != nil {
			return Ack{}, err
		}
		h.opts.Logger.Info("accepted input", "session_id", run.SessionID, "session_seq", run.SessionSeq, "input_id", run.InputID, "run_id", run.ID)
	}
	h.checkpoint(PointSubmitted)
	stored, err := DecodeConfig(run, h.opts.Workspace)
	if err != nil {
		return Ack{}, err
	}
	return Ack{Run: run, Config: stored, Snapshot: harness.Snapshot{State: initial}}, nil
}

// replay acknowledges s, a retry of the input that admitted previous.
func (h *Host) replay(ctx context.Context, s Submission, previous harness.Run) (Ack, error) {
	if previous.Goal != s.Goal || (s.ExplicitConstraints || s.SessionID == "") && previous.Constraints != s.Constraints {
		if s.SessionID != "" {
			return Ack{}, fmt.Errorf("session %w", ErrConflict)
		}
		return Ack{}, ErrConflict
	}
	c, err := DecodeConfig(previous, h.opts.Workspace)
	if err != nil {
		return Ack{}, err
	}
	snap, err := h.store.Latest(ctx, previous.ID)
	if err != nil {
		return Ack{}, err
	}
	spec := s.Spec
	if p, err := newProvider(spec.Config.API, s.Binding); err == nil {
		spec.Config.Provider = p
	}
	differs := fieldsDiffering("", c, spec.Config)
	differs = append(differs, fieldsDiffering("budget.", previous.Budget, spec.Budget)...)
	differs = append(differs, fieldsDiffering("limits.", snap.State.Limits, spec.Limits)...)
	return Ack{Run: previous, Config: c, Replayed: true, Differs: differs, Snapshot: snap}, nil
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
// closed and its step records are published: a new run is admitted only
// beside closed ones.
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
		if err := h.store.CheckAdmission(ctx); err != nil {
			return err
		}
		if err := harness.PublishHistory(ctx, h.store, HistoryRoot(h.opts.State), last.ID); err != nil {
			return fmt.Errorf("publishing the step records of run %s: %w", last.ID, err)
		}
		return nil
	case p == core.PhaseNeedsResolution:
		return fmt.Errorf("run %s needs resolution: resolve it first", last.ID)
	}
	next := "resume or stop"
	if c.ContextVersion != ContextVersion {
		next = "stop"
	}
	return fmt.Errorf("run %s is %s: %s it first", last.ID, snap.State.Phase, next)
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
