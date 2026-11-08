package hostrpc

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/host"
)

func ResolutionOf(job bool, obs harness.Observation, ownerID string) Resolution {
	if job {
		return Resolution{Job: true}
	}
	return Resolution{Execution: word(executionWords, obs.Execution), ExitCode: obs.ExitCode, Note: obs.Preview, OwnerID: ownerID}
}

// The words of the protocol for the enumerations of the host. Each table
// names every value; a value that it lacks fails TestEveryValueHasAWord and
// travels as "", never as the host's own spelling.
var (
	phaseWords = map[core.Phase]string{
		core.PhaseIdle: "idle", core.PhaseRequestFrozen: "request_frozen", core.PhaseRequestSent: "request_sent",
		core.PhaseResponseSaved: "response_saved", core.PhaseEffectSaved: "effect_saved",
		core.PhaseCancelling: "cancelling", core.PhaseReturned: "returned",
		core.PhaseNeedsResolution: "needs_resolution", core.PhaseStopped: "stopped",
	}
	reasonWords = map[core.StopReason]string{
		core.StopCancelled: "cancelled", core.StopTimedOut: "timed_out", core.StopBudgetExhausted: "budget_exhausted",
		core.StopUnrecoverable: "unrecoverable", core.StopStepsExhausted: "steps_exhausted", core.StopAttemptsExhausted: "attempts_exhausted",
		core.StopProtocolFailure: "protocol_failure", core.StopUnresolved: "unresolved",
	}
	executionWords = map[core.ExecutionState]string{
		core.ExecutionExited: "exited", core.ExecutionTimedOut: "timed_out", core.ExecutionCancelled: "cancelled",
		core.ExecutionFailedToStart: "failed_to_start", core.ExecutionUnknown: "unknown",
	}
	itemWords = map[host.ItemKind]string{
		host.ItemDecided: "decided", host.ItemReturned: "returned", host.ItemRetrying: "retrying", host.ItemCompleted: "completed",
		host.ItemContextReset: "context_reset", host.ItemRecovered: "effect_recovered", host.ItemStopRequested: "stop_requested", host.ItemNeedsResolution: "needs_resolution", host.ItemStopped: "stopped",
	}
	outcomeWords = map[harness.OutcomeStatus]string{
		harness.OutcomeDone: "done", harness.OutcomeFailed: "failed",
	}
	actionWords = map[harness.ActionName]string{
		harness.ActionCommand: "command", harness.ActionRetrieve: "retrieve", harness.ActionFocus: "focus", harness.ActionUnknown: "unknown",
	}
)

// word is the protocol's word for v in words, or "" for a value it lacks,
// or for the zero value.
func word[K comparable](words map[K]string, v K) string { return words[v] }

// value is the value of the host that w is the word for in words, or the
// zero value.
func value[K comparable](words map[K]string, w string) K {
	for k, v := range words {
		if v == w && w != "" {
			return k
		}
	}
	var zero K
	return zero
}

// status maps a phase to its status: every phase that is not terminal or
// waiting for a resolution is active.
func status(p core.Phase) string {
	switch p {
	case core.PhaseReturned:
		return StatusReturned
	case core.PhaseStopped:
		return StatusStopped
	case core.PhaseNeedsResolution:
		return StatusNeedsResolution
	}
	return StatusActive
}

func driver(driving bool) string {
	if driving {
		return DriverDriving
	}
	return DriverIdle
}

// CursorOf is the wire form of c: opaque to a client, which only gives it
// back.
func CursorOf(c host.Cursor) string { return c.RunID + ":" + strconv.FormatInt(c.Seq, 10) }

// ParseCursor is the host form of the wire cursor s, or an error wrapping
// host.ErrCursor when s is no cursor.
func ParseCursor(s string) (host.Cursor, error) {
	i := strings.LastIndexByte(s, ':')
	if i < 1 {
		return host.Cursor{}, fmt.Errorf("%w: %q is no cursor", host.ErrCursor, s)
	}
	seq, err := strconv.ParseInt(s[i+1:], 10, 64)
	if err != nil || seq < 0 || strconv.FormatInt(seq, 10) != s[i+1:] {
		return host.Cursor{}, fmt.Errorf("%w: %q is no cursor", host.ErrCursor, s)
	}
	return host.Cursor{RunID: s[:i], Seq: seq}, nil
}

// SpecOf is the wire form of s.
func SpecOf(s host.Spec) Spec {
	c, b, l := s.Config, s.Budget, s.Limits
	return Spec{Ephemeral: c.Ephemeral, Environment: c.Environment, EnvironmentWorkdir: c.EnvironmentWorkdir, Workspace: c.Workspace, Image: c.Image, Network: c.Network, Memory: c.Memory, CPUs: c.CPUs, Pids: c.Pids, OutputLimit: c.OutputLimit,
		API: c.API, Model: c.Model, MaxTokens: c.MaxTokens, Effort: c.Effort, Structured: c.Structured, DecisionFormat: c.DecisionFormat,
		Fallbacks: c.Fallbacks, RequestBytes: c.RequestBytes, ContextVersion: c.ContextVersion, HistoryImports: c.HistoryImports,
		MaxSteps: l.MaxSteps, MaxAttempts: l.MaxAttempts,
		WallTime: b.WallTime.String(), TokenBudget: b.Tokens, OutputBudget: b.OutputBytes, JobTimeout: b.JobTimeout.String()}
}

// spec is the host form of s.
func SpecFrom(s Spec) (host.Spec, error) {
	wall, err := time.ParseDuration(s.WallTime)
	if err != nil {
		return host.Spec{}, fmt.Errorf("wall time: %w", err)
	}
	job, err := time.ParseDuration(s.JobTimeout)
	if err != nil {
		return host.Spec{}, fmt.Errorf("job timeout: %w", err)
	}
	return host.Spec{
		Config: host.RunConfig{Ephemeral: s.Ephemeral, Environment: s.Environment, EnvironmentWorkdir: s.EnvironmentWorkdir, Workspace: s.Workspace, Image: s.Image, Network: s.Network, Memory: s.Memory, CPUs: s.CPUs, Pids: s.Pids, OutputLimit: s.OutputLimit,
			API: s.API, Model: s.Model, MaxTokens: s.MaxTokens, Effort: s.Effort, Structured: s.Structured, DecisionFormat: s.DecisionFormat,
			Fallbacks: s.Fallbacks, RequestBytes: s.RequestBytes, ContextVersion: s.ContextVersion, HistoryImports: s.HistoryImports,
		},
		Budget: harness.Budget{WallTime: wall, Tokens: s.TokenBudget, OutputBytes: s.OutputBudget, JobTimeout: job},
		Limits: core.Limits{MaxSteps: s.MaxSteps, MaxAttempts: s.MaxAttempts},
	}, nil
}

// BindingOf is the wire form of b, which carries only the keys of api.
func BindingOf(api string, b host.Binding) Binding {
	k := b.Keys.For(api)
	return Binding{Provider: b.Provider, Fallback: b.Fallback, AnthropicKey: k.AnthropicKey, AnthropicToken: k.AnthropicToken, OpenAIKey: k.OpenAIKey}
}

func BindingFrom(b *Binding) host.Binding {
	if b == nil {
		return host.Binding{}
	}
	return host.Binding{Provider: b.Provider, Fallback: b.Fallback, Keys: host.Keys{AnthropicKey: b.AnthropicKey, AnthropicToken: b.AnthropicToken, OpenAIKey: b.OpenAIKey}}
}

// PairsOf is the wire form of pairs.
func PairsOf(pairs []host.Pair) []Pair {
	out := make([]Pair, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, Pair{API: p.API, Provider: p.Provider})
	}
	return out
}

// RunOf is the wire form of v.
func RunOf(v host.View, driveErr error) Run {
	s, r, t, b := v.Snapshot.State, v.Run, v.Totals, v.Run.Budget
	w := Run{RunID: r.ID, SessionID: r.SessionID, SessionSeq: r.SessionSeq, InputID: r.InputID, Goal: r.Goal, Constraints: r.Constraints,
		Status: status(s.Phase), Phase: word(phaseWords, s.Phase), Step: s.Step, StopReason: word(reasonWords, s.Reason), EffectID: s.EffectID,
		Cursor: CursorOf(v.Cursor()), ContextVersion: v.Config.ContextVersion, API: v.Config.API, Provider: v.Config.Provider,
		Driver: driver(v.Driving), Drivable: v.Drivable, Why: v.Why,
		Totals: Totals{InputTokens: t.Usage.InputTokens, OutputTokens: t.Usage.OutputTokens, OutputBytes: t.OutputBytes, Steps: t.Steps, Coverage: UsageCoverage{Attempts: t.Coverage.Attempts, InputReports: t.Coverage.InputReports, OutputReports: t.Coverage.OutputReports, Lost: t.Coverage.Lost}},
		Budget: Budget{WallTime: b.WallTime.String(), Tokens: b.Tokens, OutputBytes: b.OutputBytes, JobTimeout: b.JobTimeout.String()}}
	w.Ephemeral, w.Environment = v.Config.Ephemeral, EnvironmentOf(v.Config)
	if s.Phase == core.PhaseReturned {
		w.Result = s.Decision.Payload
	}
	if driveErr != nil {
		w.DriveError = driveErr.Error()
	}
	return w
}

// State is the snapshot run r reports, as a client shows it: its run,
// phase, step, stop reason, effect, result, and sequence, from its cursor.
func StateOf(r Run) (harness.Snapshot, error) {
	c, err := ParseCursor(r.Cursor)
	if err != nil {
		return harness.Snapshot{}, err
	}
	if c.RunID != r.RunID {
		return harness.Snapshot{}, errors.New("hostrpc: a run whose cursor names another run")
	}
	return harness.Snapshot{Seq: c.Seq, State: core.State{RunID: r.RunID, Step: r.Step, Phase: value(phaseWords, r.Phase),
		Reason: value(reasonWords, r.StopReason), EffectID: r.EffectID, Decision: core.Decision{Payload: r.Result}}}, nil
}

// PageOf is the wire form of p.
func PageOf(p host.Page) Page {
	w := Page{Items: make([]Item, 0, len(p.Items)), Next: CursorOf(p.Next), More: p.More, Halted: p.Halted}
	for _, it := range p.Items {
		item := Item{Seq: it.Seq, Step: it.Step, Kind: word(itemWords, it.Kind), Command: it.Command, Purpose: it.Purpose,
			Action: word(actionWords, harness.ActionName(it.Action)), Result: it.Result, Local: it.Local, Resolved: it.Resolved,
			Execution: word(executionWords, it.Execution), ExitCode: it.ExitCode, Preview: it.Preview, Detail: it.Detail, AnchorSeq: it.AnchorSeq,
			StopReason: word(reasonWords, it.Reason), Failure: it.Failure, Truncated: it.Truncated}
		if o := it.Outcome; o != nil {
			item.Outcome = &Outcome{Status: word(outcomeWords, o.Status), Detail: o.Detail, Retrieval: o.Retrieval, RetrievalError: o.RetrievalError,
				Sources: o.Sources, Entries: o.Entries, Focus: o.Focus, FocusSources: o.FocusSources}
		}
		w.Items = append(w.Items, item)
	}
	return w
}

// EnvironmentOf projects stored wiring without sampling a live environment.
func EnvironmentOf(c host.RunConfig) *Environment {
	if a := c.Attachment; a != nil {
		return &Environment{Directory: a.Directory, ContainerID: a.ContainerID, DaemonID: a.DaemonID, Incarnation: a.Incarnation, Workdir: c.EnvironmentWorkdir}
	}
	return nil
}
