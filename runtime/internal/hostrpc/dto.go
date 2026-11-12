package hostrpc

import (
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
		core.ExecutionFailedToStart: "failed_to_start", core.ExecutionUnknown: "unknown", core.ExecutionSuperseded: "superseded",
	}
	itemWords = map[host.ItemKind]string{
		host.ItemDecided: "decided", host.ItemReturned: "returned", host.ItemRetrying: "retrying", host.ItemCompleted: "completed",
		host.ItemContextReset: "context_reset", host.ItemRecovered: "effect_recovered", host.ItemStopRequested: "stop_requested",
		host.ItemNeedsResolution: "needs_resolution", host.ItemStopped: "stopped", host.ItemInput: "input",
		host.ItemWithdrawn: "withdrawn", host.ItemInputsBound: "inputs_bound", host.ItemRunStarted: "run_started",
	}
	authorityWords = map[harness.InputAuthority]string{
		harness.AuthorityPrincipal: "principal", harness.AuthorityObservation: "observation",
	}
	deliveryWords = map[harness.InputDelivery]string{
		harness.DeliverySteer: "steer", harness.DeliveryFollowUp: "follow_up",
	}
	dispositionWords = map[harness.InputDisposition]string{
		harness.DispositionOpened: "opened", harness.DispositionSteer: "steer",
		harness.DispositionQueued: "queued", harness.DispositionWithdrawn: "withdrawn",
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
// back. It names an agent-log position, never a run.
func CursorOf(c host.Cursor) string { return "pos:" + strconv.FormatInt(int64(c), 10) }

// ParseCursor is the host form of the wire cursor s, or an error wrapping
// host.ErrCursor when s is no cursor.
func ParseCursor(s string) (host.Cursor, error) {
	rest, ok := strings.CutPrefix(s, "pos:")
	if !ok || rest == "" {
		return 0, fmt.Errorf("%w: %q is no cursor", host.ErrCursor, s)
	}
	pos, err := strconv.ParseInt(rest, 10, 64)
	if err != nil || pos < 0 || strconv.FormatInt(pos, 10) != rest {
		return 0, fmt.Errorf("%w: %q is no cursor", host.ErrCursor, s)
	}
	return host.Cursor(pos), nil
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
	w := Run{RunID: r.ID, InputID: r.InputID, InputPos: r.InputPos, Fresh: r.Fresh, Goal: r.Goal, Constraints: r.Constraints,
		Ordinal: v.Ordinal,
		Status:  status(s.Phase), Phase: word(phaseWords, s.Phase), Step: s.Step, StopReason: word(reasonWords, s.Reason), EffectID: s.EffectID,
		Cursor: CursorOf(v.Cursor()), ContextVersion: v.Config.ContextVersion, API: v.Config.API, Provider: v.Config.Provider,
		Driver: driver(v.Driving), Drivable: v.Drivable, Why: v.Why,
		PendingSteers: v.PendingSteers, Queued: v.Queued, Held: v.Held,
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

// StateOf is the state run r reports, as a client shows it: its phase, step,
// stop reason, effect, and result. The cursor names an agent-log position,
// which is not the run's transition sequence.
func StateOf(r Run) (harness.Snapshot, error) {
	if _, err := ParseCursor(r.Cursor); err != nil {
		return harness.Snapshot{}, err
	}
	return harness.Snapshot{State: core.State{RunID: r.RunID, Step: r.Step, Phase: value(phaseWords, r.Phase),
		Reason: value(reasonWords, r.StopReason), EffectID: r.EffectID, Decision: core.Decision{Payload: r.Result}}}, nil
}

// PageOf is the wire form of p.
func PageOf(p host.Page) Page {
	w := Page{Items: make([]Item, 0, len(p.Items)), Next: CursorOf(p.Next), More: p.More, Quiescent: p.Quiescent}
	for _, it := range p.Items {
		item := Item{Pos: it.Pos, RunID: it.RunID, Seq: it.Seq, Step: it.Step, Kind: word(itemWords, it.Kind), Command: it.Command, Purpose: it.Purpose,
			Action: word(actionWords, harness.ActionName(it.Action)), Result: it.Result, Local: it.Local, Resolved: it.Resolved,
			Execution: word(executionWords, it.Execution), ExitCode: it.ExitCode, Preview: it.Preview, Detail: it.Detail, AnchorSeq: it.AnchorSeq,
			StopReason: word(reasonWords, it.Reason), Failure: it.Failure, SupersededBy: it.SupersededBy, Inputs: it.Inputs,
			Withdrawn: append([]string{}, it.Withdrawn...), Truncated: it.Truncated}
		if o := it.Outcome; o != nil {
			item.Outcome = &Outcome{Status: word(outcomeWords, o.Status), Detail: o.Detail, Retrieval: o.Retrieval, RetrievalError: o.RetrievalError,
				Sources: o.Sources, Entries: o.Entries, Focus: o.Focus, FocusSources: o.FocusSources}
		}
		if in := it.Input; in != nil {
			item.Input = &Input{Pos: it.Pos, InputID: in.InputID, Authority: word(authorityWords, in.Authority), Delivery: word(deliveryWords, in.Delivery),
				Disposition: word(dispositionWords, in.Disposition), Source: in.Source, Subject: in.Subject, Text: in.Text, Truncated: in.Truncated}
		}
		w.Items = append(w.Items, item)
	}
	return w
}

// InputOf is the wire form of one admitted input.
func InputOf(in harness.Input) Input {
	return Input{Pos: in.Pos, InputID: in.InputID, Authority: word(authorityWords, in.Authority), Delivery: word(deliveryWords, in.Delivery),
		Disposition: word(dispositionWords, in.Disposition), Source: in.Source, Subject: in.Subject, Text: in.Text}
}

// AckOf is the wire form of a host acknowledgement.
func AckOf(a host.Ack, v *host.View, driveErr error) Ack {
	w := Ack{Replayed: a.Replayed, Differs: append([]string{}, a.Differs...), InputID: a.Input.InputID,
		Disposition: word(dispositionWords, a.Input.Disposition), Pos: a.Input.Pos}
	if a.Opened() && v != nil {
		run := RunOf(*v, driveErr)
		w.Run = &run
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
