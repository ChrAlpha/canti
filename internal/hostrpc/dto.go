// Package hostrpc carries the operations of a runtime host over a Unix
// socket in its state directory: one JSON request and one JSON response per
// connection, under a protocol version of their own. Its types are the wire
// form, pinned per protocol version; they embed no journal, core, or host
// type, and every enumeration they carry is this package's own word for a
// value of the host, by a table that names each one, so a change inside
// canti changes no message unless it changes this package and, with it,
// Protocol.
package hostrpc

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/host"
)

// Protocol is the version of the messages of this package. A change to any
// of them needs the next version and a new testdata/v<N> record, which is
// recorded once and never rewritten; a host and a client of different
// versions refuse each other. Versions 1 and 2 are recorded in testdata/v1
// and testdata/v2; version 3 adds the word of the carry action.
const Protocol = 3

// Request is one operation. Op names it; the fields it uses are set.
type Request struct {
	Protocol  int         `json:"protocol"`
	Op        string      `json:"op"`
	RunID     string      `json:"run_id,omitempty"`
	SessionID string      `json:"session_id,omitempty"`
	Cursor    string      `json:"cursor,omitempty"`
	Submit    *Submission `json:"submit,omitempty"`
	Binding   *Binding    `json:"binding,omitempty"`
	Once      bool        `json:"once,omitempty"`
	Limit     int         `json:"limit,omitempty"`
	TimeoutMS int64       `json:"timeout_ms,omitempty"`
	CleanupMS int64       `json:"cleanup_ms,omitempty"`
	Resolve   *Resolution `json:"resolve,omitempty"`
}

// The operations.
const (
	OpHello     = "hello"
	OpSubmit    = "submit"    // admit a run, then drive it with the binding
	OpDrive     = "drive"     // drive an admitted run with the binding
	OpView      = "view"      // the state of a run; the latest when RunID is empty
	OpProgress  = "progress"  // a page of a run's progress after Cursor
	OpWait      = "wait"      // wait for a run to pass Cursor, at most TimeoutMS
	OpStop      = "stop"      // stop a run, the host bounding cleanup by CleanupMS
	OpResolve   = "resolve"   // record the outcome or the job of an unknown effect
	OpInherited = "inherited" // the constraints a new input of SessionID inherits
	OpShutdown  = "shutdown"  // shut the host down
)

// Response answers one request: Error, or the result of its operation.
type Response struct {
	Protocol    int     `json:"protocol"`
	Error       *Error  `json:"error,omitempty"`
	Hello       *Hello  `json:"hello,omitempty"`
	Ack         *Ack    `json:"ack,omitempty"`
	Run         *Run    `json:"run,omitempty"`
	Page        *Page   `json:"page,omitempty"`
	Wait        *Wait   `json:"wait,omitempty"`
	Constraints *string `json:"constraints,omitempty"`
}

// Error is a refused or failed operation. Code classifies it for the client.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// The error codes.
const (
	CodeProtocol = "protocol" // the request is of another protocol version
	CodeSpec     = "spec"     // no run can be created from the submission
	CodeConflict = "conflict" // the input names a run of other content
	CodeClosing  = "closing"  // the host is shutting down
	CodeDriving  = "driving"  // the host already drives the run
	CodeCursor   = "cursor"   // the cursor names no position of a run
	CodeFailed   = "failed"   // anything else; Message says what
)

// Hello describes a host: its versions, for information, its process, and
// the pairs its own credentials are bound to, without the keys.
type Hello struct {
	Protocol         int    `json:"protocol"`
	Version          string `json:"version"`
	ContextVersion   int    `json:"context_version"`
	SemanticsVersion int    `json:"semantics_version"`
	PID              int    `json:"pid"`
	Bindings         []Pair `json:"bindings"`
}

// Pair names the model API and the exact provider a credential is bound to.
type Pair struct {
	API      string `json:"api"`
	Provider string `json:"provider"`
}

// Spec is the configuration of a new run, with its budget and limits.
// Durations are Go duration strings.
type Spec struct {
	Workspace      string `json:"workspace"`
	Image          string `json:"image"`
	Network        string `json:"network"`
	Memory         string `json:"memory"`
	CPUs           string `json:"cpus"`
	Pids           int    `json:"pids"`
	OutputLimit    int64  `json:"output_limit"`
	API            string `json:"api"`
	Model          string `json:"model"`
	MaxTokens      int    `json:"max_tokens"`
	Effort         string `json:"effort"`
	Structured     bool   `json:"structured"`
	DecisionFormat string `json:"decision_format"`
	Fallbacks      bool   `json:"fallbacks"`
	ContextBytes   int    `json:"context_bytes"`
	ContextVersion int    `json:"context_version"`
	HistoryImports string `json:"history_imports"`
	WindowSteps    int    `json:"window_steps"`
	WindowBytes    int    `json:"window_bytes"`
	HandoverBytes  int    `json:"handover_bytes"`
	MaxSteps       int    `json:"max_steps"`
	MaxAttempts    int    `json:"max_attempts"`
	WallTime       string `json:"wall_time"`
	TokenBudget    int64  `json:"token_budget"`
	OutputBudget   int64  `json:"output_budget"`
	JobTimeout     string `json:"job_timeout"`
}

// Submission is a request for a new run.
type Submission struct {
	SessionID           string `json:"session_id,omitempty"`
	InputID             string `json:"input_id,omitempty"`
	Goal                string `json:"goal"`
	Constraints         string `json:"constraints"`
	ExplicitConstraints bool   `json:"explicit_constraints"`
	Spec                Spec   `json:"spec"`
}

// Binding is the provider and the key a drive calls the model with. The key
// travels only in requests, over the owner's socket, and is never answered,
// logged, or stored.
type Binding struct {
	Provider       string `json:"provider,omitempty"`
	Fallback       string `json:"fallback,omitempty"`
	AnthropicKey   string `json:"anthropic_key,omitempty"`
	AnthropicToken string `json:"anthropic_token,omitempty"`
	OpenAIKey      string `json:"openai_key,omitempty"`
}

// String hides the keys of b.
func (b Binding) String() string { return "binding (keys [redacted])" }

// GoString hides the keys of b.
func (b Binding) GoString() string { return "hostrpc.Binding{[redacted]}" }

// LogValue hides the keys of b from structured logs. Only the request's
// JSON encoding, which the protocol needs, carries them.
func (b Binding) LogValue() slog.Value { return slog.StringValue(b.String()) }

// Resolution records the outcome of an unknown effect, or with Job, the job
// the executor finds for it.
type Resolution struct {
	Job       bool   `json:"job,omitempty"`
	Execution string `json:"execution,omitempty"`
	ExitCode  *int   `json:"exit_code,omitempty"`
	Note      string `json:"note,omitempty"`
}

// ResolutionOf is the wire form of a resolution: with job, the job the
// executor finds, and otherwise the confirmed outcome obs, whose execution
// state travels as the protocol's word for it.
func ResolutionOf(job bool, obs harness.Observation) Resolution {
	if job {
		return Resolution{Job: true}
	}
	return Resolution{Execution: word(executionWords, obs.Execution), ExitCode: obs.ExitCode, Note: obs.Preview}
}

// Ack acknowledges a durably admitted submission, or names the run an
// earlier attempt of the same input admitted, with Replayed, and Differs
// naming the settings in which the retry differs from that run, which keeps
// its own.
type Ack struct {
	Replayed bool     `json:"replayed"`
	Differs  []string `json:"differs"`
	Run      Run      `json:"run"`
}

// Run is the state of a run. Status is active, needs_resolution, returned,
// or stopped; Phase names its committed phase, for display. Cursor is the
// position of this state: following the run from it misses and repeats
// nothing. Driver is driving while the host drives the run and idle
// otherwise; Drivable reports that the host drives it or could continue it
// without a client's credential, and Why says what is missing otherwise.
type Run struct {
	RunID          string `json:"run_id"`
	SessionID      string `json:"session_id,omitempty"`
	SessionSeq     int64  `json:"session_seq,omitempty"`
	InputID        string `json:"input_id,omitempty"`
	Goal           string `json:"goal"`
	Constraints    string `json:"constraints"`
	Status         string `json:"status"`
	Phase          string `json:"phase"`
	Step           int    `json:"step"`
	StopReason     string `json:"stop_reason,omitempty"`
	Result         string `json:"result,omitempty"`
	EffectID       string `json:"effect_id,omitempty"`
	Cursor         string `json:"cursor"`
	ContextVersion int    `json:"context_version"`
	API            string `json:"api"`
	Provider       string `json:"provider,omitempty"`
	Driver         string `json:"driver"`
	Drivable       bool   `json:"drivable"`
	Why            string `json:"why,omitempty"`
	DriveError     string `json:"drive_error,omitempty"`
	Totals         Totals `json:"totals"`
	Budget         Budget `json:"budget"`
}

// Totals are a run's measured usage.
type Totals struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	OutputBytes  int64 `json:"output_bytes"`
	Steps        int   `json:"steps"`
}

// Budget is a run's budget; durations are Go duration strings.
type Budget struct {
	WallTime    string `json:"wall_time"`
	Tokens      int64  `json:"tokens"`
	OutputBytes int64  `json:"output_bytes"`
	JobTimeout  string `json:"job_timeout"`
}

// Page is one page of a run's progress: Next is the cursor of the page that
// follows, More reports that rows remain, and Halted that the run had halted.
type Page struct {
	Items  []Item `json:"items"`
	Next   string `json:"next"`
	More   bool   `json:"more"`
	Halted bool   `json:"halted"`
}

// Item is one entry of a run's progress. Truncated reports that one of its
// texts is cut at its cap.
type Item struct {
	Seq        int64    `json:"seq"`
	Step       int      `json:"step"`
	Kind       string   `json:"kind"`
	Command    string   `json:"command,omitempty"`
	Purpose    string   `json:"purpose,omitempty"`
	Action     string   `json:"action,omitempty"`
	Result     string   `json:"result,omitempty"`
	Local      bool     `json:"local,omitempty"`
	Resolved   bool     `json:"resolved,omitempty"`
	Execution  string   `json:"execution,omitempty"`
	ExitCode   *int     `json:"exit_code,omitempty"`
	Preview    string   `json:"preview,omitempty"`
	Detail     string   `json:"detail,omitempty"`
	Outcome    *Outcome `json:"outcome,omitempty"`
	Window     int      `json:"window,omitempty"`
	StopReason string   `json:"stop_reason,omitempty"`
	Failure    string   `json:"failure,omitempty"`
	Truncated  bool     `json:"truncated,omitempty"`
}

// Outcome is the result of a local action.
type Outcome struct {
	Status         string `json:"status"`
	Detail         string `json:"detail,omitempty"`
	Retrieval      bool   `json:"retrieval,omitempty"`
	RetrievalError string `json:"retrieval_error,omitempty"`
	Sources        int    `json:"sources,omitempty"`
	Entries        int    `json:"entries,omitempty"`
	Handover       bool   `json:"handover,omitempty"`
}

// Wait is what a wait observed: the cursor of the run's latest transition,
// whether it has halted, and its driver.
type Wait struct {
	Cursor string `json:"cursor"`
	Halted bool   `json:"halted"`
	Driver string `json:"driver"`
}

// The statuses of a run, and its drivers.
const (
	StatusActive          = "active"
	StatusNeedsResolution = "needs_resolution"
	StatusReturned        = "returned"
	StatusStopped         = "stopped"

	DriverDriving = "driving"
	DriverIdle    = "idle"
)

// The words of the protocol for the enumerations of the host. Each table
// names every value; a value that it lacks fails TestEveryValueHasAWord and
// travels as "", never as the host's own spelling.
var (
	phaseWords = map[core.Phase]string{
		core.PhaseIdle: "idle", core.PhaseRequestFrozen: "request_frozen", core.PhaseRequestSent: "request_sent",
		core.PhaseResponseSaved: "response_saved", core.PhaseEffectSaved: "effect_saved", core.PhaseJobBound: "job_bound",
		core.PhaseAwaitingObservation: "awaiting_observation", core.PhaseCancelling: "cancelling", core.PhaseReturned: "returned",
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
		host.ItemStopRequested: "stop_requested", host.ItemNeedsResolution: "needs_resolution", host.ItemStopped: "stopped",
	}
	outcomeWords = map[harness.OutcomeStatus]string{
		harness.OutcomeDone: "done", harness.OutcomeFailed: "failed", harness.OutcomeRefused: "refused",
	}
	actionWords = map[harness.ActionName]string{
		harness.ActionCommand: "command", harness.ActionRetrieve: "retrieve", harness.ActionHandover: "handover",
		harness.ActionHandoverCarry: "handover_carry", harness.ActionUnknown: "unknown", harness.ActionLegacy: "legacy",
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
	return Spec{Workspace: c.Workspace, Image: c.Image, Network: c.Network, Memory: c.Memory, CPUs: c.CPUs, Pids: c.Pids, OutputLimit: c.OutputLimit,
		API: c.API, Model: c.Model, MaxTokens: c.MaxTokens, Effort: c.Effort, Structured: c.Structured, DecisionFormat: c.DecisionFormat,
		Fallbacks: c.Fallbacks, ContextBytes: c.ContextBytes, ContextVersion: c.ContextVersion, HistoryImports: c.HistoryImports,
		WindowSteps: c.WindowSteps, WindowBytes: c.WindowBytes, HandoverBytes: c.HandoverBytes, MaxSteps: l.MaxSteps, MaxAttempts: l.MaxAttempts,
		WallTime: b.WallTime.String(), TokenBudget: b.Tokens, OutputBudget: b.OutputBytes, JobTimeout: b.JobTimeout.String()}
}

// spec is the host form of s.
func (s Spec) spec() (host.Spec, error) {
	wall, err := time.ParseDuration(s.WallTime)
	if err != nil {
		return host.Spec{}, fmt.Errorf("wall time: %w", err)
	}
	job, err := time.ParseDuration(s.JobTimeout)
	if err != nil {
		return host.Spec{}, fmt.Errorf("job timeout: %w", err)
	}
	return host.Spec{
		Config: host.RunConfig{Workspace: s.Workspace, Image: s.Image, Network: s.Network, Memory: s.Memory, CPUs: s.CPUs, Pids: s.Pids, OutputLimit: s.OutputLimit,
			API: s.API, Model: s.Model, MaxTokens: s.MaxTokens, Effort: s.Effort, Structured: s.Structured, DecisionFormat: s.DecisionFormat,
			Fallbacks: s.Fallbacks, ContextBytes: s.ContextBytes, ContextVersion: s.ContextVersion, HistoryImports: s.HistoryImports,
			WindowSteps: s.WindowSteps, WindowBytes: s.WindowBytes, HandoverBytes: s.HandoverBytes},
		Budget: harness.Budget{WallTime: wall, Tokens: s.TokenBudget, OutputBytes: s.OutputBudget, JobTimeout: job},
		Limits: core.Limits{MaxSteps: s.MaxSteps, MaxAttempts: s.MaxAttempts},
	}, nil
}

// BindingOf is the wire form of b, which carries only the keys of api.
func BindingOf(api string, b host.Binding) Binding {
	k := b.Keys.For(api)
	return Binding{Provider: b.Provider, Fallback: b.Fallback, AnthropicKey: k.AnthropicKey, AnthropicToken: k.AnthropicToken, OpenAIKey: k.OpenAIKey}
}

func (b *Binding) binding() host.Binding {
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
		Totals: Totals{InputTokens: t.Usage.InputTokens, OutputTokens: t.Usage.OutputTokens, OutputBytes: t.OutputBytes, Steps: t.Steps},
		Budget: Budget{WallTime: b.WallTime.String(), Tokens: b.Tokens, OutputBytes: b.OutputBytes, JobTimeout: b.JobTimeout.String()}}
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
func (r Run) State() (harness.Snapshot, error) {
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
			Execution: word(executionWords, it.Execution), ExitCode: it.ExitCode, Preview: it.Preview, Detail: it.Detail, Window: it.Window,
			StopReason: word(reasonWords, it.Reason), Failure: it.Failure, Truncated: it.Truncated}
		if o := it.Outcome; o != nil {
			item.Outcome = &Outcome{Status: word(outcomeWords, o.Status), Detail: o.Detail, Retrieval: o.Retrieval, RetrievalError: o.RetrievalError,
				Sources: o.Sources, Entries: o.Entries, Handover: o.Handover}
		}
		w.Items = append(w.Items, item)
	}
	return w
}

// HostPage is the host form of p, as a client renders it.
func (p Page) HostPage() (host.Page, error) {
	next, err := ParseCursor(p.Next)
	if err != nil {
		return host.Page{}, err
	}
	out := host.Page{Next: next, More: p.More, Halted: p.Halted}
	for _, it := range p.Items {
		item := host.Item{Seq: it.Seq, Step: it.Step, Kind: value(itemWords, it.Kind), Command: it.Command, Purpose: it.Purpose,
			Action: string(value(actionWords, it.Action)), Result: it.Result, Local: it.Local, Resolved: it.Resolved,
			Execution: value(executionWords, it.Execution), ExitCode: it.ExitCode, Preview: it.Preview, Detail: it.Detail, Window: it.Window,
			Reason: value(reasonWords, it.StopReason), Failure: it.Failure, Truncated: it.Truncated}
		if o := it.Outcome; o != nil {
			item.Outcome = &host.Outcome{Status: value(outcomeWords, o.Status), Detail: o.Detail, Retrieval: o.Retrieval,
				RetrievalError: o.RetrievalError, Sources: o.Sources, Entries: o.Entries, Handover: o.Handover}
		}
		out.Items = append(out.Items, item)
	}
	return out, nil
}
