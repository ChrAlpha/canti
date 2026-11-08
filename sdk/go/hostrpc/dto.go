// Package hostrpc carries the operations of a runtime host over a Unix
// socket in its state directory: one JSON request and one JSON response per
// connection, under a protocol version of their own. Its types are the wire
// form, pinned per protocol version; they embed no journal, core, or host
// type, and every enumeration they carry is this package's own word for a
// value of the host, by a table that names each one, so a change inside
// canti changes no message unless it changes this package and, with it,
// Protocol.
package hostrpc

import "log/slog"

// Protocol is the version of the messages of this package. A change to any
// of them needs the next version and a new testdata/v<N> record, which is
// recorded once and never rewritten; a host and a client of different
// versions refuse each other. Versions 1 and 2 are recorded in testdata/v1
// and testdata/v2; version 5 adds validation and cursor-free initial progress
// requests. Records live with the runtime server contract tests.
const Protocol = 7

// Request is one operation. Op names it; the fields it uses are set.
type Request struct {
	Validate  *Validation `json:"validate,omitempty"`
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
	OpValidate  = "validate"
	OpHello     = "hello"
	OpSubmit    = "submit" // admit a run, then drive it with the binding
	OpDrive     = "drive"  // drive an admitted run with the binding
	OpInspect   = "inspect"
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
	Validated   *Validated  `json:"validated,omitempty"`
	Inspection  *Inspection `json:"inspection,omitempty"`
	Protocol    int         `json:"protocol"`
	Error       *Error      `json:"error,omitempty"`
	Hello       *Hello      `json:"hello,omitempty"`
	Ack         *Ack        `json:"ack,omitempty"`
	Run         *Run        `json:"run,omitempty"`
	Page        *Page       `json:"page,omitempty"`
	Wait        *Wait       `json:"wait,omitempty"`
	Constraints *string     `json:"constraints,omitempty"`
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
	Ephemeral          bool   `json:"ephemeral"`
	Environment        string `json:"environment"`
	EnvironmentWorkdir string `json:"environment_workdir"`
	Workspace          string `json:"workspace"`
	Image              string `json:"image"`
	Network            string `json:"network"`
	Memory             string `json:"memory"`
	CPUs               string `json:"cpus"`
	Pids               int    `json:"pids"`
	OutputLimit        int64  `json:"output_limit"`
	API                string `json:"api"`
	Model              string `json:"model"`
	MaxTokens          int    `json:"max_tokens"`
	Effort             string `json:"effort"`
	Structured         bool   `json:"structured"`
	DecisionFormat     string `json:"decision_format"`
	Fallbacks          bool   `json:"fallbacks"`
	RequestBytes       int    `json:"request_bytes"`
	ContextVersion     int    `json:"context_version"`
	HistoryImports     string `json:"history_imports"`
	MaxSteps           int    `json:"max_steps"`
	MaxAttempts        int    `json:"max_attempts"`
	WallTime           string `json:"wall_time"`
	TokenBudget        int64  `json:"token_budget"`
	OutputBudget       int64  `json:"output_budget"`
	JobTimeout         string `json:"job_timeout"`
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
	OwnerID   string `json:"owner_id,omitempty"`
}

// ResolutionOf is the wire form of a resolution: with job, the job the
// executor finds, and otherwise the confirmed outcome obs, whose execution
// state travels as the protocol's word for it.

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
	Environment    *Environment `json:"environment,omitempty"`
	Ephemeral      bool         `json:"ephemeral"`
	RunID          string       `json:"run_id"`
	SessionID      string       `json:"session_id,omitempty"`
	SessionSeq     int64        `json:"session_seq,omitempty"`
	InputID        string       `json:"input_id,omitempty"`
	Goal           string       `json:"goal"`
	Constraints    string       `json:"constraints"`
	Status         string       `json:"status"`
	Phase          string       `json:"phase"`
	Step           int          `json:"step"`
	StopReason     string       `json:"stop_reason,omitempty"`
	Result         string       `json:"result,omitempty"`
	EffectID       string       `json:"effect_id,omitempty"`
	Cursor         string       `json:"cursor"`
	ContextVersion int          `json:"context_version"`
	API            string       `json:"api"`
	Provider       string       `json:"provider,omitempty"`
	Driver         string       `json:"driver"`
	Drivable       bool         `json:"drivable"`
	Why            string       `json:"why,omitempty"`
	DriveError     string       `json:"drive_error,omitempty"`
	Totals         Totals       `json:"totals"`
	Budget         Budget       `json:"budget"`
}

// Totals are a run's measured usage.
type Totals struct {
	InputTokens  int64         `json:"input_tokens"`
	OutputTokens int64         `json:"output_tokens"`
	OutputBytes  int64         `json:"output_bytes"`
	Steps        int           `json:"steps"`
	Coverage     UsageCoverage `json:"coverage"`
}

// UsageCoverage describes how many settled/lost attempts supplied each token
// count. Totals sum received counts only; missing counts remain unknown and
// a retrospective reported-token threshold cannot guarantee a charge ceiling.
type UsageCoverage struct {
	Attempts      int64 `json:"attempts"`
	InputReports  int64 `json:"input_reports"`
	OutputReports int64 `json:"output_reports"`
	Lost          int64 `json:"lost"`
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
	AnchorSeq  int64    `json:"anchor_seq,omitempty"`
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
	Focus          bool   `json:"focus,omitempty"`
	FocusSources   int    `json:"focus_sources,omitempty"`
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

// Environment describes pinned execution wiring, without reading the live backend.
type Environment struct {
	Directory   string `json:"directory"`
	ContainerID string `json:"container_id"`
	DaemonID    string `json:"daemon_id"`
	Incarnation uint64 `json:"incarnation"`
	Workdir     string `json:"workdir"`
}
