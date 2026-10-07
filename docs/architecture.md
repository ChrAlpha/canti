# Architecture

## Purpose and scope

canti explores a fully stateless run kernel with a separate harness that
composes the right information for each step and performs every I/O. The
implementation is an independent Go module, `github.com/ChrAlpha/canti`. A
future Memoh adapter may consume it; canti does not import Memoh internal
packages.

V1 is one machine, one workspace writer, serial decisions, bounded model
input, and recovery from committed facts. It is implemented: a SQLite journal,
a Docker executor, an Anthropic Messages adapter, and a CLI drive runs through
the core. [V1 acceptance](v1-acceptance.md) records what is verified and what
is not.

## Ownership

| Area | Owns | Evidence |
| --- | --- | --- |
| `core/decode.go` | Decision protocol and decoding | Offline tests |
| `core/state.go`, `core/event.go` | Run state, events, intents, transition rules, state invariants | Full transition-table test and trajectory tests |
| `internal/harness/frame.go`, `prompt.go`, `window.go` | The frame of one request, its bounded prompt, and durable handover window | Offline tests; long-history measurement over SQLite |
| `internal/harness/model.go` | Model contract: freeze, one attempt with the frozen bytes, decision extraction, `StopError` | Implemented by `anthropic` |
| `internal/harness/store.go` | `Store` and `Journal` contracts: sequenced atomic commits, bounded recovery reads, run records, budgets, totals | Implemented by `sqlite` |
| `internal/harness/runner.go` | Intent interpretation: commit, bind, start | Recording-store tests; used by the loop |
| `internal/harness/loop.go` | Driving committed intents to a halt, measured budgets, persistent stop, explicit resolution | Tests with a fake model and executor over SQLite; end to end through the CLI |
| `internal/harness/executor.go` | Executor contract: jobs keyed by effect ID, bind/start split, start latch, `ErrJobLost` | Implemented by `docker` |
| `internal/harness/executortest` | Executable start latch contract | Passed by an offline reference and the Docker executor; rejects single-clause mutants |
| `internal/harness/artifacts.go` | Observation and output reference records | Types |
| `internal/harness/history.go` | The read-only step records that jobs read, derived from committed facts | Offline tests over SQLite; Docker-backed gate tests |
| `internal/archive` | Read-only access to the step records, artifacts, and imported history that jobs see: path safety, the retrieval operations, their continuations, and the evidence each returns | Offline tests on real directories |
| `internal/harness/sqlite` | The journal on one SQLite file | Tests on real database files, including SIGKILL recovery |
| `internal/harness/docker` | One container per effect through the docker CLI, start latch, output capture | Fake-CLI tests and Docker integration tests |
| `internal/harness/anthropic` | Non-streaming Anthropic Messages adapter over `net/http` | Tests against a local HTTP server; an opt-in live test and three recorded live samples with one model through a Messages-compatible gateway ([V1 acceptance](v1-acceptance.md#live-provider-samples-2026-09-23)) |
| `cmd/canti` | CLI: commands, state directory, workspace lock, signals, credential | Subprocess tests; Docker-backed gate tests |

The core imports only the standard library and holds no per-run state between
calls. The module's one direct dependency is `modernc.org/sqlite`, a cgo-free
SQLite. Model I/O, history, time, storage, and execution belong to the
harness. Retry, termination, and cancellation rules belong to the core; the
harness reports the facts that trigger them. No framework is needed between
these packages.

## Decision protocol

The public entry point is:

```go
func Decode(raw []byte) (Decision, error)
```

It accepts exactly one UTF-8 JSON object with two required and one optional
case-sensitive, non-null string fields, `kind`, `payload`, and `purpose`:

```json
{"kind":"exec","purpose":"Run the tests, then fix the first failure.","payload":"go test ./..."}
```

```json
{"kind":"return","payload":"Need a decision from the caller."}
```

`exec` requests execution in the configured environment. The core neither
executes nor authorizes the payload, but an `exec` payload must not be empty
or only whitespace, so a reply that would run nothing is repaired like any
other invalid decision instead of consuming a step; other execution policy
belongs to the harness. `return` hands back control and may contain a result,
question, or unresolved blocker, or be empty. It is not a success flag. `purpose` is the model's note on what the step is for and what it will
do next; it is advisory text, never executed or authorized, and the harness
shows it with the step in later requests.

The decoder rejects unknown or duplicate fields, wrong types, missing
`kind` or `payload`, a null `purpose`, unsupported kinds, a blank `exec`
payload, invalid UTF-8,
malformed or incomplete JSON, and trailing values. It does not extract JSON
from Markdown. Transport completeness is a separate adapter responsibility:
valid JSON alone does not prove that a model response completed.

A raw provider response and the decision payload are different layers. The
adapter records the complete raw response with its provenance
(model/protocol/adapter); it then extracts the decision text, which
`ResponseLoaded` carries into the core and which is persisted as part of that
transition. When the adapter extracts no text, the raw body is carried
instead, which the strict decoder rejects unless it is itself exactly one
decision object; the core never interprets provider content blocks or HTTP
framing.

## Identity and causality

Every external result must name the position it belongs to; the core rejects
a result that claims a different position instead of adopting it:

| Identity | Meaning | Where it lives |
| --- | --- | --- |
| `RunID` | One autonomous run | `State.RunID` |
| `Step` | One decision position | `State.Step`; on attempt/response events |
| `RequestRev` | Which frozen request of the step is current | `State.RequestRev` |
| `Attempt` | One authorized send within a step | `State.Attempt`; on attempt/response events |
| `EffectID` | One committed action, derived from run and step | `State.EffectID`; on effect events |
| `JobID` | The backend job bound to the effect | `State.JobID`; on job events |

Derived record identities need no extra fields: request bytes belong to
(run, step, revision); a response belongs to (run, step, attempt); attempts
never reset within a step, so a late response from an earlier attempt or an
earlier step is invalid. Observations, unknown outcomes, resolutions, and job
events name their effect; an event for a foreign effect is invalid. Delivery
identity (deduplicating one commit) is separate from causal identity and is
carried by the commit protocol below, not by core events. `RunID` must be
unique across every run that shares a store or executor namespace: effect and
event identities inherit their uniqueness from it, and a reused run ID would
let a new run adopt a previous run's job and records.

## Run state and transitions

`core.State` is the complete projection a harness needs to continue a run:

```go
type State struct {
	RunID      string
	Step       int
	Attempt    int
	RequestRev int
	Phase      Phase
	Limits     Limits
	Decision   Decision
	EffectID   string
	JobID      string
	Started    bool
	Reason     StopReason
}
```

`NewState` starts at step 1 in `idle` and requires positive step and attempt
limits. `State.Intent` maps the committed phase to the single action the
harness may take next. `State.Next` applies one event and returns the new
state, or the unchanged state with `ErrInvalidEvent` or `ErrIllegalTransition`.
Both are pure functions of their inputs. A legal event may leave the state
equal to its input; the harness still records it.

`State.Validate` checks the invariants every reachable state satisfies —
phase-appropriate references, budgets, and identities. A state loaded from
storage or received from another host must pass validation before any intent
is acted on; the runner refuses I/O for an invalid state with
`ErrInvalidState`. `Next` itself stays total over state values, and unknown
phases halt rather than act.

| Phase | Intent | Legal events |
| --- | --- | --- |
| `idle` | `prepare_request` | `RequestFrozen`, `StopRequested` |
| `request_frozen` | `send_request` | `AttemptStarted`, `StopRequested` |
| `request_sent` | `send_request` | `AttemptStarted`, `ResponseSaved`, `ResponseIncomplete`, `StopRequested` |
| `response_saved` | `decode` | `ResponseLoaded`, `StopRequested` |
| `effect_saved` | `bind` | `JobBound`, `Observed`, `OutcomeUnknown`, `StopRequested` |
| `job_bound` | `start` | `JobStarted`, `Observed`, `OutcomeUnknown`, `StopRequested` |
| `awaiting_observation` | `inspect` | `Observed`, `OutcomeUnknown`, `StopRequested` |
| `cancelling` | `cancel` | `JobBound`, `Observed`, `OutcomeUnknown`, `StopRequested` |
| `needs_resolution` | `halt` | `JobBound`, `Resolved`, `StopRequested` |
| `returned`, `stopped`, unknown | `halt` | none |

Rules the table enforces:

- Every I/O is preceded by a committed phase that authorizes it. A restart
  resumes by asking `Intent` again, so recovery and fresh steps share one path.
- `RequestFrozen` increments the step's request revision. `AttemptStarted` is
  committed before each model send and names its step and attempt. In
  `request_sent` it names the next attempt and reports the previous send lost,
  so sends are counted even when a crash loses their outcome. The bound is on
  authorized sends: a crash between the `AttemptStarted` commit and the send
  consumes an attempt without sending.
- Retry taxonomy is explicit. A transport failure (`ResponseIncomplete`)
  returns to `request_frozen`: same frozen bytes, next attempt. An
  undecodable decision returns to `idle`: the next `prepare_request` freezes a
  new request revision that may carry protocol feedback (the harness can
  reproduce the decode error by calling `core.Decode` on the saved body).
  Both consume attempts from the same per-step budget until `MaxAttempts`,
  stopping with `attempts_exhausted` or `protocol_failure`. An adapter must
  map conditions a retry cannot fix to `StopRequested`, not to
  `ResponseIncomplete`: authentication and configuration failures are
  `unrecoverable`, an exhausted quota is `budget_exhausted`.
- An `exec` decision commits `effect_saved` with an effect ID derived from
  the run ID and step. A `return` decision closes the run as `returned`.
- Binding and starting are separate boundaries. `JobBound` records which
  backend job owns the effect and never implies execution; only from a
  committed `job_bound` phase may a start be issued, recorded as
  `JobStarted`, which sets `Started`. From then on side effects may exist.
  `job_bound` always means no start was committed — `Validate` enforces it —
  though a crashed controller may have issued one without committing, which
  is why `Start` must be a no-op for a job that ever started. `job_bound`
  also accepts a direct `Observed` so a host that inspects a bound job — one
  whose start was issued but whose commit was lost — can record an
  already-finished result; the current runner instead always re-issues the
  idempotent start.
- Outside `cancelling`, `Observed` completes the step: the run continues in
  `idle` at the next step with identities cleared, or stops with
  `steps_exhausted`. That stop is deliberate: the final observation is
  recorded but the model gets no closing call. `Observed` rejects the
  `unknown` execution state, which must be reported as `OutcomeUnknown`.
  Without a recorded job, `effect_saved` accepts only `failed_to_start` and
  `cancelling` only `cancelled`; any other outcome must first record its job
  with `JobBound`.
- `OutcomeUnknown` enters `needs_resolution`. It accepts a later `JobBound`
  to resume behind the start boundary at `job_bound` when no start was
  committed, at `awaiting_observation` when one was, or at `cancelling` when
  a stop is pending — so an unknown outcome after a committed start goes back
  to inspection and never re-issues the start. It also accepts an explicit
  `Resolved`: a known outcome continues the run or completes a
  pending stop, except that a jobless resolution cannot claim `exited` or
  `timed_out`, which are impossible without a started job; resolving to
  `unknown` abandons the run as `stopped`, keeping the first stop reason or
  recording `unresolved`, preserving the uncertainty instead of fabricating
  an outcome.
- `StopRequested` stops immediately while no effect exists and enters
  `cancelling` once one may exist. `cancelling` records the true result before
  the run becomes `stopped`. In `cancelling` and `needs_resolution` a stop
  request is recorded without changing phase; the first reason is kept. Only
  `cancelled`, `timed_out`, `budget_exhausted`, and `unrecoverable` may be
  reported; the remaining stop reasons are set by the core's own rules and
  cannot be claimed from outside.
- `JobBound` requires effect and job IDs and must match a job already known;
  `JobStarted` must name the bound job.
- Adding a capability must not add an event kind; capabilities are payloads.

## Interpreter and loop

Runs commit against a `Snapshot{Seq, State}`: the state plus the sequence of
the last committed transition. `Runner.Commit` takes the snapshot, one event,
and the facts that event permits. It checks cancellation once, validates the
state, derives the next state with `core.State.Next`, validates the facts,
and hands a single `Transition` to the store at sequence `Seq+1` with a
deterministic event ID, `core.EventID`, derived from run, sequence, and event
content.

```text
Commit: check cancellation -> Validate -> Next -> check facts
        -> Store.Commit(Transition{Seq+1, EventID, From, Event, To, Facts})
Bind:   require bind intent -> Validate -> check cancellation -> Executor.Bind
        -> Commit(JobBound) | Commit(OutcomeUnknown) | return cancellation
Start:  require start intent -> Validate -> check cancellation -> Executor.Start
        -> Commit(JobStarted) | Commit(OutcomeUnknown) | return cancellation
```

Each event permits a fixed set of facts and anything else is rejected:
`RequestFrozen` requires request bytes; `ResponseSaved` a complete response
with a body; `ResponseIncomplete` may carry the partial response and failure
text; `Observed` and `Resolved` an observation whose execution state matches
the event; `OutcomeUnknown` and `StopRequested` failure text. Facts learned
after the harness's own context was cancelled must be committed with a
separate context, because `Commit` writes nothing once its context is done.

A rejected transition writes nothing, but a store error does not prove the
commit failed: the transaction may have committed before the acknowledgement
was lost. The caller resolves this by retrying the identical snapshot and
event — the derived event ID is the same, and the store confirms the recorded
transition without recording twice. `Runner.Bind` and `Runner.Start` commit
what the executor reported: a bound job whatever its status snapshot, because
`Inspect` is the authority on outcomes; an executor error, a missing job ID,
or a job other than the bound one becomes `OutcomeUnknown` with the failure
text. A cancellation observed around either call returns without committing,
so a resume finds the job by effect ID.

An effect ID hashes a length-prefixed run ID followed by the step number. The
same pair produces the same ID. The store and executor reject conflicting
content under an existing identity and accept identical retries without
recording again or creating another job; see
[persistence](#persistence-and-recovery) and [execution](#execution).

`harness.Loop` drives the intents of runs in a `Journal` until they halt. It
keeps no run state between calls: each `Step` loads and validates the latest
snapshot and reads the run record. Unless a stop reason is already recorded or
only a response already paid for waits to be decoded, a spent wall-time,
token, or output-byte budget is committed as a stop instead of the intent.
Otherwise `Step` performs the one I/O the intent authorizes, so a new `Loop`,
in the same or another process, continues any run. `Run` repeats `Step` until
the intent is `halt` or a step fails, and at a halt publishes the run's
[step records](#step-records), logging a failure without changing its result.

| Intent | Loop action |
| --- | --- |
| `prepare_request` | Build the [frame](#input) from the run record, the complete active window and its handover, and in a repair the rejected response, returning the error of a failed journal read and committing nothing; publish the run's [step records](#step-records), returning the error and committing nothing when that fails; render the prompt; `Model.Freeze`; commit `RequestFrozen` with the request, or `StopRequested{unrecoverable}` when the recorded window, the run's session fields, or earlier session runs fail their checks, the goal or constraints exceed their limit, no prompt fits, or freezing fails |
| `send_request` | Load the frozen request of the current revision; commit `AttemptStarted`, naming the next attempt when the phase is `request_sent`; send once, bounded by the remaining wall time; commit `ResponseSaved`, a `StopRequested` for a `*StopError` or for a wall time spent during the request, or `ResponseIncomplete` with any partial body and then wait 1 s, doubling per consumed attempt up to 30 s |
| `decode` | Load the saved response and commit `ResponseLoaded` with the decision the adapter extracts, or the raw body when it extracts none; never call the model |
| `bind`, `start` | Route the effect: when the frozen request of the step requires a handover and the decision is not one, commit `Observed{failed_to_start}` with the refusal as its preview, binding and running nothing; a `canti:retrieve` or `canti:handover` action binds the in-process job `read_<effect ID>` and runs in the harness; otherwise `Runner.Bind`, `Runner.Start`. A bound job's ID decides the route of the later intents |
| `inspect` | Poll `Inspect` every 500 ms. Commit `Observed` for a finished job; cancel a running job past `Budget.JobTimeout` and observe it as `timed_out`; commit a `timed_out` stop when the wall time runs out while waiting; commit `OutcomeUnknown` for a lost, missing, or foreign job, a result without an exit code, an unexpected status, or a sixth consecutive `Inspect` error |
| `cancel` | `Cancel` by effect ID; then watch a recorded job to its result, record a found job with `JobBound`, or observe a positively missing job as `cancelled` |

`Loop.Stop` commits `StopRequested` with the given reason unless the run is
closed or already records a reason, then runs it to a halt; its context
carries the cleanup deadline. `ResolveJob` records with `JobBound` the job the
executor finds for the effect of a run in `needs_resolution`, refusing without
a commit one that reports an unknown outcome, and `Resolve` records an
explicit outcome with `Resolved`. `Resolve` records the outcome
only: it leaves the backend untouched, so a job of the effect that may still
run keeps running, and a created container of the effect that never started
is not removed. Every commit handed to the store
is logged with its run ID, sequence, phase, event kind, any failure text, and
any store error; request and response bodies are not logged.

## Persistence and recovery

SQLite holds the authoritative run state and facts, and files hold large
outputs. `canti history` prints the committed transitions as a read-only JSON
Lines projection. Agent-written handover never decides whether an action
occurred.

The store contract is one atomic `Commit(Transition)`. Inside that transaction
the store verifies that `Seq` is exactly one past the last committed sequence
for the run and rejects the transition with `ErrStaleSnapshot` otherwise. The
sequence, not the state value, is the compare-and-set: a legal event can
leave the state unchanged and is still recorded, so equality of states cannot
detect a competing commit. If a record already exists at `Seq`, an identical
retry (same event ID and content) succeeds without recording again, and
conflicting content fails and never overwrites. `SemanticsVersion` and the
event `Kind` belong in each record; events are encoded by their stable kind,
never by Go type names, and a record with an unknown kind or version is
refused, not reinterpreted.

The store persists the facts attached to the event: request bytes and
provenance at `RequestFrozen` (keyed by step and revision), the response at
`ResponseSaved` (keyed by step and attempt), the observation at `Observed` or
`Resolved`, failure text and any partial response at `ResponseIncomplete`,
and failure text at `OutcomeUnknown` or `StopRequested`. It provides recovery
reads for the latest `Snapshot`, the frozen request of the current revision,
and the saved response of the current attempt. `returned` and `stopped`
accept no events, and nothing is recorded after a run closes: the usage of a
model request abandoned by a stop, for example, is never learned. No archive
outside run transitions exists; the [step records](#step-records) are a
projection of them.

`sqlite.Store` implements the `Journal` on one database file, `canti.db` in
the CLI's state directory. It bundles SQLite 3.53.4 through
`modernc.org/sqlite` v1.59.0 and opens the file with `journal_mode=WAL`,
`synchronous=FULL`, `fullfsync=1` (which SQLite applies only on macOS),
`foreign_keys=1`, a 5 s busy timeout, and immediate write transactions.

- The schema version is `user_version` 2. Opening a version-1 database adds
  the session table; another version or a foreign schema is refused with
  `ErrIncompatible`.
- `runs` holds one row per run: the immutable `harness.Run` record (goal,
  constraints, budget, creation time, opaque host config), its initial state,
  its semantics version, and running totals.
- `transitions` holds one row per committed transition, keyed by run and
  sequence: event ID, kind, semantics version, and the event, both states, and
  the attached facts as JSON. A transition and its facts are one row.
- `Commit` is one short transaction, never held across a model call or job
  wait. It refuses a transition that would not read back as committed (JSON
  replaces invalid UTF-8, which is accepted only in fact text); confirms an
  identical record at a used sequence without writing; rejects other content
  there, or a non-consecutive sequence, with `ErrStaleSnapshot`; rejects a
  `From` other than the last committed state with `ErrBrokenChain`; and adds
  the usage of saved and incomplete responses and the output bytes and count
  of observed or resolved steps to the run's totals in the same transaction.
  It writes nothing once its context is done.
- Every read decodes strictly: an unknown field, trailing data, an unknown
  kind, or a semantics version other than 1, 2, and the current 3 fails with
  `ErrIncompatible`, as does a version 1 state without positive max steps or
  a state before version 3 with a decision purpose (`core.KnownSemantics`,
  `core.Compatible`). New records are written
  with the current version, so a resumed run may mix versions.
- Each recovery read is an index search bounded by its limit, never a scan
  of the history. `Latest` and `Transitions` search the primary key; `Request`, `Response`, and `Recent`
  search partial indexes over `request_frozen` rows (run, step, revision),
  `response_saved` rows (run, step, attempt), and observed or resolved rows
  (run, sequence); `Totals` reads the run row. `Recent` pages backward by
  commit sequence: it returns the latest steps completed below a given
  sequence, each with the sequence of the transition that completed it.
- `CreateRun` and `Submit` record a new run in the same transaction that
  admits it: they refuse while any run of the journal is open or was
  abandoned with an effect whose outcome stays unknown (`harness.Blocks`,
  applied to every run's latest state and event); an identical retry of a
  recorded run is confirmed without admission. `CheckAdmission` applies the
  same rule without writing.
- Several `Store`s, in one or several processes, may share a file; the
  sequence check admits exactly one commit per sequence.

An acknowledged commit survives SIGKILL of the process, which is tested.
Survival of an OS crash or power loss depends on the filesystem and device
honouring fsync and is not tested.

The store does not lock the workspace, and a database that rejects a stale
writer does not stop that writer from touching the execution backend. The CLI
therefore takes a non-blocking `flock` on `lock` in the state directory for
every command that can commit (`run`, `resume`, `stop`, `resolve`) and holds
it until it exits; a second writer exits with status 6. `status` and `history`
read without it. The lock excludes processes that share a state directory,
which by default is derived from the workspace's resolved path; two state
directories used for one workspace do not exclude each other. It is a lock on
a pathname: a process of the same user that replaces the `lock` file defeats
it. The sequence check still rejects that process's stale commits, but not
the executor actions it takes.

Three operations must never share a name. Event replay rebuilds or verifies a
state from committed transitions and causes no external effect. Run resume
loads the latest snapshot, validates it, verifies external jobs, and continues
pending operations. Request resend calls the model again: it can cost money
and return a different response, and it may reuse only the currently frozen
bytes — building different input requires a new revision via `RequestFrozen`.

| Last committed phase | Intent on resume | Behavior |
| --- | --- | --- |
| `idle` | `prepare_request` | Build the bounded input from committed facts and freeze it as the next revision; a mid-step repair includes the decode feedback |
| `request_frozen` | `send_request` | Commit `AttemptStarted`, then send the frozen bytes |
| `request_sent` | `send_request` | The in-flight attempt is lost: commit `AttemptStarted` to count it, then resend or stop. In-process, this phase also covers an attempt that is still in flight; only a restart interprets it as lost |
| `response_saved` | `decode` | Load the saved bytes; never call the model again for this attempt |
| `effect_saved` | `bind` | `Bind` by effect ID; find the existing job before creating one; binding never executes anything |
| `job_bound` | `start` | `Start` the bound job. A crashed controller may have issued an uncommitted start, so `Start` is a no-op for a job that ever started, even a finished one |
| `awaiting_observation` | `inspect` | `Inspect` by effect ID; collect the original result and commit one observation. A recorded job that cannot be found is `OutcomeUnknown` |
| `cancelling` | `cancel` | With no job recorded, call the fencing `Cancel` by effect ID first, then `Inspect`: a positively missing job is observed as `cancelled` — sound because an unstarted bind can never execute and the fence stops a bind that lands late. An indeterminate lookup is an executor error, never a missing job. Record a found job with `JobBound`. With a job recorded, a missing or different job is `OutcomeUnknown`. Cancel the recorded job and commit its true result within a cleanup deadline |
| `needs_resolution` | `halt` | Wait for an explicitly recorded `JobBound` (resuming at `job_bound`, or at `awaiting_observation` after a committed start) or `Resolved`; never redispatch |
| `returned` | `halt` | `canti resume`, which still requires the credential, prints the saved handoff without calling the model; `canti status` shows it without one |
| `stopped` | `halt` | `canti resume` reports the stop reason, and `status` and `history` show it and the partial results |

`harness.Loop` performs these actions, bounding cleanup by the deadline of its
caller's context.

A retryable model request assumes the provider does not execute hosted tools.
Request replay can incur another charge and does not promise the same response.
A stable effect ID deduplicates job identity, not arbitrary command side effects
or a later decision with a new identity. Timeouts, cancellation, and non-zero
exit codes do not prove that no business effect occurred.

### Step records

A job can read files, not the journal. `harness.PublishHistory` therefore
publishes a derived projection of a run's committed facts under
`history/<run ID>/` in the CLI's state directory, which jobs mount read-only
(see [execution](#execution)): `goal`, `constraints` when set,
`steps/<step number in 8 digits>/` for every completed step, and `result`
once the run is closed. A step's `meta` holds one `key: value` line each,
line breaks replaced, for the step, execution, exit code when recorded,
whether it was set by explicit resolution, the output's container path or
`unavailable`, its bytes, whether it is complete, and any detail; `command`
and `preview` hold the committed command and preview verbatim, so the
command is whole where the prompt cuts it, and the preview holds the
output's head and true tail even when its artifact kept only a prefix.
`result` is `phase: returned`, a blank line, and the payload, or
`phase: stopped` and `reason: <stop reason>`. The content is a pure function
of committed facts.

Every file and step directory is written under a temporary name in the run
directory, synced, renamed into place, and its directory synced; an existing
one is never rewritten, and every publication removes stale temporaries. A
publication adds the goal, the constraints, and once the run is closed the
result when they are missing. A missing `steps` directory, as after the loss
of the run directory, is rebuilt from every completed step by paging
`Recent` backward; otherwise a publication adds at most the latest completed
step. That suffices by induction over the publication points:

- `Loop.prepare` publishes after it reads the active window steps and before it
  renders and freezes the request. When that fails it returns the error and
  freezes and commits nothing, so no frozen request names records that are
  not published. Once the request of step N is frozen, steps 1 to N-1 are
  published: step N-1 by that preparation and the others by earlier ones. A
  crash between an observation and the next freeze leaves the step to the
  next preparation.
- `Loop.Run` publishes at a halt, adding a final step that no request
  follows and the result; a failure there is only logged, because the next
  `canti run` repairs it.
- `canti run` publishes the latest run of its state directory before it
  creates a run and refuses, creating nothing, when that fails, so every
  earlier run a job sees has its final step and result published.

The records are never an authority: nothing in canti reads them back, and
`status` and `history` read the journal. In the CLI only commands holding
the workspace lock publish; concurrent publications of one run are not
supported. A publication fails when an entry it looks at has another type:
the goal, constraints, result, and files of the latest step must be regular
files, and the run directory, `steps`, and the latest step's directory real
directories, never symlinks; a stale temporary that cannot be listed or
removed fails it too. The history root and the directories above it are
resolved like the state directory, so a symlink there is followed. A process
of the same user that deletes or edits files inside a run directory is
outside the trust model, and a publication detects only what it would add: a
step directory other than the latest that such a process removes inside an
existing `steps` directory, or a file that it edits or removes inside a step
directory, stays so until the `steps` directory is removed and rebuilt.

## Lifecycle boundaries

Three lifetimes must not be conflated. A workspace is the environment and its
files; it outlives runs. A run is one autonomous progression from `NewState`
to a terminal phase; `returned` and `stopped` are final, and a user reply
after `return` starts a new run that may reuse the same workspace. The
association between runs and a caller conversation is the host's concern and
never enters the core. Waiting for approval inside a run would need an
explicit non-terminal phase; `return` must not be reused for it.

Context cancellation and run cancellation are different facts. A cancelled
`ctx` is controller shutdown: nothing is committed and a new controller
resumes from the last snapshot. A persistent stop is `StopRequested` with a
reason. An operation timeout is a harness observation reported through the
matching event for its boundary (`ResponseIncomplete`, `StopRequested`, or a
`timed_out` observation).

The CLI maps signals onto both. `run` and `resume` catch SIGINT and SIGTERM
from before they open the journal; a signal that arrives during setup is held
until the loop is built. The first signal shuts the controller down and then
calls `Loop.Stop` with `cancelled` under a new 30 s cleanup deadline, so an
effect that may exist is cancelled and its true result recorded. A second signal abandons that
cleanup, after which the CLI stops catching signals. `canti stop` performs the
same persistent stop under `-cleanup` (30 s by default). The cleanup deadline
must also cover copying the cancelled job's Docker log into its artifact, so
a job that wrote a large output needs a longer `-cleanup`. A stop that is
abandoned or misses its deadline leaves the run for a later `canti stop`. A
stop of a run in `needs_resolution` records its reason and exits with status
4: the job may still run until `canti resolve -job` records it, after which
`canti stop` cancels it. A job whose container exited without an exit record
inspects the same way every time, so `canti resolve -job` refuses it, and only
`canti resolve -execution`, after reading `docker logs canti-<effect ID>`,
moves its run on.
`run` refuses to start a run while the latest run of its state directory is
open or needs resolution, and publishes the step records of that run first,
refusing when it cannot.

## Input

Every request is prepared from committed facts and a durable handover window.
The window begins at `FirstStep`; only its completed steps enter the frame.
`Journal.Recent` remains a chronological storage query: the loop reads backward
by commit sequence, 64 records per page, stopping at the window boundary.
It does not select a sliding suffix by prompt capacity. Lost [step
records](#step-records) can still be rebuilt from the full committed history.

A `Frame` holds the authoritative goal and constraints, environment, step and
attempt positions, complete current-page trajectory, artifact and history
locations, window state, and any rejected response for protocol repair.
`Frame.Validate` checks the window boundary and contiguous step coverage.
Workspace notes are not automatically read or injected.

`Frame.Prompt` renders a system message and user content blocks. The system
message states the decision protocol of the run's transport: one JSON object
as the reply text, or exactly one `canti_decision` tool call, set by
`-decision-format`. The prompt budget charges that system message. The first
block holds Goal, Constraints, Environment, Context window, and the short
Handover when present. One block follows per current-page step, oldest first,
starting with the committed decision's purpose on one line when it has one;
the tail holds Progress, any required handover instruction, and a rejected
response. Appending a completed step preserves the prefix within a window,
since every step block, its frozen retrieval evidence included, is a
function of committed facts. A new window changes the first block.

`PromptLimits` bounds bytes, not provider tokens: defaults are 32 KiB for the
goal, 16 KiB for constraints, 2 KiB for the environment and each command,
512 bytes per step purpose, 4 KiB per output preview, 2 KiB each for a
rejected response and its error, and 128 KiB total. The CLI defaults the total
to 256 KiB. Goal and constraints are never truncated. Other bounded inputs use
explicit truncation markers, valid UTF-8, and fences longer than embedded
backtick runs. An oversized page fails visibly rather than dropping older
steps.

The loop reserves space before the window reaches its prompt capacity and
requests a bounded `canti:handover` action. Optional step and byte thresholds
can request this sooner; both zero still enables the capacity trigger. A
valid proposal becomes the next window's handover and lower history bound
when the next request is frozen. These window facts are committed with the
request bytes, so recovery reuses the same boundary and actual handover.
Invalid proposals keep the current page; repeated failure stops the run.
Run identity, effect identity, step numbering, and execution recovery do not
change when a window advances. `return` still ends the run.

Native retrieval records its reason, original paths, byte ranges, hashes,
continuation information, and errors, and commits the bytes it read for
each source with the step's observation, at most the query's byte limit.
Every later request of the window renders that frozen evidence, so a
source changed or removed after the retrieval changes no request, and the
context reserves no growth for evidence that might change. The step's
published preview keeps the references only: reading a past retrieval
returns references rather than recursively duplicating history. Frozen
request bodies still retain the actual bytes sent for exact retry. Old pages
remain accessible through explicit retrieval, including imported history
when configured. Retrieved evidence and handover text are not authoritative
instructions or proof that a side effect completed.

`anthropic.Model` implements `Model` for the non-streaming Messages API at
API version `2023-06-01`, as adapter `canti-anthropic/2`. `Freeze` builds a
deterministic body — model, `max_tokens` (16,000 by default), the system
prompt as a string, one user message with a text block per prompt block, and
optionally a JSON-schema output format for the decision object and
server-side fallbacks — that never contains credentials. The first and the
last stable block carry `ephemeral` `cache_control` breakpoints, so that a
provider that caches prompt prefixes can reuse the stable blocks of the
previous request, within the current window; actual cache reuse depends on the provider. The body
and the model, protocol, and adapter identifiers are committed with
`RequestFrozen` before any send. The base URL must use `https` unless it is a
loopback address. `Complete` sends exactly the frozen body once over
`net/http`, with no SDK, no hidden retries, and no followed redirects, under a
per-attempt timeout (10 min by default) and an 8 MiB response limit, and
replaces the credential with `[redacted]` wherever a response body echoes it.
A 2xx response is complete only with `stop_reason` `end_turn`,
`stop_sequence`, or `refusal`. Another stop reason, a malformed, truncated, or
oversized body, and statuses 408, 409, 429, and 5xx are incomplete and
retryable; 402, or 400 with a credit-balance message, is
`StopError{budget_exhausted}`; any other status or an unsupported protocol or
adapter is `StopError{unrecoverable}`. Usage is the provider-reported input
tokens, including cache creation and reads, plus output tokens. `Decision`
concatenates the text blocks.

With `-decision-format tool` the adapter is `canti-anthropic/3-tool`: the body
forces one `canti_decision` tool call whose input schema is the decision
object, instead of the output format; `tool_use` also completes a response,
and `Decision` returns the input of its single `canti_decision` call. Both
schemas require `kind`, `payload`, and `purpose`; a decision without a
purpose still decodes. The adapter identifiers name how a frozen request is
sent and its decision extracted, which the purpose did not change.

A resend reuses the frozen bytes; changing the input requires a new revision. Response records keep the raw body,
completeness, usage, and provenance, and an incomplete response keeps its
failure text. A run's configuration is stored in its run record; the code
revision is not. The configuration records the context version of the canti
that created the run, which names the rules that render its prompts and
account for their bytes. `TestContextVersionPinsItsRules` records, once per
version, what those rules produce: every request and every transition, with
its facts, of the frozen-request corpus, and every request of a few CLI runs.
A change that alters any of them needs a new version; a rule that no recorded
input reaches is not pinned. A run of another context version, or of none, is
decoded by the fields it stores and stays readable; `resolve` still records
its unknown results and `stop` closes it, since neither renders a request.
`resume` refuses it until it halts and then only reports it, either way
before it builds the model or the executor and without a commit, so no canti
continues a run under rules it was not created with. Once that run is closed,
the state directory takes new runs, unless it ended with an outcome resolved
as unknown, which keeps blocking them. A stored configuration the current one
cannot decode makes every command refuse the run; use a fresh `-state`.

## Execution

The execution backend is keyed by the stable effect ID; the `Executor`
comment states its full contract. `Bind` finds or creates the job that owns
the effect and never executes anything; a deterministic per-effect identity
makes late duplicates collide instead of running twice. Each effect has one
durable start latch, set once by `Start` or by `Cancel` and never reset. A
resumed controller re-issues `Start` from `job_bound`, so the latch survives
restarts; a naive `docker start` restarts an exited container, so the
executor gates on its own record instead. A fenced job reports `cancelled`,
which is what lets a `cancelling` run commit a true result while a stale
controller may still hold `job_bound`; `Bind` and `Start` report it as-is,
exactly as they report a job whose latch `Start` set. `executortest.Check`
exercises the latch through a fresh executor for every call, as a restarted
or competing controller would see it. A PID alone is insufficient for
recovery, and cancellation needs a bounded cleanup and result-recording phase
independent of the already-cancelled execution context.

`docker.Executor` implements the contract through the docker CLI. The job of
an effect is the container `canti-<effect ID>`, created by `Bind` with labels
naming the effect, the executor's state directory, and a SHA-256 hash of the
execution spec: image, user, network, resource limits, mounts, log options,
and command. The hash leaves out the exit-recording wrapper described under
Output, so a container that an earlier revision created, which lacks the
`canti.exit` label, still binds and is derived from Docker's exit code.
`Bind` looks for the container before creating one and rejects a different
spec hash; a container whose labels name another
effect or state directory is an error, never adopted. `Config.Mounts` adds
read-only mounts in order: `New` requires each source to be an existing
absolute directory that neither overlaps the workspace nor contains the
state directory, and each target a clean absolute path that overlaps neither
`/workspace` nor `/canti/artifacts`.

- **Latch.** The latch is a file under `jobs/<effect ID>/` in the state
  directory. It is published at most once: a temporary file is written and
  fsynced, hard-linked into place, which fails if the latch exists, and the
  directory is fsynced. Every operation on an effect holds that directory's
  `flock`, so no caller observes a half-done start.
- **Start.** `Start` sets the `start` latch and then runs `docker start` on a
  container still in `created`, finishing the launch even if its context
  ends. Once the latch is set, `Start` only reports the job.
- **Cancel.** `Cancel` sets a `cancel` latch on an unstarted effect, so that a
  container bound earlier reports `cancelled` and never starts, and a later
  `Bind` that finds no container records the job `cancelled`, with an empty
  output and the ID `fenced:<effect ID>`, without creating one. For a running
  job it writes a cancel marker and runs `docker stop` with the stop grace
  (5 s by default), after which Docker kills the container's processes. The
  marker labels the job cancelled even if it exited on its own between
  `Cancel`'s inspection and the marker write, or after a `docker stop` cut
  short by the caller's deadline.
- **Status.** A container in `created` is pending without a latch and
  cancelled under the `cancel` latch. Under the `start` latch it failed to
  start when Docker reports a start error and no start time, as for an
  unknown user, with the output detail `job never started: <daemon error>`,
  which is kept even if the capture fails or the artifact later goes
  missing, and is
  otherwise unknown, because the launch may or may not have happened; the
  loop observes a failed start as `failed_to_start`. An exited container is cancelled with
  the marker. Otherwise its exit is final only when the exit status the
  job's shell recorded agrees with Docker's exit code: succeeded for 0 and
  failed for any other. A child killed for memory while the shell survives
  leaves the shell's own exit status, which is 137 only when that child's
  status is the shell's. A container that exited without that record — killed
  by `docker kill`, or reaped by the daemon after a host restart with exit
  status 255 — reports unknown with the detail `no exit record at
  /tmp/.canti-exit: the process was killed before it could record, or the
  record could not be written`, extended when Docker observed an
  out-of-memory kill, a flag that stays set once any process of the
  container was killed; one whose record differs from Docker's exit code
  is unknown as well. A signal the script observes, such as the SIGTERM of
  a `docker stop` or of a graceful daemon shutdown, is reported as its exit
  status; only deaths the wrapper cannot record are unknown. An unknown job
  is never recorded or released: the loop commits `outcome_unknown` with the
  detail, and its container stays for inspection with
  `docker logs canti-<effect ID>` until it is removed by hand. A started effect whose container Docker reports missing, with no final
  record, is `ErrJobLost`, never "not found".
- **Output.** Jobs run `/bin/sh -c` with standard error merged into standard
  output, as a child of a wrapper shell that, once the child returns, writes
  its exit status to `/tmp/.canti-exit` in the container's writable layer,
  which outlives the container's processes. The image must give the job's
  uid a writable `/tmp`; without one every job is unknown, so the first
  step shows it. The wrapper discards its own diagnostics and leaves the job's
  environment unchanged; tini signals the whole process group, which the
  wrapper survives, so a stop signal still reaches the job's processes. The
  record is read with `docker cp`, at most 64 KiB. A job can forge it, but a
  forged record that differs from Docker's exit code is not final. Jobs run
  under Docker's json-file log driver with rotation disabled
  (`max-size=1p`) and `mode=blocking`, so no output is dropped. `Bind` and
  `Start` never read the exit record, so a job whose result needs it is
  `unknown` until `Inspect` reads it. On the first report of a finished job
  whose result is known, the executor copies `docker logs` through a tap
  that keeps the first `OutputLimit` bytes (8 MiB by default) as
  `artifacts/<effect ID>/output`, counts every byte, and keeps a preview of at
  most `PreviewLimit` bytes (4 KiB by default; the CLI sets it to the
  prompt's preview limit): the head, a `... N bytes omitted ...` marker, and
  the true tail, cut at rune boundaries. A truncated or cancelled job's output is marked incomplete,
  with details such as `kept N of M bytes`. A failed `docker logs` call or
  artifact write is recorded as an unavailable artifact with the error as
  its detail, because the report of a finished job is final. Only a capture
  cut short by the caller's context, or a final record that cannot be
  written, is left to the next report; a record that keeps failing to be
  written ends as an unknown outcome after the inspect retries. The
  json-file log replaces invalid UTF-8 with U+FFFD, so the artifact of binary
  output is not byte-exact and its byte count is that of the replaced text.
- **Release.** After the capture, the job's final record — status, exit
  code, times, job ID, spec hash, preview, and artifact reference — is
  published as `jobs/<effect ID>/output.json`, and only then
  is the container removed by its ID (`docker rm -v`), together with its
  Docker log; a failed removal is retried by the next report of the effect
  and by every `Bind` that creates a container in the state directory. From
  then on the record is the authority: `Inspect`, `Bind`, and `Start` report
  the recorded job, `Bind` still rejects a changed spec, `Cancel` does
  nothing, and a record that does not decode strictly is an error, never a
  missing job. Every report re-checks the artifact, and a missing or resized
  file makes it unavailable with a detail.

Host disk therefore holds at most `OutputLimit` bytes of artifact per job,
plus Docker's log and the writable container layer of each job that is still
running or awaiting capture, which only that job's run time bounds, and the
container of a finished job whose removal failed, until a later report of the
effect or a `Bind` that creates a container in the state directory retries
it; for the last job of a run, none may come.

Every docker CLI call has its own deadline: 30 s, plus the stop grace for
`docker stop`, and 10 min for a create, which may pull the image, and for
reading a job's log. A call past only its own deadline fails with an ordinary
error, never taken for the caller's shutdown, so a hung daemon cannot hold an
effect's lock forever: the loop retries a hung inspection and then records an
unknown outcome.

The trust boundary is Docker's. A job sees the workspace, mounted read-write
at `/workspace`, and the artifact root, mounted read-only at
`/canti/artifacts`. The CLI also mounts the [step records](#step-records)
read-only: `history` in the state directory at `/canti/runs`, creating it
before it builds the executor. The mount is the same for every command, so
a resumed `Bind` finds the same spec. The state directory itself, the
journal, latches, and job records are not mounted, and the state directory
must not overlap the workspace. No
host environment variable reaches a job: the proxy variables that the docker
CLI's configuration would inject are set empty. Jobs run with `--init`, as
the uid and gid of the canti process, with `--cap-drop ALL`,
`no-new-privileges`, network `none` unless configured, and optional memory,
CPU, and process limits (the CLI defaults to 2g of memory and 512 processes).
The docker CLI runs in its own process group, so a terminal's interrupt
reaches only canti. The CLI reads the model credential once and removes it
from its environment before starting any process; the credential is sent
only as an HTTP header, and the tests check that it appears in no request
body, CLI output, or file of the state directory.

The executor trusts the Docker daemon, anything with access to it, and any
process of the same user that can modify the state directory. Jobs have
neither: the daemon socket is not mounted, and of the state directory they
see only the artifacts and the step records, read-only. Within that
assumption, container ownership is proven only by the name and the
`canti.effect`, `canti.state`, and `canti.spec` labels, so a principal with
daemon access could plant a matching container that `Bind` adopts; docker
CLI output other than a job's exit record, and final job records, are read
without a size cap.

## Budgets

Step and attempt budgets are `core.Limits` and are enforced by `Next`:
`MaxSteps` bounds decision positions per run and `MaxAttempts` bounds
authorized sends per step, shared by lost sends, transport retries, and
protocol repairs. Neither a new revision, a restart, nor a stop-and-resume
grants new budget within a run. Wall-time, token, output-byte, and job-time
budgets are a `harness.Budget` stored with the run and measured by the loop;
zero means unlimited. A spent wall-time, token, or output-byte budget is
reported as `StopRequested` with a reason. Stop on return, cancellation,
exhausted budgets, protocol failure, or abandonment; the state preserves the
reason.

The loop checks the wall time, tokens, and output bytes before each step
except decoding a response already paid for, bounds each model request by the
remaining wall time, and checks the wall time while it waits on a running
job. The job timeout stops the job, not the run: while inspecting, the loop
cancels a job once the host clock is `Budget.JobTimeout` past the start time
the Docker daemon reports, observes it as `timed_out`, and continues with the
next step. A restarted controller that finds a cancelled job observes it as
`timed_out` when the daemon's start and finish times span the timeout. Both
assume the daemon shares the host clock. Tokens
are the provider-reported usage of saved and incomplete responses, so a run
can end up to one response over its token budget; cost is not measured. The
output-byte budget counts retained artifact bytes and is checked between
steps, so a run can end up to one per-job output limit over it. It is not a
filesystem quota: workspace writes, the journal's request and response
bodies, and Docker's log of a running job are not counted. See
[V1 acceptance](v1-acceptance.md) for the evidence.

## Complexity budget

Keep one Go module and the current package split. V1 has one model protocol,
one SQLite journal, and one queryable executor. Distributed scheduling,
multi-tenant policy, generic workflow graphs, automatic planning, automatic
memory compaction, and mid-run user input are outside the initial experiment;
so are separate response IDs and input revisions, which stay derivable from
the position identities above until a real consumer needs them. New run rules
belong in `core.State.Next` with a transition-table test, not in the harness.
