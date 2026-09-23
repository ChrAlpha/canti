# Architecture

## Purpose and scope

canti explores a fully stateless run kernel with a separate harness that
composes the right information for each step and performs every I/O. The
implementation is an independent Go module, `github.com/ChrAlpha/canti`. A
future Memoh adapter may consume it; canti does not import Memoh internal
packages.

The V1 target is one machine, one workspace writer, serial decisions, bounded
model input, and recovery from committed facts. This document distinguishes
that target from the current skeleton.

## Ownership

| Area | Owns | Current implementation |
| --- | --- | --- |
| `core/decode.go` | Decision protocol and decoding | Implemented with offline tests |
| `core/state.go`, `core/event.go` | Run state, events, intents, transition rules, state invariants | Implemented with a full transition-table test and trajectory tests |
| `internal/harness/frame.go` | Input frame shape | Required goal/environment validation only |
| `internal/harness/model.go` | Independent requests and raw responses | Interface and provenance fields only |
| `internal/harness/store.go` | Sequenced atomic transition commits | Interface, snapshot, and record types only |
| `internal/harness/runner.go` | Intent interpretation: commit, bind, start | Implemented against store/executor interfaces |
| `internal/harness/loop.go` | Driving committed intents to a halt, measured budgets, persistent stop, explicit resolution | Implemented against the SQLite journal with a scripted model and an in-memory executor |
| `internal/harness/executor.go` | External jobs keyed by effect ID, bind/start split | Interface and status types only |
| `internal/harness/executortest` | Executable start latch contract for executors | Checked against an offline reference and single-clause mutants |
| `internal/harness/artifacts.go` | Observations and output references | Types only |
| `cmd/canti` | User-facing entry point | Help and development version only |

The core imports only the standard library and holds no per-run state between
calls. Model I/O, history, time, storage, and execution belong to the harness.
Retry, termination, and cancellation rules belong to the core; the harness
reports the facts that trigger them. No framework is needed between these
packages.

## Decision protocol

The public entry point is:

```go
func Decode(raw []byte) (Decision, error)
```

It accepts exactly one UTF-8 JSON object with two required, case-sensitive,
non-null string fields:

```json
{"kind":"exec","payload":"go test ./..."}
```

```json
{"kind":"return","payload":"Need a decision from the caller."}
```

`exec` requests execution in the configured environment. The core neither
executes nor authorizes the payload. Empty strings are permitted by the
protocol; execution policy belongs to the harness. `return` hands back control
and may contain a result, question, or unresolved blocker. It is not a success
flag.

The decoder rejects unknown or duplicate fields, wrong types, missing fields,
unsupported kinds, invalid UTF-8, malformed or incomplete JSON, and trailing
values. It does not extract JSON from Markdown. Transport completeness is a
separate adapter responsibility: valid JSON alone does not prove that a model
response completed.

A raw provider response and the decision payload are different layers. The
adapter records the complete raw response with its provenance
(model/protocol/adapter); it then extracts exactly one decision JSON, which
`ResponseLoaded` carries into the core and which is persisted as part of that
transition. The core never sees provider content blocks or HTTP framing.

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

## Implemented interpreter

Runs commit against a `Snapshot{Seq, State}`: the state plus the sequence of
the last committed transition. `Runner.Commit` takes the snapshot, one event,
and the facts that event permits. It checks cancellation once, validates the
state, derives the next state with `core.State.Next`, validates the facts,
and hands a single `Transition` to the store at sequence `Seq+1` with a
deterministic event ID derived from run, sequence, and event content.

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
same pair produces the same ID. A concrete store and executor must reject
conflicting content under an existing identity and accept identical retries
without creating another job.

These guarantees currently have recording fixtures. There is no concrete
backend enforcing atomic commits, durable writes, or job deduplication.

## Persistence and recovery target

SQLite should own authoritative run state and facts; files should hold large
outputs. JSONL may expose a read-only history projection. Agent-editable notes
must not become the authority for whether an action occurred.

The store contract is one atomic `Commit(Transition)`. Inside that transaction
a concrete implementation must verify that `Seq` is exactly one past the last
committed sequence for the run and reject the transition with
`ErrStaleSnapshot` otherwise. The sequence, not the state value, is the
compare-and-set: a legal event can leave the state unchanged and is still
recorded, so equality of states cannot detect a competing commit. If a record
already exists at `Seq`, an identical retry (same event ID and content) must
succeed without recording again, and conflicting content must fail and never
overwrite. `SemanticsVersion` and the event `Kind` belong in each record;
events must be encoded by their stable kind, never by Go type names, and a
record with an unknown kind or version must be refused, not reinterpreted.

The store must persist the facts attached to the event: request bytes and
provenance at `RequestFrozen` (keyed by step and revision), the response at
`ResponseSaved` (keyed by step and attempt), the observation at `Observed` or
`Resolved`, and failure text at `ResponseIncomplete`, `OutcomeUnknown`, or
`StopRequested`. It must provide recovery reads for the latest `Snapshot`, the
frozen request of the current revision, and the saved response of the current
attempt. Facts that arrive after a run is closed — the true result of a
cancelled job, late usage numbers — are archived outside run transitions;
`returned` and `stopped` accept no events and archival never reactivates a
closed run.

Three operations must never share a name. Event replay rebuilds or verifies a
state from committed transitions and causes no external effect. Run resume
loads the latest snapshot, validates it, verifies external jobs, and continues
pending operations. Request resend calls the model again: it can cost money
and return a different response, and it may reuse only the currently frozen
bytes — building different input requires a new revision via `RequestFrozen`.

Use short transactions, never one held across a model call or job wait. The
concrete store must specify crash and power-loss assumptions; SQLite WAL with
`synchronous=FULL` is the intended durability baseline, subject to filesystem
and device sync guarantees. A cross-process workspace lock must prevent two
harnesses from acting on the same pending intent; a database that rejects a
stale writer does not, by itself, stop that writer from touching the
execution backend.

| Last committed phase | Intent on resume | Required behavior |
| --- | --- | --- |
| `idle` | `prepare_request` | Build the bounded input from committed facts and freeze it as the next revision; a mid-step repair includes the decode feedback |
| `request_frozen` | `send_request` | Commit `AttemptStarted`, then send the frozen bytes |
| `request_sent` | `send_request` | The in-flight attempt is lost: commit `AttemptStarted` to count it, then resend or stop. In-process, this phase also covers an attempt that is still in flight; only a restart interprets it as lost |
| `response_saved` | `decode` | Load the saved bytes; never call the model again for this attempt |
| `effect_saved` | `bind` | `Bind` by effect ID; find the existing job before creating one; binding must not execute anything |
| `job_bound` | `start` | `Start` the bound job. A crashed controller may have issued an uncommitted start, so `Start` must be a no-op for a job that ever started, even a finished one |
| `awaiting_observation` | `inspect` | `Inspect` by effect ID; collect the original result and commit one observation. A recorded job that cannot be found is `OutcomeUnknown` |
| `cancelling` | `cancel` | With no job recorded, call the fencing `Cancel` by effect ID first, then `Inspect`: a positively missing job is observed as `cancelled` — sound because an unstarted bind can never execute and the fence stops a bind that lands late. An indeterminate lookup is an executor error, never a missing job. Record a found job with `JobBound`. With a job recorded, a missing or different job is `OutcomeUnknown`. Cancel the recorded job and commit its true result within a cleanup deadline |
| `needs_resolution` | `halt` | Wait for an explicitly recorded `JobBound` (resuming at `job_bound`, or at `awaiting_observation` after a committed start) or `Resolved`; never redispatch |
| `returned` | `halt` | Redisplay the saved handoff without restarting the agent |
| `stopped` | `halt` | Report the stop reason and partial results |

`harness.Loop` performs these actions, bounding cleanup by the deadline of its
caller's context; redisplaying a handoff or a stop is left to its host.

A retryable model request assumes the provider does not execute hosted tools.
Request replay can incur another charge and does not promise the same response.
A stable effect ID deduplicates job identity, not arbitrary command side effects
or a later decision with a new identity. Timeouts, cancellation, and non-zero
exit codes do not prove that no business effect occurred.

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

## Input and execution target

Every request should be prepared from the current goal and constraints,
environment, recent confirmed observation, bounded notes, and references to
history/artifacts. Do not read the full history only to truncate it. The current
`Frame.Validate` checks only non-empty goal and environment; it enforces no
byte, token, or history-read budget.

Freeze the final provider-facing bytes and model/protocol/adapter versions
before sending. Request slices and record structs are mutable Go values today;
immutability requires owned copies and durable storage. Credentials must remain
outside those facts. Future attempt records must capture errors, completeness,
usage, timeouts, and the configuration and code revision used for the run.
Resending a revision must reuse its frozen bytes; reading notes again or
changing input selection requires a new revision.

The execution backend is keyed by the stable effect ID; the `Executor`
comment states its full contract. `Bind` finds or creates the job that owns
the effect and must not execute anything; a deterministic per-effect identity
(for Docker, a create with a name derived from the effect ID) makes late
duplicates collide instead of running twice. Each effect has one durable
start latch, set once by `Start` or by `Cancel` and never reset. A resumed
controller re-issues `Start` from `job_bound`, so the latch must survive
restarts; a naive `docker start` restarts an exited container, so the
executor gates on its own record instead. A fenced job reports `cancelled`,
which is what lets a `cancelling` run commit a true result while a stale
controller may still hold `job_bound`; `Bind` and `Start` report it as-is,
exactly as they report a job whose latch `Start` set. `executortest.Check`
exercises the latch through a fresh executor for every call, as a restarted
or competing controller would see it. A concrete executor must bind the
effect ID to the full execution specification it runs under (backend,
workspace, working directory, interpretation of the command, resource
limits), not only the command text, and must reject changed content under an
existing identity. The backend must provide retained results, bounded output
previews, and explicit artifact availability. A PID alone is insufficient for
recovery. Cancellation needs a bounded cleanup and result-recording phase
independent of the already-cancelled execution context.

Step and attempt budgets are `core.Limits` and are enforced by `Next`:
`MaxSteps` bounds decision positions per run and `MaxAttempts` bounds
authorized sends per step, shared by lost sends, transport retries, and
protocol repairs. Neither a new revision, a restart, nor a stop-and-resume
grants new budget within a run. Wall-time, token, storage, and cost budgets
are measured by the harness and reported as `StopRequested` with a reason.
Stop on return, cancellation, exhausted budgets, protocol failure, or
abandonment; the state preserves the reason. `harness.Loop` checks the
wall-time, token, and output-byte budgets before each step and the wall time
while it waits on a job. It cancels a job past `Budget.JobTimeout` when the
backend reports the job's start time and observes it as `timed_out`. See
[V1 acceptance](v1-acceptance.md) for the required evidence.

## Complexity budget

Keep one Go module and the current package split. Start with one model protocol,
one SQLite implementation, and one queryable executor. Distributed scheduling,
multi-tenant policy, generic workflow graphs, automatic planning, automatic
memory compaction, and mid-run user input are outside the initial experiment;
so are separate response IDs and input revisions, which stay derivable from
the position identities above until a real consumer needs them. New run rules
belong in `core.State.Next` with a transition-table test, not in the harness.
