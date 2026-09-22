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
| `core/state.go`, `core/event.go` | Run state, events, intents, transition rules | Implemented with a full transition-table test |
| `internal/harness/frame.go` | Input frame shape | Required goal/environment validation only |
| `internal/harness/model.go` | Independent requests and raw responses | Interface and provenance fields only |
| `internal/harness/store.go` | Atomic transition commits | Interface and record types only |
| `internal/harness/runner.go` | Intent interpretation: commit and dispatch | Implemented against store/executor interfaces |
| `internal/harness/executor.go` | External jobs keyed by effect ID | Interface and status types only |
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

## Run state and transitions

`core.State` is the complete projection a harness needs to continue a run:

```go
type State struct {
	RunID    string
	Step     int
	Attempt  int
	Phase    Phase
	Limits   Limits
	Decision Decision
	EffectID string
	JobID    string
	Reason   StopReason
}
```

`NewState` starts at step 1 in `idle` and requires positive step and attempt
limits. `State.Intent` maps the committed phase to the single action the
harness may take next. `State.Next` applies one event and returns the new
state, or the unchanged state with `ErrInvalidEvent` or `ErrIllegalTransition`.
Both are pure functions of their inputs. A legal event may leave the state
equal to its input; the harness still records it.

| Phase | Intent | Legal events |
| --- | --- | --- |
| `idle` | `prepare_request` | `RequestFrozen`, `StopRequested` |
| `request_frozen` | `send_request` | `AttemptStarted`, `StopRequested` |
| `request_sent` | `send_request` | `AttemptStarted`, `ResponseSaved`, `ResponseIncomplete`, `StopRequested` |
| `response_saved` | `decode` | `ResponseLoaded`, `StopRequested` |
| `effect_saved` | `dispatch` | `JobStarted`, `Observed`, `OutcomeUnknown`, `StopRequested` |
| `awaiting_observation` | `inspect` | `Observed`, `OutcomeUnknown`, `StopRequested` |
| `cancelling` | `cancel` | `JobStarted`, `Observed`, `OutcomeUnknown`, `StopRequested` |
| `needs_resolution` | `halt` | `Resolved`, `StopRequested` |
| `returned`, `stopped`, unknown | `halt` | none |

Rules the table enforces:

- Every I/O is preceded by a committed phase that authorizes it. A restart
  resumes by asking `Intent` again, so recovery and fresh steps share one path.
- `AttemptStarted` is committed before each model send. In `request_sent` it
  means the previous send never reported a result, so it consumes an attempt;
  sends are therefore counted even when a crash loses their outcome. The bound
  is on authorized sends: a crash between the `AttemptStarted` commit and the
  send consumes an attempt without sending.
- `ResponseIncomplete` and an undecodable response return to `request_frozen`
  as a new attempt until `MaxAttempts`, then stop with `attempts_exhausted` or
  `protocol_failure`. An empty response body is an invalid event, not a retry.
- An `exec` decision commits `effect_saved` with an effect ID derived from the
  run ID and step. A `return` decision closes the run as `returned`.
- `Observed` completes the step: the run continues in `idle` at the next step
  with identities cleared, or stops with `steps_exhausted`. `Observed` rejects
  the `unknown` execution state, which must be reported as `OutcomeUnknown`.
  Without a recorded job, `effect_saved` accepts only `failed_to_start` and
  `cancelling` only `cancelled`; any other outcome must first record its job
  with `JobStarted`.
- `OutcomeUnknown` enters `needs_resolution`; only an explicit `Resolved`
  leaves it, continuing the run or completing a pending stop.
- `StopRequested` stops immediately while no effect exists and enters
  `cancelling` once one may exist. `cancelling` records the true result before
  the run becomes `stopped`. In `cancelling` and `needs_resolution` a stop
  request is recorded without changing phase; the first reason is kept.
- `JobStarted` requires a job ID and must match a job already known.
- Adding a capability must not add an event kind; capabilities are payloads.

## Implemented interpreter

`Runner.Commit` takes the committed state, one event, and the facts that event
permits. It checks cancellation once, derives the next state with
`core.State.Next`, validates the facts, and hands a single `Transition` to the
store. Any error returns the unchanged state; nothing is written. Core errors
keep their sentinels; fact errors wrap `ErrFacts`.

```text
Commit:   check cancellation -> Next -> check facts -> Store.Commit(Transition)
Dispatch: require dispatch intent -> check cancellation -> Executor.StartOrResume
          -> Commit(JobStarted) | Commit(OutcomeUnknown) | return cancellation
```

Each event permits a fixed set of facts and anything else is rejected:
`RequestFrozen` requires request bytes; `ResponseSaved` a complete response
with a body; `ResponseIncomplete` may carry the partial response and failure
text; `Observed` and `Resolved` an observation whose execution state matches
the event; `OutcomeUnknown` failure text. Facts learned after the harness's
own context was cancelled must be committed with a separate context, because
`Commit` writes nothing once its context is done.

`Runner.Dispatch` runs only from `effect_saved` and hands the committed effect
ID and decision payload to the executor. A returned job ID commits
`JobStarted` whatever its status snapshot, because `Inspect` is the authority
on the outcome. An executor error or a job without an ID commits
`OutcomeUnknown` with the failure text; if that commit fails, the returned
error wraps both causes and the state stays `effect_saved`. A cancellation
observed around the call returns without committing, so a resume finds the job
by effect ID. The executor must still honor cancellation before creating a
job; a check in the runner cannot close that race.

An effect ID hashes a length-prefixed run ID followed by the step number. The
same pair produces the same ID. A concrete store and executor must reject
conflicting content under an existing identity and accept identical retries
without starting another job.

These guarantees currently have recording fixtures. There is no concrete
backend enforcing atomic commits, durable writes, or job deduplication.

## Persistence and recovery target

SQLite should own authoritative run state and facts; files should hold large
outputs. JSONL may expose a read-only history projection. Agent-editable notes
must not become the authority for whether an action occurred.

The store contract is one atomic `Commit(Transition)`. Inside that transaction
a concrete implementation must verify that the durable state equals `From` and
reject the transition otherwise; this compare-and-set is what keeps one pending
intent per run when a stale or competing harness commits. `To` may equal
`From`, because a legal event can leave the state unchanged and is still
recorded, so enforce the check against the current state rather than as a
uniqueness rule on `From`. The store must persist the new state and the facts
attached to the event: request bytes and provenance at `RequestFrozen`, the
response at `ResponseSaved`, the observation at `Observed` or `Resolved`, and
failure text at `ResponseIncomplete` or `OutcomeUnknown`. It must also provide
recovery reads for the last committed state, the frozen request, and the saved
response. The current interface defines the commit but no recovery reads and
no workspace-lock API. `core.State` is the recovery projection; loading it and
re-deriving the intent is the resume path, but no loader or loop exists yet.

Use short transactions, never one held across a model call or job wait. The
concrete store must specify crash and power-loss assumptions; SQLite WAL with
`synchronous=FULL` is the intended durability baseline, subject to filesystem
and device sync guarantees. A cross-process workspace lock must prevent two
harnesses from acting on the same pending intent.

| Last committed phase | Intent on resume | Required behavior, not implemented yet |
| --- | --- | --- |
| `idle` | `prepare_request` | Build the bounded input from committed facts and freeze it |
| `request_frozen` | `send_request` | Commit `AttemptStarted`, then send the frozen bytes |
| `request_sent` | `send_request` | The in-flight attempt is lost: commit `AttemptStarted` to count it, then resend or stop |
| `response_saved` | `decode` | Load the saved bytes; never call the model again for this attempt |
| `effect_saved` | `dispatch` | `StartOrResume` by effect ID; find the existing job before creating one |
| `awaiting_observation` | `inspect` | `Inspect` by effect ID; collect the original result and commit one observation. A recorded job that cannot be found is `OutcomeUnknown` |
| `cancelling` | `cancel` | `Inspect` by effect ID. With no job recorded, observe a missing job as `cancelled` and record a found job with `JobStarted`. With a job recorded, a missing or different job is `OutcomeUnknown`. Cancel the recorded job and commit its true result within a cleanup deadline |
| `needs_resolution` | `halt` | Wait for an explicitly recorded `Resolved`; never redispatch |
| `returned` | `halt` | Redisplay the saved handoff without restarting the agent |
| `stopped` | `halt` | Report the stop reason and partial results |

A retryable model request assumes the provider does not execute hosted tools.
Request replay can incur another charge and does not promise the same response.
A stable effect ID deduplicates job identity, not arbitrary command side effects
or a later decision with a new identity. Timeouts, cancellation, and non-zero
exit codes do not prove that no business effect occurred.

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

The execution backend is keyed by the stable effect ID. `StartOrResume` finds
or starts the job; `Inspect` and `Cancel` never create one, and `Cancel` is
idempotent because the `cancelling` resume path may call it again. The backend
must provide retained results, bounded output previews, and explicit artifact
availability. A PID alone is insufficient for recovery. Cancellation needs a
bounded cleanup and result-recording phase independent of the already-cancelled
execution context.

Step and attempt budgets are `core.Limits` and are enforced by `Next`.
Wall-time, token, storage, and cost budgets are measured by the harness and
reported as `StopRequested` with a reason. Stop on return, cancellation,
exhausted budgets, protocol failure, or `needs_resolution`; the state preserves
the reason. No loop, model adapter, budget measurement, sandbox, or cleanup
implementation exists yet. See [V1 acceptance](v1-acceptance.md) for the
required evidence.

## Complexity budget

Keep one Go module and the current package split. Start with one model protocol,
one SQLite implementation, and one queryable executor. Distributed scheduling,
multi-tenant policy, generic workflow graphs, automatic planning, and automatic
memory compaction are outside the initial experiment. New run rules belong in
`core.State.Next` with a transition-table test, not in the harness.
