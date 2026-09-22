# Architecture

## Purpose and scope

canti explores a fully stateless decision protocol with a separate harness
that composes the right information for each step. The implementation is an
independent Go module, `github.com/ChrAlpha/canti`. A future Memoh adapter may
consume it; canti does not import Memoh internal packages.

The V1 target is one machine, one workspace writer, serial decisions, bounded
model input, and recovery from committed facts. This document distinguishes
that target from the current skeleton.

## Ownership

| Area | Owns | Current implementation |
| --- | --- | --- |
| `core` | Decision protocol and decoding | Implemented with offline tests |
| `internal/harness/frame.go` | Input frame shape | Required goal/environment validation only |
| `internal/harness/model.go` | Independent requests and raw responses | Interface and provenance fields only |
| `internal/harness/store.go` | Authoritative facts | Write interfaces and records only |
| `internal/harness/runner.go` | Decision-to-effect ordering | Implemented against store/executor interfaces |
| `internal/harness/executor.go` | Queryable external jobs | Interface and status types only |
| `internal/harness/recovery.go` | Recovery projection and phases | Types and a phase allowlist only |
| `internal/harness/artifacts.go` | Observations and output references | Types only |
| `cmd/canti` | User-facing entry point | Help and development version only |

The core imports only the standard library and holds no per-run state. Model
I/O, history, retries, time, storage, execution, and cancellation belong to the
harness. No framework is needed between these packages.

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

## Implemented decision boundary

`Runner.ApplyDecision` requires a complete response and non-empty run/step IDs.
Its caller must already have saved the raw response. The runner cannot verify
that prerequisite through the current write-only `Store` interface.

```text
check cancellation and completeness -> decode -> validate IDs -> SaveDecision
  -> check cancellation
  -> return: SaveReturn -> return control
  -> exec: SaveEffect -> check cancellation -> StartOrResume
```

Any failed write prevents the following operation. Store and executor errors
remain wrapped so callers can inspect their causes. Cancellation observed after
a decision or effect write stops progression while preserving that committed
fact. The executor must also check cancellation before creating a job; a check
in the runner cannot eliminate the race at the external dispatch boundary.

An effect ID hashes a length-prefixed run ID followed by the step ID. The same
pair produces the same ID; embedded separators cannot change the pair's
encoding. The command is stored separately. A concrete store and executor must
reject conflicting content under an existing identity and accept identical
retries without starting another job.

These guarantees currently have recording fixtures. There is no concrete
backend enforcing immutable records, durable writes, or job deduplication.

## Persistence and recovery target

SQLite should own authoritative run state and facts; files should hold large
outputs. JSONL may expose a read-only history projection. Agent-editable notes
must not become the authority for whether an action occurred.

Before implementing storage, establish transaction operations for:

- a decision plus its execution intent or return state;
- an observation plus the corresponding run progress update; and
- immutable request/attempt and response records with recoverable identities.

The current `Store` exposes separate save methods, no recovery reads, and no
transaction or workspace-lock API. It does not yet meet these requirements.
`RunState` is a prospective projection; `Resumable` merely permits known active
phases when no input is needed. It neither validates facts nor resumes a run.

Use short transactions, never one held across a model call or job wait. The
concrete store must specify crash and power-loss assumptions; SQLite WAL with
`synchronous=FULL` is the intended durability baseline, subject to filesystem
and device sync guarantees. A cross-process workspace lock must prevent two
harnesses from dispatching the same pending work.

| Last confirmed facts | Required recovery behavior, not implemented yet |
| --- | --- |
| Frozen request, no complete response | Resend identical request with a new recorded attempt and finite retry budget |
| Complete response, no decision | Decode the saved bytes without another model call |
| Committed decision and effect | Look up the effect's job before creating work |
| Dispatch attempted, outcome uncertain | Inspect the existing job; preserve uncertainty if it cannot be resolved |
| Job ended and output is available | Collect its original result and commit the observation |
| Observation and progress committed | Construct the next bounded input from those facts |
| Return committed | Redisplay the saved handoff without restarting the agent |
| Unverifiable external action | Enter `needs_resolution` and wait for an explicitly recorded resolution |

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

The execution backend must provide stable job lookup, cancellation, retained
results, bounded output previews, and explicit artifact availability. A PID
alone is insufficient for recovery. Cancellation needs a bounded cleanup and
result-recording phase independent of the already-cancelled execution context.

Define finite step, attempt, wall-time, input/output, storage, and model-usage
budgets before adding a loop. Stop on return, cancellation, exhausted budgets,
unrecoverable protocol failures, or `needs_resolution`; preserve the reason.
No loop, automatic retries, budgets, sandbox, or cleanup implementation exists
yet. See [V1 acceptance](v1-acceptance.md) for the required evidence.

## Complexity budget

Keep one Go module and the current package split. Start with one model protocol,
one SQLite implementation, and one queryable executor. Distributed scheduling,
multi-tenant policy, generic workflow graphs, automatic planning, and automatic
memory compaction are outside the initial experiment.
