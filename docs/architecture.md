# Architecture

## Purpose and scope

canti explores a fully stateless run kernel with a separate harness that
composes the right information for each step and performs every I/O. The
implementation is a monorepo of independent Go modules: `github.com/ChrAlpha/canti/runtime`,
`github.com/ChrAlpha/canti/sdk/go`, and `github.com/ChrAlpha/canti/apps/tui`,
joined by root `go.work`. A
future Memoh adapter may consume it; canti does not import Memoh internal
packages.

V1 is one machine, one workspace writer, serial decisions, bounded model
input, and recovery from committed facts. It is implemented: a SQLite journal,
a Docker executor, Anthropic Messages and Responses adapters, and a CLI drive
runs through the core. [V1 acceptance](v1-acceptance.md) records what is verified and what
is not.

## Ownership

| Area | Owns | Evidence |
| --- | --- | --- |
| `runtime/core/decode.go` | Decision protocol and decoding | Offline tests |
| `runtime/core/state.go`, `runtime/core/event.go` | Run state, events, intents, transition rules, state invariants | Full transition-table test and trajectory tests |
| `runtime/internal/harness/frame.go`, `reader_prompt.go`, `context_state.go`, `handover.go` | The request frame, resident epoch, agent handover boundary, earlier-steps index, degradation floor and durable context anchor | Offline tests; long-history measurement over SQLite |
| `runtime/internal/harness/model.go` | Model contract: freeze, one attempt with the frozen bytes, decision extraction, `StopError` | Implemented by `anthropic` and `openai` |
| `runtime/internal/harness/store.go` | `Store` and `Journal` contracts: sequenced atomic commits, bounded recovery reads, run records, budgets, totals | Implemented by `sqlite` |
| `runtime/internal/harness/runner.go` | Transition derivation and atomic commit | Recording-store tests; used by the loop |
| `runtime/internal/harness/action.go`, `outcome.go`, `effect.go`, `perform.go` | The normalized action of an accepted exec decision and its recovery contract, the local outcome and its presentation, the provenance check of every effect transition, and the harness's own actions | Offline tests over SQLite; Docker-backed gate tests |
| `runtime/internal/harness/loop.go` | Driving committed intents to a halt, measured budgets, persistent stop, explicit resolution | Tests with a fake model and executor over SQLite; end to end through the CLI |
| `runtime/internal/harness/executor.go` | Executor contract: accepted descriptor, bounded Drive, non-starting Inspect/Cancel, authoritative receipt | Implemented by `docker` |
| `runtime/internal/harness/executortest` | Executable start latch contract | Passed by an offline reference and the Docker executor; rejects single-clause mutants |
| `runtime/internal/harness/artifacts.go` | Observation and output reference records | Types |
| `runtime/internal/harness/history.go` | The read-only step records that jobs read, derived from committed facts | Offline tests over SQLite; Docker-backed gate tests |
| `runtime/internal/archive` | Read-only access to the step records, artifacts, and imported history that jobs see: path safety, the retrieval operations, their continuations, and the evidence each returns | Offline tests on real directories |
| `runtime/internal/harness/sqlite` | The journal on one SQLite file | Tests on real database files, including SIGKILL recovery |
| `runtime/internal/harness/docker` | One container per effect through the Docker Engine API, start latch, output capture | Fake-daemon tests and Docker integration tests |
| `runtime/internal/harness/provider` | One HTTP exchange of frozen bytes, status classification, strict JSON envelopes, shared by the model adapters | Through the adapter tests |
| `runtime/internal/harness/openai` | Non-streaming Responses adapter | Tests against a local HTTP server; CLI runs against a scripted Responses server; an opt-in live test |
| `runtime/internal/harness/anthropic` | Non-streaming Anthropic Messages adapter | Tests against a local HTTP server; an opt-in live test and live runs with one model per evaluation through a Messages-compatible gateway ([V1 acceptance](v1-acceptance.md#live-provider-findings)) |
| `runtime/internal/host` | The runtime host of one state directory: its lock and journal, serial admission with input identity, drives, stop and resolution, host shutdown, progress pages and waits; the stored run config, context version, and the construction of a run's loop, model adapter, and executor from its stored provider and a given credential | Offline tests over SQLite; through the CLI tests |
| `runtime/internal/hostrpc` | The host protocol over a Unix socket in the state directory: one request and one response per connection, wire messages pinned per protocol version (version 9, with typed inputs, steering and follow-ups, observations, withdrawal, effect cancellation, opaque agent-log cursors, and handover progress), side-effect-free preflight validation, peer user checks at both ends, bounded reads, writes, answers, and waits | Offline tests over real sockets; golden wire records |
| `runtime/cmd/canti` | Operational CLI, runtime host and side-effect-free preflight; flags, signals, environment and credential | Subprocess tests; Docker-backed gate tests |
| `sdk/go/hostrpc` | Runtime-independent protocol types and Unix client, errors and opaque progress cursors | Protocol and socket tests |
| `apps/tui/cmd/canti-tui` | Flags, environment, runtime selection and composition | Startup tests; root-orchestrated app acceptance launches separate runtime and TUI executables |
| `apps/tui/internal/chat`, `ui`, `pipe`, `present`, `terminal` | Chat lifecycle and reconciliation, native editable drafts, pipe input, presentation of protocol projections, and bounded output | UI/transport tests; acceptance covers steering, follow-ups, cancellation, held-input withdrawal, retained drafts, archive navigation and output pressure |

The core imports only the standard library and holds no per-run state between
calls. The journal uses `modernc.org/sqlite`, a cgo-free SQLite implementation. Model I/O, history, time, storage, and execution belong to the
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
| Physical owner ID | The real backend owner bound to the accepted effect | Executor binding and authoritative receipts |

Derived record identities need no extra fields: request bytes belong to
(run, step, revision); a response belongs to (run, step, attempt); attempts
never reset within a step, so a late response from an earlier attempt or an
earlier step is invalid. Observations, unknown outcomes, recoveries and resolutions
name their effect; an event for a foreign effect is invalid. Delivery
identity (deduplicating one commit) is separate from causal identity and is
carried by the commit protocol below, not by core events. `RunID` must be
unique across every run that shares a store or executor namespace: effect and
event identities inherit their uniqueness from it, and a reused run ID would
let a new run adopt a previous run's job and records.

## Run state and transitions

`core.State` contains RunID, Step, Attempt, RequestRev, Phase, Limits, Decision,
EffectID and Reason. It contains no physical owner or launch state. `NewState`
starts at step/attempt 1; MaxSteps zero is unlimited, and MaxAttempts is positive.
`Intent`, `Next` and `Validate` are pure. Semantics 8 is the only readable version. `State.Inputs` counts the principal instructions bound to a run and `State.Seen` those included in the current frozen request; `RequestFrozen` sets `Seen = Inputs`, and `InputsBound` advances `Inputs` to a prefix of the run's admitted steers. A bind may name fewer steers than are admitted when a concurrent steer arrived after the read; the rest bind at the next binding point, and a response cannot return while an admitted steer is unbound. A decodable decision from a request that did not include every bound instruction is superseded: it completes the step through `advance` with no effect and no return. Core validation requires `0 <= Seen <= Inputs`.

| Phase | Intent | Legal events |
| --- | --- | --- |
| idle | prepare_request | RequestFrozen, InputsBound, StopRequested |
| request_frozen | send_request | AttemptStarted, StopRequested |
| request_sent | send_request | AttemptStarted, ResponseSaved, ResponseIncomplete, StopRequested |
| response_saved | decode | ResponseLoaded, InputsBound, StopRequested |
| effect_saved | effect | Observed, Performed, OutcomeUnknown, StopRequested |
| cancelling | cancel | Observed, OutcomeUnknown, StopRequested |
| needs_resolution | halt | EffectRecovered, Resolved, StopRequested |
| returned, stopped | halt | none |

ResponseLoaded decodes the saved response. Exec acceptance records its normalized
action and, for external work, its expected execution specification and immutable
configuration digest. Return hands control back without asserting success.
A local action records Performed with its Outcome and optional Window atomically.
An external known receipt records Observed. Both advance or exhaust the step
budget. Cancellation retains the first reason, fences work and records its true
result before stopping. OutcomeUnknown halts without authorizing repetition.
EffectRecovered returns inspected pending/running work to effect_saved or, with
a stop reason, cancelling. Inspected terminal and operator settlement use
Resolved with explicit provenance; an unknown settlement closes without erasing
uncertainty or its admission block.

Core validates effect identity and transition legality. The harness validates
accepted recovery contracts and receipts; it never restates transition rules.
The execution and persistence sections below define the evidence and crash contract.

## Interpreter and loop

Runner owns only Commit(Store): derive Next, validate attached facts, derive the
sequence-based event ID and commit one Transition. It has no executor or physical
lifecycle methods. The Loop builds model input, sends frozen bytes, decodes saved
responses, performs local actions, or samples the external executor.

External acceptance commits `ExecutionIdentity{Spec,ConfigDigest}` before any
execution. Spec is produced by the trusted adapter's pure Specification(command)
and ConfigDigest hashes the exact immutable Run.Config bytes. A descriptor for
Drive, Inspect or Cancel is reconstructed from committed acceptance. A changed
adapter configuration is refused before backend I/O.

Drive advances/reconciles one accepted effect within bounded operations. The
loop checks budgets between samples and derives its pre-latch deadline from the
remaining wall budget and caller deadline. Caller shutdown returns without a Run
stop; expiry of the harness wall deadline commits StopRequested with its cause.
After latch publication the executor finishes bounded launch work synchronously,
so Host ownership cannot be released while that work acts. Job timeout uses
backend start/finish times, or a separately recorded dispatch time when the
backend cannot supply process timestamps, and the configured duration; clock assumptions and
restart classification are explicit in the C2 contract.

Inspect and Cancel never create or launch work. Every operation returns the same
authoritative Job receipt. Pending/running samples require no Run commit. Known
terminal output, receipt and progress are committed together. Explicit inspection
of unresolved work either records EffectRecovered, records an inspected Resolved,
or retains uncertainty. Operator Resolved carries an evidence note and an explicit
physical owner for exited/timed-out assertions; it is never an inspected receipt.

## Persistence and recovery

SQLite schema 6 is current-only. A new empty database receives all tables and
indexes; other schema versions are refused without migration. State, event and
fact readers are strict, including current semantics. WAL and synchronous=FULL
retain the existing acknowledged-commit durability assumptions.

Commit checks sequence CAS and the exact predecessor state. Retrying identical
content at the same event ID/sequence succeeds without another write; conflicting
content fails. The transaction validates effect facts against committed acceptance,
request context and immutable configuration, without Docker parsing or queries.
An external receipt must match effect and accepted Spec, have truthful status and
owner/fence/exit evidence, and agree with its observation. A cancelled command
may omit its exit code only with a typed cessation witness matching its
environment binding, daemon, attachment incarnation, effect/specification, owner,
command generation and durable stop-intent identity. A stopped-but-quarantined
witness does not authorize environment reuse. The witness confirmation time is
not an actual command exit time; unknown process timestamps remain unset. A local cancellation is
the only receipt-free Observed. Performed validates a local Outcome and, for a successful handover, the closed done outcome and the head it opens in that same transaction.
Confirmed rejection commits its response, usage, typed signal, the rejected request's shape and the next head together.
The run's context_seq points to its latest successful handover or rejection; zero materializes the initial head. The run also records the rejection that opened its current head lineage; a confirmed rejection that opens no head (a terminal minimal or authoritative-orientation rejection, or attempt exhaustion) leaves that pointer and the head's carried rejection and usage unchanged.
A consistent Context read compares the expected run snapshot and resolves that pointer
within one transaction; fabricated handover, rejection or anchor provenance is rejected. Totals update atomically.

Recent and CompletionAtStep read current-format completed steps in bounded
pages/point reads. Only operator provenance sets StepRecord.Resolved; inspected settlement
renders the same result as an ordinary observation. Frozen requests, acceptance
facts, completed rows and frozen evidence never change within a supported run.

The Run has no binding or launch journal phases. Executor records own the missing
crash boundaries: acceptance before execution, binding before latch, latch before
launch, capture before terminal receipt, receipt before physical release. A lost
journal acknowledgement reuses the retained result; an unknown owner never gives
permission to create a replacement. See the C2 crash matrix for required evidence.

### Step records

A job can read files, not the journal. `harness.PublishHistory` therefore
publishes a derived projection of a run's committed facts under
`history/<run ID>/` in the CLI's state directory, which jobs mount read-only
(see [execution](#execution)): `goal`, `constraints` when set,
`steps/<step number in 8 digits>/` for every completed step, and `result`
once the run is closed. A step's `meta` holds one `key: value` line each,
line breaks replaced, for the step, execution, exit code when recorded,
whether it was set by explicit resolution, the output's container path or
`unavailable`, its bytes, whether it is complete, and any detail; `command`,
`purpose`, and `preview` hold the committed command, decision purpose, and
preview verbatim, so the command and purpose are whole where the prompt cuts
them, and the preview holds the output's head and true tail even when its
artifact kept only a prefix. `purpose` is empty when the decision had none; a
historical imported step directory may lack `purpose`. Publication repairs a
missing derived purpose from the committed decision. A step the harness performed itself ran no
process; its `meta` and `preview` present its outcome as context version 12
presented the same action, `exited` with exit code 0 when done or 1 when
failed and `failed_to_start` when refused, a rendering rule of the context
version rather than a fact. A record holding NUL or invalid UTF-8 stays verbatim, so
native retrieval refuses to read it and fails a directory search over it.
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

- `Loop.prepare` publishes after it reads the active receipts and before it
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
  earlier run a job sees has its final step and result published. Admission
  waits for a closed run's driver to finish its final publication, holding
  that run's operation lock through its own publication. Cancelling the
  waiting submission admits nothing and does not interrupt the prior driver.

Published records are derived evidence for retrieval, not an authority for
run state. `status` and `history` read the journal. In the CLI only commands holding
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
same persistent stop under `-cleanup` (30 s by default). The host keeps that
stop observable as owned work across interruption of the old drive and durable
cleanup. Waiters stay attached; they receive the stop result or its actual bounded
failure, while the interrupted drive retains its own cancellation result. This
ownership is operational state, not a new journal event. The cleanup deadline
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

An operational command drives its run through the host of the state directory: by
default (`-host auto`) the host process that serves the state directory
(`canti host`, started on demand), which outlives its clients; with
`-host embedded` one in its own process, whose lifetime is the command's, so
the signal rules above end the run with the command. The independent TUI uses
`-host auto` for shared attachment or `-host owned` for an exclusive subprocess
per input. Owned refuses an existing/shared host and checks the child PID at
handshake; it releases that host between inputs. Hangup stops the owned run;
a second signal abandons cleanup and shuts the owned host down. Abrupt client
death shuts down its child without asserting an outcome. The following detach
row applies to clients of a shared host. Three events stay distinct:

| Event | Journal | Drive | Job |
| --- | --- | --- | --- |
| A client goes away (detach): its connection closes, a second signal, a hangup, or a chat that quits mid-run | Nothing | Continues in the host | Continues |
| An explicit stop: `canti stop`, or the first signal of an attached client | `StopRequested{cancelled}` and the cancel path to the true result; the host bounds the cleanup, not the client | Interrupted, then stopped | Cancelled and recorded |
| The host shuts down: a signal to it, `canti host -stop`, or idleness | Nothing of its own: at most the commit each drive is in lands | Interrupted; nothing new is admitted or started from the moment the shutdown begins | Continues; the next host's drive inspects it |

Every submission, stop, resolution, and protocol request is an operation of
the host. A shutdown refuses new ones from its first moment, ends the
context of every drive and operation, including one still waiting for its
run's lock or for admission, which then commits nothing, and releases the
journal and the lock only after all of them have ended. A stop whose
cleanup it ends leaves the run `cancelling`, which a later stop or drive
finishes. The server first stops accepting, removes the socket, and ends its
open connections. A host that cannot end its drives and operations within
`-shutdown-timeout` exits holding the lock, so that no other host acts
beside a controller that still might. A
host holds the credentials it is given only for the drives it is given them
for, and a foreground host also those of its own environment, each bound to
an exact model API and provider. A host that starts continues the open run
it can continue by itself (`ResumeOpen`): it finishes a pending stop, which
needs no credential, or drives a run of its context version whose pair one of
its credentials is bound to. Every other open run, one of another context
version, one whose effect's outcome is unknown, or one that needs a
credential the host lacks, is left untouched, reported as not drivable with
the reason, until a client acts on it.

Readers follow one total agent log, a projection of its committed transitions
and admitted inputs: one page of at most 64 rows that never selects a request,
a response, or retrieved evidence, and that clips every text in SQL to its cap
(a command or purpose 2 KiB, a returned result 8 KiB, a preview 4 KiB, an input
text 2 KiB, any other text 2 KiB), then cuts it at a rune boundary and marks the
item `Truncated`. Every item therefore fits a page of 256 KiB, and `View` keeps
the full result. Cursors are opaque agent-log positions; items carry the
position and the run they belong to when they belong to one. Items report admitted principal inputs and
observations, instruction bindings, run starts, superseded steps, and a
withdrawal. A withdrawal is one agent-log fact at its own position: its
transaction takes one position, records it as `withdrawn_pos` on every row it
takes back, and projects one `withdrawn` item there naming up to 64 inputs by
input ID or position, marked `Truncated` when more were taken back. `Wait`
returns when the log passes the cursor, when no drive or operation owns work,
or at shutdown; the journal, not the wake-up, decides. `View` reports pending
steers, queued follow-ups, and held inputs beside the run. A protocol answer is bounded
by the JSON its valid facts take at worst, six bytes per byte of a full
result, goal, and constraints, plus 1 MiB (about 193 MiB); it is written
without HTML escaping, and the client reads no further than that bound.

## Input

Every request is prepared from committed facts and a materialized `ContextState`:
`AnchorSeq`, `FirstStep`, `Minimal` and the run's most recent `Rejection`. A run
ID is at most `MaxRunIDBytes` (200) so the archive paths a request and the
minimal notice render always fit their bounds. The anchor is the context
boundary: sequence 0 and step 1 is the initial head every run starts from, and a
successful `canti:handover` or a provider rejection opens the next one. The core
owns the generic `RequestRejected` transition; the adapters alone classify
provider overflow.

A confirmed rejection consumes an attempt. When another attempt is permitted:
a request with resident epoch steps or a protocol repair opens a head at the same
unfinished step with the same `Minimal`; a request with optional
content but no epoch step opens the degradation floor `Minimal: true`; a
rejection of authoritative orientation alone, or of a minimal context, is
terminal. Attempt exhaustion opens no head. Repeated overflow remains bounded by
the core's step-attempt limit. The rejection record keeps the shape of the
rejected request and the largest input usage any accepted request before it
reported; the commit checks the shape against the frozen request capture, so
later reads never reclassify it.

The stable head contains the exact goal and constraints, the principal
instructions bound at or before its anchor, environment, run and archive
navigation, the closed runs of the line, the earlier-steps index, and the epoch
header. The closed runs render newest first with their goal
heads, outcomes and archive paths; a closed run whose mandate was amended also
names the latest delivered instruction paths under `/canti/inputs` (at most
three paths plus a count), never their text. Everything in the head is fixed for
the whole epoch. The stable epoch renders every completed step since the anchor
in full, with each instruction bound after the anchor interleaved at its
committed sequence; it is append-only, so the provider prefix cache keeps
working. A
superseded decision renders as one block naming the instruction that set it
aside. The latest completion is rendered once: in the epoch when it belongs
there, otherwise as a one-line tail reference, omitted when the index lists it.
The replaceable tail contains observations, the execution position with
provider-reported usage, and any protocol repair. When bound instructions
exist, the execution position also carries one bounded line naming the latest
instruction by number and the step before which it was received, without
repeating its text; this changes no stable-prefix byte.

The earlier-steps index selects the newest steps before the anchor, including a
handover step, within its bounds, and displays them in ascending step order.
Older records remain in the archive. A
handover moves only the read boundary: as Bub's handoff does, it generates no summary and deletes no
history. Its note lives in the handover step record's `command`; no request
renders it automatically, and ordinary retrieval can read it. `Minimal` renders
no optional body: a fixed notice names the archive and the runs and inputs
roots. It stays minimal through provider resets and is cleared only by a
successful handover.

`PromptLimits` bounds bytes, never predicted tokens. Defaults are 32 KiB for the
goal, 16 KiB for constraints, 2 KiB for environment and command, 512 bytes per
purpose, 4 KiB per preview, 2 KiB for rejected reply/error, and an 8 MiB total
resource ceiling. `-request-bytes` can lower that ceiling. Both rendered input and
serialized provider body must fit. Goal and constraints are never truncated.
Per-field cuts, the rendered bytes of each step block, and the total request cap
are accounted separately and captured per step. Resident steps are loaded in
bounded pages while the running total is checked, so an unbounded epoch is never
materialized. Exceeding the resource ceiling fails visibly and terminally; it is
never a provider rejection. There is no token estimation, model-window table,
capacity selection or reserve.

Native retrieval commits exact observed source bytes with its outcome, including
original path, range, hash, continuation and errors. The epoch keeps every
resident retrieval, and the context capture records its operation, continuation
and every source range. `canti:handover <text>` is a local action of recovery
class local change: the note is the part of the payload after the action word and
one separator, verbatim, optional, at most 2,048 bytes and valid UTF-8. A
successful handover commits, in one `Performed` transition, the closed done
outcome, run progress and the new head; the note is stored only in the payload
and the step's `command`, and the projection derives its byte count from there.
No other action may carry a context change. Unknown `canti:` names, including
words that only begin with `canti:handover`, fail locally.

The system prompt states the decision protocol, the native retrieval syntax and
the handover contract. The mandate is the goal as amended by the principal's
instructions: where they conflict, the latest instruction wins over the goal and
earlier instructions, and constraints narrow all of them. Retrieved records,
commands and outputs are evidence; environment restrictions narrow the mandate
unless explicitly lifted. Workspace notes remain ordinary files.
Omitted history remains available through the archive.

The input archive holds exactly what the agent was shown. A `RequestFrozen`
transaction records `delivered_pos` on each input it renders for the first time;
undelivered inputs stay in the journal and progress log and have no archive
record. After that commit, the loop publishes exactly the rendered inputs,
bounded by the request, under `/canti/inputs/<12-digit position>/`, without
scanning the archive root or journal history. A record is an immutable
projection of admission facts only: position, input ID, authority, delivery,
source, subject, admitted time, and the run fixed at admission. Its directory
is written under a temporary name, synced and renamed into place, never
rewritten; a partial directory is replaced and stale temporaries for that input
are removed. A native read of a missing record repairs it from the journal when
`delivered_pos` is set and reports it absent otherwise. Each retrieval step publishes `evidence.json` plus `evidence/0000` and subsequent
files containing exactly its observed bytes. A native read of a historical
snapshot repairs missing derived files from the requested committed step before
safe archive access, even after the original source has gone. The archive owns
canonical target parsing. The harness uses a bounded `CompletionAtStep` point
lookup backed by `(run_id,json_extract(from_state,'$.Step'))` for completion kinds,
without scanning Recent or Receipts. Existing conflicting content fails integrity
checks and is never overwritten. Cancellation is checked before publication.
Publication's fast path otherwise adds only the latest step; missing run/steps
directories can still be rebuilt from bounded journal pages.

Usage is provider-reported only. Input and output each distinguish unknown from
reported zero; Anthropic input includes reported cache creation and reads. Counts
are nonnegative int64 values, checked for overflow when combined or settled.
Raw provider usage remains in the response body. Coverage counts attempts, input
and output reports, and lost attempts whose response was never durably recorded.
An observed response settles its received usage once, including error, rejection,
and terminal paths. `-token-budget` is a retrospective threshold over reported
usage; missing usage never prevents recovery and is not estimated.

A run names its model API, `anthropic-messages` or `openai-responses`, and
its model; canti has no default for either and refuses the Chat Completions
API by name. The provider is the API's base URL: a new run stores it, from
`-model-url`, the API's base URL variable, or the adapter's default, and is
driven against it only, so no later process, terminal, or host supplies
another; it must hold no user information, query, or fragment. Credentials
are never stored: a drive is given the keys of the run's API and holds them
only while it runs, and a key is sent only to the exact provider its caller
resolved, which must be the run's; otherwise the drive is refused before any
request. Both adapters build a
deterministic body that never contains credentials from the system prompt and
one user message with a text part per prompt block, describe the decision
with `harness.DecisionSchema`, and send the run's effort only when the run
names one, since each API's effort levels and default differ by model. The
body and the model, protocol, and adapter identifiers are committed with
`RequestFrozen` before any send.

`provider.Endpoint` sends exactly the frozen body once over `net/http`, with
no SDK, no hidden retries, and no followed redirects, under a per-attempt
timeout (10 min by default) and an 8 MiB response limit, and replaces the
credential with `[redacted]` wherever a response body echoes it. The base URL
must use `https` unless it is a loopback address. A malformed, truncated, or
oversized body and statuses 408, 409, 429, and 5xx are incomplete and
retryable; 402, or the API's exhausted-credit signal, is
`StopError{budget_exhausted}`; any other status or an unsupported protocol or
adapter is `StopError{unrecoverable}`.

`anthropic.Model` implements `Model` for the non-streaming Messages API at
API version `2023-06-01`, as adapter `canti-anthropic/4`: model, `max_tokens`
(16,000 by default), the system prompt as a string, text blocks, optionally an
`output_config` with the effort and a JSON-schema format for the decision
object, and, with `-fallbacks`, server-side refusal fallbacks. The first and
the last stable block carry `ephemeral` `cache_control` breakpoints, so that
a provider that caches prompt prefixes can reuse the stable blocks of the
previous request, within the current anchor; actual cache reuse depends on
the provider. Confirmed overflow requires exact, unique canonical discriminator keys: HTTP 200
`stop_reason=model_context_window_exceeded`, or HTTP 400 `invalid_request_error`
with `prompt is too long`. Other incomplete content is not confirmed overflow.
A 2xx response is complete only with `stop_reason` `end_turn`,
`stop_sequence`, or `refusal`; 400 with a credit-balance message exhausts the
budget. Usage is the provider-reported input tokens, including cache creation
and reads, plus output tokens. `Decision` concatenates the text blocks.

`openai.Model` implements `Model` for the non-streaming Responses API as
adapter `canti-openai-responses/2`, protocol `openai-responses/v1`, explicitly disabling truncation and posting to
the base URL plus `/responses`: model, the system prompt as `instructions`,
`input_text` parts, `max_output_tokens` (16,000 by default), optionally
`reasoning.effort` and a strict `json_schema` text format, and `store: false`,
since the journal keeps the response. The provider caches prompt prefixes
without breakpoints. A 2xx response is complete only with status `completed`;
`incomplete`, with its reason, and `failed` are retryable;
`insufficient_quota` exhausts the budget. Exact HTTP 400 `error.code` or
HTTP 200 failed-response `error.code` of `context_length_exceeded` confirms
overflow; `max_output_tokens` does not. Usage is `input_tokens`, which
include cached ones, plus `output_tokens`. `Decision` concatenates the
`output_text` parts of the message items, so a refusal holds no decision.

With `-decision-format tool` the adapters are `canti-anthropic/4-tool` and
`canti-openai-responses/2-tool`: the body offers one strict `canti_decision`
tool whose input schema is the decision object, instead of the output format,
with tool choice `auto` and no parallel calls. A response that calls the tool
is complete, and `Decision` returns the input of its single `canti_decision`
call; reasoning and narration may accompany it. The tool choice is `auto`
because current models reject a forced one; the system prompt asks for the
call, and a reply without it is repaired like any reply that holds no
decision. Both schemas require `kind`, `payload`, and `purpose`; a decision
without a purpose still decodes. The adapter identifiers name how a frozen
request is sent and its decision extracted, which neither the purpose nor the
tool choice changed.

A resend reuses the frozen bytes; changing the input requires a new revision. Response records keep the raw body,
completeness, usage, and provenance, and an incomplete response keeps its
failure text. A run's configuration is stored in its run record; the code
revision is not. The configuration records the context version of the canti
that created the run, which names the rules that render its prompts and
account for their bytes. `TestContextVersionPinsItsRules` records, once per
version, what those rules produce: every request and every transition, with
its facts, of the frozen-request corpus, and every request of a few CLI runs.
A rendering or accounting rule change requires the next context version; a
byte-identical refactor does not. Only the current context version is supported.
Unsupported state is rejected with a fresh-state remedy. Current-version frozen
requests retain their exact bytes and provider/API provenance across restart.

## Execution

The executor owns the physical lifecycle and its durable ordering. New runs
explicitly select persistent attachment or ephemeral execution. The host commits
the resolved attachment and mapped cwd in Run.Config before accepting commands.
Input replay returns stored facts before resolving a live environment; resumed
construction and Specification remain offline and use retained mount metadata.
The CLI owns Docker-context selection; the host reads no ambient environment.
Context 18 describes logical retrieval addresses in attached mode, and RPC 7
projects pinned environments and typed command binding/cessation evidence.
Earlier frozen context and wire records retain their original bytes.
The ephemeral Docker executor computes a canonical specification over its effective
configuration, command, wrapper and retention policy. It uses one named container per effect,
with effect/state/spec ownership labels. Configuration identity does not freeze
workspace contents, image tags or daemon defaults. DOCKER_DEFAULT_PLATFORM is
not an implicit create/pull override.

Under a per-effect flock, Drive recovers a matching created container or creates
one only before any binding/start/fence exists. It fsyncs binding.json with the
real owner before consuming the one launch latch. A recorded missing/replaced
owner is lost; it is never recreated. A create whose reply was lost is recovered
by deterministic name and exact labels before binding. Latch-before-launch loss
can remain unknown; a consumed latch is never launched again.

Cancel durably fences even never-bound work. No-owner cancellation uses an empty
owner ID, not a fabricated job. The fence prevents launch but does not relabel an
already-exited result. When cancellation observes a running owner, it publishes
an identity/owner stop witness before the stop call. That witness permits a
cancelled result after termination, including a natural exit racing the request.
Without it, wrapper exit evidence decides the result and externally killed work
without an exit record remains unknown. A running owner can truthfully be fenced.

Jobs run a shell wrapper whose child executes the command with stderr merged into
stdout; its exit record in /tmp/.canti-exit must match Docker's exit code. Missing
or contradictory evidence remains unknown. The image must provide /bin/sh and a
writable /tmp. Known terminal output is captured within OutputLimit and PreviewLimit;
publication/capture failure retains the container for retry. The terminal receipt
is published before release by real owner ID, and remains unchanged across all
later operations, including Cancel. Failed release is retried on a later sample
of that effect; no unbounded cross-effect sweep is part of Drive.

The attached executor instead pins an explicitly prepared container, local Unix
endpoint, daemon, initialization authority ID, and attachment incarnation. The
prepared container's immutable `io.canti.control` and `io.canti.authority` labels
must match its host directory and initialization ID. Missing authority is never
reset while state or evidence remains; a fresh initialization cannot reopen the
old physical container. One foreground slot survives released process locks and
controller death across run state directories. Stored mount metadata rejects
journal exposure without creating directories; dispatch also checks actual mounts.

Every command has its own real Exec ID, start epoch, command generation, local
reference/binding/latch mirrors and an authoritative record outside the writable
environment. A versioned read-only `canti-job` helper captures bounded output in
a separate writable spool. Its evidence is cross-checked with exact exec metadata;
missing metadata never permits replay. Retained terminal receipts survive later
container or authority loss. The spool stays private to the host user;
attachment proves its pinned container user can create evidence in a command
directory, and helper evidence stays collector-readable when a root command
writes it.

Raw start/stop requests are single-send and keep durable exact-request references.
Only their specific successful acknowledgements, or positive exact-exec start
evidence for a launch, settle those references. A stopped sample cannot reopen
an ambiguous environment. Ordinary acknowledged cancellation stops the whole
environment, including services, and explicit start retains its filesystem. A
quarantined environment needs explicit old-ID removal, retirement, and attachment
to a genuinely new ID. Retirement preserves old facts; it never invents a command
exit. This protocol assumes Docker lifecycle changes use the controller; the
same-user host authority and Docker daemon remain trusted.

Docker calls have their own deadlines; pre-latch calls obey the caller's earlier
deadline, while post-latch launch completion is synchronous and bounded. Inspect
is non-starting, not filesystem-read-only: it may capture, publish and release.
Host Inspect caps one physical sample at 30 seconds and includes its timestamp
and unavailability; it is separate from journal-only status and progress.

Docker's trust assumptions remain: the daemon and same-user state directory
writers are trusted. Ephemeral jobs see the workspace read-write, artifacts and step records
read-only, with no journal, latch, binding or daemon socket mount. They use init,
the configured user, dropped capabilities, no-new-privileges, networking and
resource limits. Persistent commands use the prepared environment policy and
read history, artifacts and imports through native retrieval logical addresses;
those addresses promise no shell mounts. Neither mode inherits host model credentials. Model credentials
are removed from the CLI environment before children start and are used only for
the exact provider/API binding. The executor specification checks configuration
identity; it is not a security proof against a principal with daemon access.

Retained output is bounded. Workspace writes, the journal, running Docker logs,
container writable layers and unresolved owners have no filesystem quota. Unknown
containers are retained for explicit inspection; daemon restart, power-loss and
arbitrary-effect exactly-once claims require separate evidence.

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
the backend reports (or its separate durable dispatch time), observes it as `timed_out`, and continues with the
next step. A restarted controller that finds a cancelled job observes it as
`timed_out` when its budget origin and completion or cessation confirmation span the timeout.
Backend timestamps assume the backend shares the host clock; dispatch and
cessation observations use the controller clock. Tokens
are the provider-reported usage of saved and incomplete responses, so a run
can end up to one response over its token budget; cost is not measured. The
output-byte budget counts retained artifact bytes and is checked between
steps, so a run can end up to one per-job output limit over it. It is not a
filesystem quota: workspace writes, the journal's request and response
bodies, and Docker's log of a running job are not counted. See
[V1 acceptance](v1-acceptance.md) for the evidence.

## Complexity budget

Keep runtime, SDK, and TUI in their independent Go modules. V1 has one model protocol,
one SQLite journal, and one queryable executor. Distributed scheduling,
multi-tenant policy, generic workflow graphs, automatic planning, and
automatic memory compaction are outside the initial experiment; so are
separate response IDs and input revisions, which stay derivable from
the position identities above until a real consumer needs them. New run rules
belong in `core.State.Next` with a transition-table test, not in the harness.
