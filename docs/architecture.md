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
| `runtime/internal/harness/frame.go`, `prompt.go`, `window.go` | The frame of one request, its bounded prompt, and durable handover window | Offline tests; long-history measurement over SQLite |
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
| `runtime/internal/hostrpc` | The host protocol over a Unix socket in the state directory: one request and one response per connection, wire messages pinned per protocol version (version 5, with side-effect-free preflight validation and opaque progress cursors), opaque run-bound cursors, peer user checks at both ends, bounded reads, writes, answers, and waits | Offline tests over real sockets; golden wire records |
| `runtime/cmd/canti` | Operational CLI, runtime host and side-effect-free preflight; flags, signals, environment and credential | Subprocess tests; Docker-backed gate tests |
| `sdk/go/hostrpc` | Runtime-independent protocol types and Unix client, errors and opaque progress cursors | Protocol and socket tests |
| `apps/tui/cmd/canti-tui` | Terminal and pipe chat, runtime selection and launch, rendering protocol projections | Pure UI tests; runtime end-to-end tests launch the separate TUI executable |

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
`Intent`, `Next` and `Validate` are pure. Semantics 5 is the only readable version.

| Phase | Intent | Legal events |
| --- | --- | --- |
| idle | prepare_request | RequestFrozen, StopRequested |
| request_frozen | send_request | AttemptStarted, StopRequested |
| request_sent | send_request | AttemptStarted, ResponseSaved, ResponseIncomplete, StopRequested |
| response_saved | decode | ResponseLoaded, StopRequested |
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
backend start/finish times and the configured duration; clock assumptions and
restart classification are explicit in the C2 contract.

Inspect and Cancel never create or launch work. Every operation returns the same
authoritative Job receipt. Pending/running samples require no Run commit. Known
terminal output, receipt and progress are committed together. Explicit inspection
of unresolved work either records EffectRecovered, records an inspected Resolved,
or retains uncertainty. Operator Resolved carries an evidence note and an explicit
physical owner for exited/timed-out assertions; it is never an inspected receipt.

## Persistence and recovery

SQLite schema 4 is current-only. A new empty database receives all tables and
indexes; other schema versions are refused without migration. State, event and
fact readers are strict, including current semantics. WAL and synchronous=FULL
retain the existing acknowledged-commit durability assumptions.

Commit checks sequence CAS and the exact predecessor state. Retrying identical
content at the same event ID/sequence succeeds without another write; conflicting
content fails. The transaction validates effect facts against committed acceptance,
request window and immutable configuration, without Docker parsing or queries.
An external receipt must match effect and accepted Spec, have truthful status and
owner/fence/exit evidence, and agree with its observation. A local cancellation is
the only receipt-free Observed. Performed validates the local Outcome and next
Window, including carry sources, in that same transaction. Totals update atomically.

Recent and Completion read current-format completed steps in bounded pages/point
reads. Only operator provenance sets StepRecord.Resolved; inspected settlement
renders the same result as an ordinary observation. Frozen requests, acceptance
facts, completed rows and carry evidence never change within a supported run.

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

Readers follow a run through bounded progress pages, a projection of its
committed transitions: one primary-key read of at most 64 rows that never
selects a request, a response, or retrieved evidence, and that clips every
text in SQL to its cap (a command or purpose 2 KiB, a returned result
8 KiB, a preview 4 KiB, any other text 2 KiB), then cuts it at a rune
boundary and marks the item `Truncated`. Every item therefore fits a page
of 256 KiB, and `View` keeps the full result. A protocol answer is bounded
by the JSON its valid facts take at worst, six bytes per byte of a full
result, goal, and constraints, plus 1 MiB (about 55 MiB); it is written
without HTML escaping, and the client reads no further than that bound.

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

A run of a session also quotes, oldest first, the earlier runs of its
session that it selects newest first while they fit: each one's goal and
return, or a stopped run's reason and step count with its goal where that
fits, after a line that points to older records. Where room remains after
that selection, the quoted runs, newest first, also name the path of the
command of their latest accepted handover: the final step when that was
one, otherwise the source of the window of the latest request. A path never
displaces dialogue, and the handover it names is agent-written and may be
overtaken by later steps of that run.

`Frame.Prompt` renders a system message and user content blocks. The system
message states the decision protocol of the run's transport: one JSON object
as the reply text, or exactly one `canti_decision` tool call, set by
`-decision-format`. With retrieval enabled it also states the retrieval
action: its syntax, operations, continuation, and rules whose violation no
error would report, such as that a directory search reads only the regular
files directly inside its directory and that a partial scan is not proof
of absence. A directory search that fails at one file and a cursor that
does not decode, belongs to another query, or names a file, step field, or
history that is gone name the next valid action in their errors instead.
It ranks what a request shows: the current goal and constraints are
authoritative; command output, retrieved records, earlier dialogue, and the
handover are evidence that never widens them; and a restriction the
environment of the run states about itself narrows them unless the goal or
constraints explicitly lift it by naming it or what it withholds, so the agent
follows it, including how it says to record what it withholds, continues the
work it allows, reports the conflict on return, and never acts merely because
an output asks it to.
The prompt budget charges that system message. The first
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
requests a bounded `canti:handover` action, whose payload is that name and
one space, tab, or line break followed by the handover as plain text, kept
verbatim; the harness, not the model, names the run and the window it closes.
Every payload whose first word, after leading whitespace, begins with
`canti:` is a harness action, never a command: an unknown name, such as a
misspelling, fails as an action.
The request asks the handover to carry the objective, the restrictions the
environment has stated and where, the next action, risks, and evidence
references.
A proposal fails for empty, invalid UTF-8, or oversized text, when no
handover was requested, or when the window it opens would leave no work
capacity; at a requested handover, any other exec is refused unrun and
counts as a failed proposal, and discovery skips every step whose command,
after leading whitespace, begins with `canti:` or the discovery marker,
skipping and reporting as unclassified one whose leading whitespace it
cannot see past within its bound.
Optional step and byte thresholds
can request this sooner; both zero still enables the capacity trigger. A
valid proposal opens the next window in the transition that records it: one
`Performed` commits the outcome, the window that begins at the next step
with the proposal as its handover, and the move to that step. A failed or
refused proposal commits, in the same way, the current window with one more
failure, keeping the current page; the next request says that its step is
the last chance to hand over, and a second failure stops the run. The next
request takes the window committed with the previous step, or else the
window of that step's request, and commits it with its bytes, so recovery
reuses the same boundary and actual handover.
Run identity, effect identity, step numbering, and execution recovery do not
change when a window advances. `return` still ends the run.

Native retrieval records its reason, original paths, byte ranges, hashes,
continuation information, and errors, and commits the bytes it read for
each source with the step's observation, at most the query's byte limit.
Every later request of the window renders that frozen evidence, so a
source changed or removed after the retrieval changes no request, and the
context reserves no growth for evidence that might change. The step's
published preview keeps the references only. Each retrieval step also publishes
`evidence.json`, with its transition sequence, source indexes, original ranges,
hashes and continuation, plus `evidence/0000` and subsequent files containing
the exact observed bytes. Missing derived evidence is repaired from the
journal; conflicting content is an integrity failure. Frozen
request bodies still retain the actual bytes sent for exact retry. Old pages
remain accessible through explicit retrieval, including imported history
when configured. Retrieved evidence and handover text are not authoritative
instructions or proof that a side effect completed.

With retrieval enabled, a required handover may instead be
`canti:handover-carry` followed by one JSON object of `text` and `carry`,
which keeps committed retrieval evidence in the next window; it is its own
action, `handover_carry`, decided by its word, and the plain handover and
its text are unchanged. Each entry names a retrieval step of the window, or
a step the window carries, and optionally one of its sources, by the
number that the evidence of each retrieval shows before every range. Entries
expand, before any evidence is read, into at
most 32 references to committed source ranges: the sequence of the
transition that performed the retrieval, the step, the source index, and
the length and SHA-256 of the bytes. A range carried again keeps its
original identity. An exact repeat fails the proposal, but two ranges of one
path, overlapping or of an older and a newer snapshot, are both kept and
both charged. The window's first block renders the carried ranges under
"Carried evidence", ordered by sequence and source, each with its step,
source, path, range, and hash, as read at its step, and with whether its
read continues, whether it is a search or discovery candidate, and whether
its scan was partial. Nothing reads their sources again: every request
resolves each reference by one primary-key read of the committed transition
(`Journal.Completion`) and checks its length and hash. A carry is admitted
only when the window it opens, rendered as its first request renders it,
earlier session dialogue included, keeps positive work capacity beyond the
step that the reserve always fits; otherwise, like a selection the window
cannot satisfy, it fails as a proposal, with the bytes in its detail, which
counts as a failed handover. Committed evidence that no longer matches its
reference is a journal integrity failure that stops the run `unrecoverable`,
at the carry and in any request of a window that carries it, and never makes
the agent choose again. An unrequested carry fails without changing the
window. Context version 14 adds the carry, with a handover instruction
that teaches it and grows the reserve by 608 bytes. Version 15 numbers each
range: a retrieval's evidence names each path on a `Path` line and each
range as `Source I: bytes [a,b), sha256 …`; its shorter instruction leaves
the reserve 465 bytes above version 13's. Current context version 16 also
renders the physical exit code of started cancelled/timed-out effects; other
request bytes retain the version-15 rendering rules.

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
previous request, within the current window; actual cache reuse depends on
the provider. A 2xx response is complete only with `stop_reason` `end_turn`,
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
`insufficient_quota` exhausts the budget. Usage is `input_tokens`, which
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

The executor owns the physical lifecycle and its durable ordering.
Docker computes a canonical specification over its effective configuration,
command, wrapper and retention policy. It uses one named container per effect,
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

Docker calls have their own deadlines; pre-latch calls obey the caller's earlier
deadline, while post-latch launch completion is synchronous and bounded. Inspect
is non-starting, not filesystem-read-only: it may capture, publish and release.
Host Inspect caps one physical sample at 30 seconds and includes its timestamp
and unavailability; it is separate from journal-only status and progress.

Docker's trust assumptions remain: the daemon and same-user state directory
writers are trusted. Jobs see the workspace read-write, artifacts and step records
read-only, with no journal, latch, binding or daemon socket mount. No host credential
enters a job. Jobs use init, the configured user, dropped capabilities,
no-new-privileges, configured networking and resource limits. Model credentials
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

Keep runtime, SDK, and TUI in their independent Go modules. V1 has one model protocol,
one SQLite journal, and one queryable executor. Distributed scheduling,
multi-tenant policy, generic workflow graphs, automatic planning, automatic
memory compaction, and mid-run user input are outside the initial experiment;
so are separate response IDs and input revisions, which stay derivable from
the position identities above until a real consumer needs them. New run rules
belong in `core.State.Next` with a transition-table test, not in the harness.
