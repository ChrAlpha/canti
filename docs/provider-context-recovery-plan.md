# Provider-driven context recovery: implementation plan

Status: implementation specification proposed from the agreed direction.
Date: 2026-11-08. Source baseline: canti
`a7e40fa244dae1d4ded178dabb559749f2659aeb`.
The [research report](provider-context-recovery-research.md) contains the Bub,
RLM and provider evidence. This document defines what to build and how to
validate it through canti's own execution.

## Delivery decision and feasibility

The user has chosen **build, then dogfood**, without a comparative-evaluation
gate. The required behavior is provider-only token accounting, automatic
context reset after confirmed provider overflow, and fresh-context retrieval
of original history. Generating a predecessor summary is not a prerequisite
for reset or continuation.

This is feasible within the existing architecture. SQLite already commits
requests and results atomically; the archive already supports bounded reads,
discovery and programmatic filesystem access; retrieval already freezes exact
source bytes; local actions already have a replayable transaction boundary.
The changes are substantial because they cross request recovery, context
provenance, storage and the public configuration protocol. No new model API,
database, tokenizer, vector index or general workflow framework is needed.

The following are concrete implementation choices, rather than claims already
implemented: the `RequestRejected` event, `ContextState`, `canti:focus`, archive
evidence manifests, and the resource limits below. All form one delivery.
Commit boundaries in the sequence below are for review and buildability.

The machine currently has Go `1.26.10` and a reachable Docker server
`29.9.1-1`. Live verification additionally needs an explicitly selected API,
provider and model, and a credential injected through the existing CLI path.
There is no default API/model and no dependency on reading environment files.

## User-visible contract

- Report only received provider usage. Missing usage remains unknown. Keep
  output-token limits and retrospective run budgets separate from context fit.
- Send frozen requests without predicting their tokens or consulting model
  window-size tables. Only a recognized provider context-limit signal starts
  automatic reset. Ordinary transport retry keeps identical frozen bytes.
- On reset, commit a new anchor and keep the same unfinished decision step.
  Start the next request from exact task/constraints, runtime position and
  history entry points. Preserve original records and previous request bytes.
- Let the new context discover and read historical evidence as needed.
  Reading evidence once does not retain its body in every subsequent request.
- Show an actual context-reset event and usage coverage in CLI/TUI diagnostics.
  Preserve unresolved effect state and cancellation behavior across reset.

## Core transition and durable facts

Add the generic event `RequestRejected{Step, Attempt}`. Its meaning is that
this model attempt ended and the next attempt requires newly prepared input.
It contains no anchor, retrieval, model-capacity or provider-specific fields.

| From | Event | Result |
| --- | --- | --- |
| `request_sent` | `RequestRejected` with matching identity and another attempt available | `idle`, same step, increment attempt, retain current request revision until the next `RequestFrozen` increments it |
| `request_sent` | `RequestRejected` at the attempt limit | `stopped/attempts_exhausted`; no new context is opened |
| `request_sent` | Existing `ResponseIncomplete` | Existing resend behavior, same frozen revision |
| `response_saved` | Invalid `ResponseLoaded` | Existing protocol-repair behavior, distinguished from rejection |

`core.State.Next` owns those rules and limits. `prepare` must select its reason
from committed facts: first request, protocol repair, or rejected-request
rebuild. `RequestRev > 0` alone must no longer mean "decode the previous
response as a malformed agent decision".

Use a concrete harness projection:

```go
type ContextState struct {
    AnchorSeq int64
    FirstStep int
    Focus     []EvidenceRef
}
```

The initial value is `{AnchorSeq: 0, FirstStep: 1}`. A committed overflow reset
uses its own transition sequence as `AnchorSeq`, the unfinished step as
`FirstStep`, and an empty focus set. The anchor's origin is the rejection
transition, not a made-up predecessor handover step. Its previous history is
everything committed before that sequence. The rejection fact names the
previous context head and the provider signal; event identity names the
failed step and attempt. `Request` freezes this context projection alongside
the rendered input and its diagnostic capture.

**Reset commit:** one transaction records `RequestRejected`, the sanitized
provider response/signal, any reported usage, and the new `ContextState` when
core permits continuation. If attempts are exhausted, persist the rejection
and usage but omit the new context state. Do not fabricate a tool completion,
advance the task step or execute partial model content.

Add a materialized `runs.context_seq` pointer to the latest transition that
changes context. Update it in the same transaction as reset or focus changes.
`Journal.Context` dereferences that transition by primary key; zero means the
initial context. The transition remains authoritative. Validate the pointer,
the permitted mutation and the request's context against the current journal
head. Read the pointer/context consistently with the expected snapshot in one
transaction; a newer journal head is a stale-snapshot error, not permission
to combine old run state with new context. Context recovery must not scan a
run's complete history.

## Provider errors and accounting

Keep the existing `Model` boundary. Add a typed `ContextLimitError` carrying
the recognized provider signal: HTTP status, error type/code or stop reason,
and request ID when available. Return the raw sanitized response through
`Response`, as today; do not turn arbitrary exception text into a reset.

| Provider signal | Action |
| --- | --- |
| Anthropic structured 400 `invalid_request_error` with its documented prompt-too-long form | Reject and rebuild |
| Anthropic 200 `model_context_window_exceeded` | Reject and rebuild; retain usage and partial content, execute none of it |
| Responses structured `context_length_exceeded`, including a failed-response envelope where the endpoint supplies it | Reject and rebuild |
| Responses `incomplete_details.reason=max_output_tokens` | Existing bounded incomplete-response handling; this signal alone does not establish overflow |
| Generic 400, 413, 429, timeout, output limit, unknown message | Existing distinct validation/resource/rate/transport handling |

Freeze Responses `truncation=disabled` explicitly. Give changed adapter
contracts new adapter versions. Fixture both text and tool decision formats.
Compatible endpoints must satisfy the same tested envelope contract; a broad
global regex is not part of the design.

Represent each normalized input/output count as optional, so reported zero is
different from missing. Reject negative or overflowing counters. Use checked
addition for provider normalization, transactional cumulative totals and the
input-plus-output budget comparison. Anthropic
normalization includes cache creation/read according to its API contract;
Responses input already includes cached tokens. Preserve raw usage for audit.
Totals contain sums of available counts plus coverage for incomplete/missing
reports and lost attempts. Never use an overflow error's stated prompt size
as billed usage.

Extend fact validation and SQLite delta accounting to record received usage
on rejection and terminal model-error paths, rather than dropping it in
`StopError`. A known model result and the stop/rejection it causes commit
together where that attempt is being settled. If cancellation/crash prevents
settlement, preserve the distinction between unreported/lost usage and zero;
never reconstruct a bill from prompt bytes. Idempotent commit replay counts
each received report once.

The token budget is a retrospective stop threshold on **reported usage**:
the last call can overshoot it even when all usage is reported. Missing usage on an
error or lost attempt does not prevent ordinary recovery; otherwise a normal
overflow rejection without usage would defeat this design. Keep coverage
incomplete rather than filling zero. Apply the same rule to missing usage on
a successful compatible-provider response, and display that limitation.
Invalid numeric reports are provider-contract failures. Finite wall-time,
step and per-step attempt limits bound dogfood runs independently. A reported
usage budget cannot guarantee a hard total charge when reports are missing;
the CLI flag/help and SDK coverage fields must state this honestly.

## Request composition and explicit evidence selection

Render each request in this order:

1. Protocol, exact current goal/constraints, environment restrictions and
   run/session identity. Prior-session dialogue is discoverable through
   session links instead of automatically expanding a capacity-based page.
2. Immutable anchor identity and history navigation for the current epoch.
3. Lightweight receipts for completed steps of this epoch: identity, outcome,
   bounded existing purpose display and source handles. Preserve the whole
   command/purpose/output in the archive. Old retrieval/output bodies are not
   replayed through these receipts.
4. The explicitly selected focus evidence, resolved from frozen observations.
5. Dynamic actual execution position and latest completion status/reference,
   the latest bounded result body if it occurred after the anchor, and
   protocol-repair evidence only for an actual protocol repair. Progress means
   committed steps/effects, not an inferred checklist of completed features.

Steps 3 and 4 have deterministic order and provenance. The stable prefix is
the fixed header and append-only receipts; it ends before replaceable focus
and dynamic progress/result material. Update cache breakpoints accordingly.
Bytes are measured as resource usage, never converted to tokens.

Introduce a local action using the existing decision protocol:

```text
canti:focus {"sources":[{"seq":123,"source":0},{"seq":146,"source":1}]}
canti:focus {"sources":[]}
```

This **replaces** the selected reference set. `seq` identifies a completed
retrieval transition in the current run; `source` identifies one frozen source
within it. The harness derives the step, length and hash and verifies the
committed bytes, reusing the useful parts of `carry.go`. Selection can reach
any earlier committed retrieval in the run, independently of the current
anchor. Reading another run's archive first creates a current-run observation
that can be selected in the same way.

Normalize `focus` once as `RecoveryLocal`. Validate at most 32 unique sources,
canonical order and prior sequence identity. Each existing retrieval source
is at most 8 KiB, giving an explicit 256 KiB maximum selected body without a
new token-related setting. Invalid selection produces an ordinary local-action
failure and leaves the prior set unchanged. `Performed` commits the selected
references, outcome and step progress atomically. A focus operation consumes
a normal decision/action step; replacement semantics make replay simple.

A read's result enters the next attempted request. If that request overflows,
reset removes automatic inclusion of that result and all old focus bodies.
Fresh orientation still names the committed latest result, so the new reader
can fetch a smaller range or choose different evidence. Evidence availability
and successful model consumption are separate facts.
Within the same logical decision step, transport retries and protocol-repair
revisions retain the same latest result. Its ordinary lifetime ends when that
step completes, not after the first attempted send; reset is the explicit
exception. No process-local consumed flag is needed.

Retain byte limits for goals/constraints, previews, archive pages and provider
responses. Replace `-context-bytes` with `-request-bytes`: proposed default
8 MiB, also the hard supported maximum; callers may choose a smaller resource
cap. Apply it to both rendered input and the final serialized
provider request, including escaping overhead. Bound construction allocations
as well. Exceeding it reports a local resource
failure and never masquerades as provider overflow or triggers handover.
Remove future-step/failed-handover reservations from admission. If a request
containing only authoritative orientation already overflows, record a terminal
failure with its provider evidence instead of creating another empty context.
This is an observed failure, not a prediction from tokens or bytes.
Recalculate SDK/server diagnostic message bounds for the supported request
and response bodies, JSON encoding overhead and paged records. Preserve
whole frozen bytes; do not silently truncate diagnostics to old frame limits.

## Original evidence through the existing archive

Extend each published retrieval step atomically with:

```text
steps/00000037/
  meta
  command
  purpose
  preview
  evidence.json
  evidence/0000
  evidence/0001
```

`evidence.json` records the committed transition sequence, source indexes,
original paths/byte ranges, lengths/hashes, local evidence filenames and query
continuation/completeness. Each evidence file contains the exact observed
bytes already committed in `Outcome.Retrieval`, not a fresh read of its source.
Reuse the existing temporary-directory, sync and rename publication path.
Publication can be retried from the journal after a crash; a missing derived
file is repairable, while a hash/content conflict is an integrity failure.

Existing archive `read`, `list` and `search` then access historical snapshots
without exposing the database or runtime internal types. Include stable
`seq/source` handles in rendered retrieval results and manifests. Extend
original-step `discover` to include the existing `purpose` file and update
cursor validation. Keep action/retrieval echoes excluded from default
discovery; exact lookup remains available.

Run ID and current step give deterministic paths for recent-record lookup;
session files link earlier user goals and returns. Programmatic readers can
iterate these paths, filter or aggregate outside the model request, and return
references and selected results. Use the existing Docker execution boundary
for arbitrary code. Its read-only archive mount does not change arbitrary
shell code into a trusted repeatable-read action.

## Cleanup and compatibility

Remove the runtime policy for `canti:handover` and `canti:handover-carry`, their
mandatory summary/gating/failure counters, and active-window-only selection.
Replace `Window`/`HandoverCheck` diagnostics with anchor/focus/source capture.
Extract reusable frozen-evidence validation; remove carry admission margins,
`requestHandover`, `ContextFit` reserves and capacity-selected session dialogue.

Remove `WindowSteps`, `WindowBytes`, `HandoverBytes`, and their CLI/TUI/SDK
fields and flags. Replace context-byte admission configuration with the new
request resource cap. Unknown old actions remain local failures through the
existing reserved `canti:` namespace and must never become shell commands.
Keep old frozen fixtures untouched; add the new corpus and update active
behavior tests rather than weakening them.

| Version boundary | Planned next value from this baseline |
| --- | --- |
| Core semantics | 5 -> 6 |
| Context rendering/accounting | 16 -> 17 |
| SQLite schema, including context head and usage coverage | 3 -> 4 |
| SDK/host RPC, including config and progress DTOs | 5 -> 6, append `testdata/v6` |

Use the next available number if another change lands first. Existing state
directories follow the documented fresh-state remedy; no silent migration or
reinterpretation. The SDK imports no runtime implementation; TUI changes are
limited to configuration, progress and usage presentation. Update existing
README, architecture, development and acceptance descriptions with the code.

## Implementation sequence

| Work unit | Concrete output and check |
| --- | --- |
| 1. Attempt semantics and provider results | `RequestRejected`, typed context errors, optional usage and transitions/fact validation; provider-envelope and core transition tests |
| 2. Durable context and archive | Context head, reset/focus transactions, exact evidence files and repair; SQLite reopen, lost acknowledgement and hash tests |
| 3. Reader-driven loop | New request composition, `focus`, revision reasons, byte resource admission; frozen requests, transient-result and restart tests |
| 4. Public surface and legacy cleanup | SDK/host protocol, CLI/TUI, versions and existing docs; append-only wire samples, independent module build/vet |
| 5. Integrated delivery and dogfood | Full repository gates, updated executable running on its own source, fix issues discovered and repeat affected checks |

Keep commits buildable by adding contracts before switching consumers. The
final tree exposes one policy and contains no dormant second context engine.

## Verification by construction and by using canti

Deterministic tests must establish: same-byte retry versus rebuilt revision;
rejection in step 1; partial valid-looking decisions never executed; actual
usage counted once; missing usage not zero; reset/focus committed atomically;
crashes before/after commit and request freeze; exact historical reads after
source mutation/removal; focus replacement/release; unseen latest result after
reset; cancellation and unknown effects preserved; unrelated provider errors
never resetting context. Use the existing core, adapter, runner, loop, SQLite,
archive, host RPC and CLI test files of these contracts.

Run the required delivery commands:

```sh
make check lint
go test -count=1 -race ./runtime/... ./sdk/go/... ./apps/tui/...
make integration
git diff --check
make commits BASE=<implementation-base>
```

Dogfood the built executable against its own repository in an isolated
worktree, with a separate state directory and a prewarmed Go test image pinned
by digest. Keep the executable fixed at the delivered revision while it edits
the checkout. The task is to repair a deliberately seeded archive
paging/evidence regression in canti and deliver a tested patch. The seed and
held-out assertions belong to the test controller, so the outcome is concrete
and independently checkable rather than a model's self-reported success.

Use a named live model for decisions. A test-only relay can inject two known
overflow envelopes after meaningful observations and block the next send
while the controller kills/restarts the isolated host. This makes reset and
resume reproducible within a finite budget. Label it **live model + injected
failure**, not a real provider capacity measurement. Test both API envelope
contracts offline; record actual provider overflow separately only if observed
within the allocated live budget.

Proposed dogfood bounds: one run at a time, 160 steps, 4 attempts per step,
45 minutes, 2,000,000 reported tokens, 120 seconds per job, and existing 8 MiB
retained output per job. Missing usage keeps the coverage visibly incomplete;
time/step/attempt bounds still apply and error recovery remains available.
Use the existing credential injection and child-environment cleanup; no
credential or environment file enters the worktree, fixtures or report.

Acceptance requires all of the following on the new implementation:

- Two reset boundaries and one process restart with no predecessor-summary
  request and no replay of a previously accepted external effect.
- At least one successful read of pre-anchor frozen evidence and one
  programmatic history selection used in subsequent work; source references
  and the selected focus are visible in the journal.
- The seeded defect is fixed; held-out assertions and repository checks pass.
  A `return` without the working patch and external verification fails.
- The controller confirms immutable old requests, attributable rebuilt
  requests, consistent usage coverage, bounded resources and preserved output.

If canti gets stuck, locate the cause from its saved requests, receipts and
queries, repair the implementation, and rerun the affected task. A task
failure does not justify silently restoring mandatory summaries. This is
dogfood acceptance, not a comparison with Bub or the old canti policy.

## Remaining limits and evidence at planning time

OpenAI's ambiguous `max_output_tokens` signal remains ambiguous. Arbitrary
compatible gateways may need additional verified classifier cases. Neither
retrieval nor an anchor can recover information never retained. Effect-ID
replay safety does not prevent a model from choosing a logically duplicate
business action under a new ID. Large jointly necessary evidence can still
overflow; global budgets bound failure but do not prove useful progress.

The repository is clean at the source baseline. Earlier research ran all
offline checks and race tests successfully on unchanged production code.
This feasibility pass additionally confirmed Go/Docker availability and
focused existing carry/history/discovery recovery tests. These are baseline
facts, not evidence that this proposed implementation already exists.

Prior context was checked through Nowledge MCP, normal query `canti immediate
anchor reader-driven history retrieval no mandatory summary implementation`,
strict `default` scope, limit 3, no extra filters. The relevant result was
rank 1, `26d4d212-b6a8-4ab9-8a8a-5f982b33dcdb`, "Canti handover research revised:
immediate anchor and reader-driven history access, with no mandatory
predecessor summary", score 0.99. Current source and the user's latest
build-then-dogfood direction govern this specification.
