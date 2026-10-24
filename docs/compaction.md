# Independent context compaction

Compaction belongs to the harness. It automatically maintains one rolling
summary and a contiguous retained suffix for a session across runs and restarts.
A standalone run owns a separate stream. There is no cross-session memory,
agent-facing handover command, handover gate, or legacy handover reader.
Core semantics, event kinds, business attempts, step numbers, and effect IDs do
not change. Journal schema 3 and context version 13 record these rules.

## Source and working context

SQLite appends an input, a completed step, or a terminal result to the owner's
source stream in the same transaction as the original fact. Each entry names
its run, session sequence, journal sequence, and monotonic source index.
Unknown executions do not enter this stream until resolved. A stopped run's
status remains explicit; a return never proves business success.

A working request includes the current goal and effective constraints,
unchanged apart from the existing outer whitespace normalization, the active
summary, and every source after its coverage cursor. Prior inputs, returns,
and steps are retained together. Current-run steps preserve their original
numbering. A previous retained tail is included in the next compaction; only
successful adoption advances coverage. Full source records and frozen request
bytes remain available, including evidence no longer in the prompt. Previews
and commands use their documented presentation caps with truncation markers.
The summary precedes the retained history, including prior runs' corrections
and returns. Each complete source has a frozen content-block boundary.

Capacity, or an optional `-compact-steps` / `-compact-bytes` threshold, selects
an oldest prefix. `-retain-bytes` targets a recent suffix of complete entries;
the newest completed step in the current run is protected regardless of the
target. The current goal and constraints remain separately pinned even when
their source entry joins the summary. No compact runs between a decision and
its effect, during an unknown outcome, or inside protocol repair.

## Durable operation

1. Freeze a maintenance request at `IntentPrepareRequest`, revision zero.
   Its plan records the active base, candidate parent, source range and hash,
   exact request bytes, model/adapter provenance, limits, and token reserve.
   SQLite verifies the committed source hash and guards the run position and
   checkpoint lineage before committing the plan. When its sources are already
   in the latest business request with the same checkpoint, copy that request's
   complete prefix. Record and verify the parent request hash and block boundary.
   New-run and backlog inputs use the business renderer to form an unsent base.
2. Persist each attempt and its token reservation before sending once. A retry
   uses the same frozen bytes and a separate summary attempt counter. A started
   attempt with no committed response remains unknown after restart. Its
   reservation is retained; a retry never assumes it was free.
3. Persist response bytes, usage, and failure. Keep the parent's text or tool
   decision protocol and accept only a complete `return` decision's payload,
   nonempty UTF-8 within the byte cap, which reduces the
   actual rendered replacement including headings and fences. A saved valid
   response can be finished after restart without calling the model again.
   An `exec` response fails validation and never reaches the business state
   machine or executor. A maintenance `return` does not end the business run.
4. Keep the validated summary as a candidate. If a backlog needs multiple
   bounded passes, the next plan names that candidate as its parent and adds
   the next source prefix. None of these intermediate candidates changes the
   active checkpoint.
5. Render and admit the next working request. Atomically replace the active
   checkpoint and commit `RequestFrozen`; its metadata names that checkpoint
   and the retained sources. A transaction failure moves neither boundary.

A source plan, candidate, or response from another run position cannot be
adopted. Cancellation fences late results through the core stop transition.
Invalid summaries, exhausted attempts, irreducible protected context, and
budget exhaustion stop visibly; the original active context remains intact.
An arbitrary shell effect is not made exactly-once by compaction or stable IDs.

## Limits and usage

Text (`-context-bytes`), serialized request bytes (`-request-bytes`), and model
context allowance (`-context-tokens`) are separate checks for both summary and
working requests. Token admission counts every serialized byte plus 256 as an
input token and reserves output tokens. This deliberately conservative bound
is an estimate for this text Messages adapter, not a tokenizer or cost metric.
Configure the allowance for the selected model. Actual recorded usage comes
from provider input/output and cache token counts.

The summary keeps the business request's model, system prompt, tool definitions,
tool choice, structured output, protocol headers and cache markers. It appends
the maintenance instruction after the selected complete source prefix, omitting
the mutable progress/repair suffix and the uncovered tail. Business requests
also mark their prospective compactable boundary for caching. Input and output
are admitted together for each summary; an oversized prefix is reduced only
at complete source boundaries. Cache hits do not reduce context admission or
the run's token budget.
The summary response cap is 16 KiB by default (64 KiB maximum); the output token
cap is 8,192, bounded by the business model's output cap. The default attempt
limit is 3. All calls share the run's wall-time and token budgets. Missing,
partial, or invalid usage retains the attempt's conservative reservation and
is exposed as unknown in diagnostics. The previous run's paid usage is not
charged again merely because its context continues in a later run.
Per-response `Usage.Cache` records read and write token counts only when both
are explicitly reported. A missing cache breakdown is unknown, not zero; on a
cached Messages request it also leaves total input usage unknown and retains
the summary reservation. Run totals retain their existing all-input token
accounting; the individual attempts supply the cache and cost breakdown.

Source reads are paged, with at most 32 MiB of persisted source data per plan.
Large backlogs use multiple passes. A single source unit that cannot fit the
summary input fails visibly; it is never silently skipped or cut to move the
coverage cursor. The archive and journal are append-only for this purpose;
there is no retention or disk quota policy for them.

## Evidence and inspection

`canti compactions` exposes source ranges, parent/base identity, frozen-prefix
provenance, per-attempt cache counts, attempts,
usage uncertainty, and adoption; `-id ID -raw` returns frozen request bytes.
`canti context` exposes the exact working request and its checkpoint/source
capture. `status` includes total tokens, summary tokens, and unknown reservations.
These commands do not publish archive files or contact the provider.

Tests cover exact prefixes in both text and tool modes, chronological summary
placement, rejection of executable summaries, repeated compaction, retained-tail reuse, frozen-byte retries,
response recovery, transactional adoption, cancellation fencing, unknown usage,
and request/source mismatches. The frozen corpus pins summary requests and
operation records as well as working requests. Integration tests use real
SQLite, CLI processes, Docker effects, and a scripted Messages server. This
establishes transport and lifecycle behavior, not real-model summary fidelity,
long-horizon task success, cache reuse, or provider billing.
