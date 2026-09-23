# V1 acceptance

V1 aims to combine a stateless core with a durable, bounded single-machine
harness. This repository currently delivers the protocol and boundary skeleton.
V1 durability is **not complete**.

## Current evidence

| Property | Current evidence | Limit |
| --- | --- | --- |
| Strict decision protocol | Valid/invalid input tests in `core/decode_test.go` | Offline parsing only |
| Stateless calls | Concurrent decode tests and race checks | Does not test model or runtime behavior |
| Complete transition table | Every legal phase/event pair asserts the full resulting state and every other pair is rejected unchanged in `core/state_test.go` | Offline values only |
| Causal event identity | Attempt/response events name their step and attempt; effect events name their effect; echoes from an earlier attempt, step, or effect are invalid and trajectory tests walk two real steps plus a protocol repair | Offline values only |
| State invariants | `State.Validate` rejects unreachable projections, including a `job_bound` state with a committed start; legal transitions are tested to preserve the invariants; the runner refuses commit/bind/start for invalid states | No loader exists yet |
| Budgets and stop reasons | Transport, lost-send, protocol, and step exhaustion tests | No wall-time, token, or storage measurement |
| Counted attempts | `AttemptStarted` in `request_sent` consumes an attempt and stops at `MaxAttempts`; protocol repair returns to `idle` and freezes a new revision under the same budget | No adapter sends anything yet |
| Sequenced commits | Every accepted event advances `Snapshot.Seq` even on state self-loops; a stale snapshot cannot commit after a self-loop; a `From` outside the committed chain is rejected with `ErrBrokenChain`; an identical retry after a lost acknowledgement is confirmed without a second record | Recording store only |
| Commit before bind, bind before start | Shared event log shows the effect commit before `Bind` and the `job_bound` commit before `Start`; a store failure after either call leaves the prior phase and wraps both errors | Recording store/executor, no durable backend |
| Cancellation | Cancel before commit, before bind/start, and around either call leaves the snapshot unchanged and writes nothing; repeated stop requests in `cancelling` and `needs_resolution` are recorded without changing phase | No process-tree cleanup or persistent cancellation |
| Unknown outcomes | Executor error, missing job ID, or a foreign job commits `OutcomeUnknown` with failure text; `needs_resolution` accepts a found job, resuming behind the start boundary or at inspection after a committed start; a jobless resolution cannot claim `exited` or `timed_out`; resolving to `unknown` abandons the run, keeping the first stop reason or recording `unresolved` | No real backend inspection |
| Permitted facts | Each event accepts only its own records; missing, mismatched, or extraneous facts write nothing and wrap `ErrFacts`; core sentinels survive `Commit` | Recording store only |
| Stable effect identity | Repeatability and ambiguous-separator regression tests | No real backend deduplication |
| Executor start latch | `executortest.Check` opens a fresh executor for every call and covers bind identity, changed commands, unbound start, start-once after finish, the cancel fence before and after bind, concurrent bind and start, a start racing a cancel, and runs recounted after every case; an in-memory reference passes and each single-clause mutant is rejected by its case | Offline reference only; no concrete executor has run it; no crash injection, running-job cancellation, or context handling; a start racing a cancel is caught only when the calls overlap |
| Stable event kinds | Every event exposes a unique persisted kind and `SemanticsVersion` pins the transition rules | No encoder or migration exists yet |
| Offline loop shape | Exec then return steps drive `Commit`/`Bind`/`Start` through eleven committed phases with strictly increasing sequences | No model, inspect, or restart |
| Input requirements | Missing goal/environment tests | No size or token budget |
| CLI entry point | Build and version/help smoke check | No runnable agent workflow |

Run current evidence with `make check`, `go test -count=1 -race ./...`, and
the CLI commands in [development](development.md). Test results apply only to
the revision and environment actually checked.

## Required integration gates

Every gate below is pending. Do not mark one passed merely because its types,
documentation, or an in-memory simulation exists.

| Gate | Required evidence |
| --- | --- |
| Transactional facts | Concrete SQLite transactions commit decision plus effect/return, and observation plus progress; injected failures cannot leave a falsely advanced run |
| Cold restart after every step | A subprocess test exits after each committed step and reconstructs the next step from disk, with no retained Go objects or SDK session |
| Crash at commit boundaries | Kill before and after request, response, decision/effect, dispatch, and observation commits; assert no lost confirmed facts or blindly repeated effects |
| In-flight job recovery | Restart the harness while a real backend job runs; find the original job, collect its original output, and commit one observation |
| Unknown outcomes | Make job status/result unavailable; persist `needs_resolution`, prevent automatic redispatch, and record an explicit resolution before continuation |
| Single workspace writer | Competing processes cannot advance the same workspace; restart takeover inspects existing jobs before new decisions |
| Frozen request replay | Capture actual outbound request bytes and provenance; retries preserve bytes and record distinct attempts; incomplete responses never dispatch |
| Long history | Grow stored history beyond the model window; bound request tokens/bytes, per-step reads, memory, and construction time while older facts remain queryable |
| Large output | Stream output to owned artifacts with bounded previews; report truncation, exhausted disk quota, missing artifacts, and partial output explicitly |
| Cancellation and timeout | Stop new dispatch, cancel or inspect the backend job, bound process-tree and pipe cleanup, and persist the true result using a separate cleanup deadline |
| Finite runs | Exhaust step, attempt, time, token/cost, and disk budgets; persist a specific stop reason and partial results |
| Trust boundary | The execution environment cannot modify authoritative facts or obtain host model credentials; agent notes remain writable and separate |
| Complete loop | A concrete model adapter, core, store, and executor drive exec/observation/next-step/return in one isolated workspace |

Neither a successful command nor a `return` decision proves the user's task
succeeded. Do not claim exactly-once arbitrary shell effects: job identity
deduplication does not cover effects inside a command or later semantic repeats.

## Evidence record

For each integration gate, retain:

- code revision, platform, Go version, and backend versions/configuration;
- exact invocation and finite concurrency, time, usage, and storage limits;
- immutable fixture or request identifiers and relevant checksums;
- injected failure point, expected state, and observed committed state;
- run, step, effect, and job identities needed to trace recovery;
- output and error artifacts, including partial and failed runs; and
- resource cleanup and any unmet acceptance condition.

For model runs, also record provider/model identifiers, protocol and adapter
versions, prompt/request configuration, and measured usage separately from cost
estimates. Label each result as offline fixture, concrete backend integration,
or live provider evidence. A scripted provider helps test recovery determinism;
it cannot substitute for checking a concrete model adapter's transport behavior.

Choose numeric budgets before each boundedness experiment and compare measured
peaks as history grows. Do not declare bounded behavior from a single small
sample or hide failed samples from the result set.

## Implementation order

1. Implement `Store.Commit` and recovery reads for the latest `Snapshot`, the
   frozen request, and the saved response in SQLite with workspace ownership,
   and test crash recovery of persisted transitions, sequence conflicts, and
   idempotent retries.
2. Add a queryable executor with stable job identity, a bind that never
   executes, a fencing cancel, retained output, explicit uncertainty, and
   bounded cancellation/cleanup; it must pass `executortest.Check`.
3. Add one model adapter and bounded input builder with frozen request replay
   and attempt/usage accounting.
4. Wire the loop and CLI, then exercise every integration gate above, including
   cold restart after each step.

Revisit interface shapes when a concrete transaction requires it. Run rules
stay in the core's transition table; I/O, storage, and execution stay in the
harness. Neither layer restates the other's responsibility.
