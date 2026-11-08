# V1 acceptance

V1 combines a stateless core with a durable, bounded single-machine harness:
a SQLite journal, a Docker executor, model adapters for Anthropic Messages and
OpenAI Responses, and a CLI. Every integration gate below has concrete-backend
evidence except where its status says otherwise. The model in every
end-to-end test is a scripted local HTTP server: the adapter's HTTP path is
exercised, a provider's behavior is not. Live provider evidence is summarized
under [Live provider findings](#live-provider-findings).

Current source uses Context17, semantics6, schema4 and RPC6. The provider-context
cutover has focused offline and SQLite regression evidence; independent exact-SHA
full gates and dogfood are required before accepting its final candidate. Historical
live observations below describe the versions they name.

## Offline evidence

These tests run in `make check`, and under the race detector in
`go test -count=1 -race ./runtime/... ./sdk/go/... ./apps/tui/...`; they use no Docker daemon or credential.

| Property | Evidence | Limit |
| --- | --- | --- |
| Strict decision protocol | Valid/invalid input tests in `runtime/core/decode_test.go` | Parsing only |
| Stateless calls | Concurrent decode tests and race checks | Does not test model or runtime behavior |
| Complete transition table | Every legal phase/event pair asserts the full resulting state and every other pair is rejected unchanged in `runtime/core/state_test.go` | Values only |
| Causal event identity | Attempt/response events name their step and attempt; effect events name their effect; echoes from an earlier attempt, step, or effect are invalid, and trajectory tests walk two real steps plus a protocol repair | Values only |
| State invariants | `State.Validate` rejects unreachable projections of the current accepted-effect phase, decision, effect, budgets and stop reason; legal transitions preserve these invariants. Core has no physical owner or launch field. Runner refuses invalid commit states and Loop validates every loaded snapshot | Values only; receipt/owner validation belongs to the harness and executor |
| Budgets and stop reasons | Transport, lost-send, protocol, and step exhaustion in the core; the loop stops on spent wall-time (before a step, during a model request, during a job), token, and output-byte budgets, and cancels a job past its job timeout as `timed_out` | Fake model and executor |
| Counted attempts | `AttemptStarted` in `request_sent` consumes an attempt and stops at `MaxAttempts`; protocol repair returns to `idle` and freezes a new revision under the same budget | Values and fakes |
| Sequenced commits | Every accepted event advances `Snapshot.Seq` even on state self-loops; a stale snapshot cannot commit after a self-loop; a `From` outside the committed chain is rejected with `ErrBrokenChain`; an identical retry after a lost acknowledgement is confirmed without a second record | Recording store; the SQLite rows below repeat these on a real file |
| Acceptance before execution, binding before launch | `TestLoopActsOnlyOnARecordedAcceptance` requires committed normalized action and execution identity before executor work. `TestDrivePublishesBindingBeforeLaunch`, `TestBindingPublicationFailureCannotConsumeLaunchLatch`, and `TestDriveRechecksDeadlineAfterBindingBeforeLatch` check the executor-owned binding publication and deadline before latch consumption | SQLite and fake Docker; concrete executor conformance also runs against Docker |
| Runner cancellation | A cancelled commit context writes nothing. Controller cancellation leaves accepted facts for restart; repeated persistent stop requests remain legal in cancelling and needs_resolution. `TestStopDeadlineKeepsPostLatchWorkAccountedUntilQuiescent` verifies that a Stop deadline can return while Host retains ownership of its cancelled controller | Recording store, real SQLite and deterministic blocked executor I/O |
| Permitted facts | Each event accepts only its own records; missing, mismatched, or extraneous facts write nothing and wrap `ErrFacts`; core sentinels survive `Commit` | Recording store |
| Stable effect and event identity | Effect ID repeatability and ambiguous-separator tests; every event exposes a unique persisted kind, and a test pins current `SemanticsVersion` at 6; the current-version recorded exploration fails until a changed rule is recorded under a new version | A rule outside the explored limits needs its own test; no migration between semantics versions exists |
| Executor start latch | `executortest.Check` crosses Specification/Drive/Inspect/Cancel with fresh executors, rejects changed identity and invalid terminal/fence behavior, and verifies one launch under repetition and cancellation. Publication/owner/receipt failure injection is in `docker/fake_test.go`. `TestDriveOwnsBoundedLaunchAfterCallerCancellation` holds the start response after latch publication, verifies that cancelled caller context does not detach Drive, and checks bounded completion and one start across a fresh repeat | Offline reference and fake daemon; concrete Docker conformance is recorded below |
| Loop | `harness.Loop` over a real SQLite journal with a fake model and executor: exact commit sequences for exec, observation, and return; frozen-byte resends; protocol repair; budgets; unknown outcomes halting until `ResolveJob` or `Resolve`; persistent stop; controller shutdown committing nothing; a new loop and store for every step committing the same complete current-format transition sequence as an uninterrupted run | Restarts are in-process |
| Bounded input | Frame validation, byte-limited authoritative fields, compact receipts and bounded dynamic evidence; resource-cap failures retain all committed history. Oversized failed retrieval queries retain their original facts while diagnostic capture remains bounded across reopen and protocol repair (`TestContextCaptureBoundsRejectedRetrievalAcrossReopen`) | Receipts grow until provider rejection or the configured resource cap |
| Whole goal and constraints | Current goal and constraints are rendered exactly, including whitespace and UTF-8 boundaries, across reset and repair; oversized authoritative input fails visibly | Per-field resource bounds remain |
| Request resource ceiling | Both rendered prompt and serialized provider body are bounded by `-request-bytes`, at most 8 MiB. Exact goal/constraints, bounded receipts, latest-result lifetime and explicit failures are exercised in harness and CLI tests | Byte resource bound, no provider-window prediction |
| Stable prompt prefix | A fixed anchor header and append-only receipts precede selected focus and the dynamic latest-result/repair tail; retries reuse exact frozen bytes | Provider cache reuse is provider-dependent |
| Cache breakpoints | The frozen body holds one user message with a text block per prompt block, in order; the first and the last stable block, and no other, carry an `ephemeral` `cache_control`, also for prompts rendered at steps 1 to 3; none does without a stable block, and one does when a single block is stable (`anthropic.TestFreezeMarksTheFirstAndLastStableBlocks`) | Request bytes only here; the gateway of the 2026-09-24 evaluation ignores `cache_control`, and the one of the 2026-10-03 evaluation reported cache reads for 86–97% of input tokens, which does not show which breakpoint produced them |
| Anthropic adapter | Against a local HTTP server: frozen body sent verbatim with the right headers; status, stop-reason, truncation, size, timeout, redirect, and context handling; decision extraction; a request frozen with fallbacks keeps its beta header when a model configured without them sends it, and one whose protocol is unknown or whose adapter is neither `canti-anthropic/4` nor `canti-anthropic/4-tool`, the earlier `canti-anthropic/1` included, stops as `unrecoverable` without a send (`TestCompleteHonorsFrozenProtocol`) | No live provider; the 2026-09-23 samples used `canti-anthropic/1` |
| Decision purpose | Strict core decision parsing retains optional purpose. Receipts show its bounded one-line display while the archive keeps it whole; restart and protocol repair preserve the stable prefix (`TestLoopShowsCommittedPurposesAcrossRestart`). Discovery includes original purpose fields | Purpose describes intended work, not proof of completion |
| Decision transport prompt | Text and tool protocols render the same selective reader context; previous context fixtures remain frozen under their historical version | Model compliance requires live evidence |
| Frozen retrieval evidence | Retrieval outcomes commit exact source bytes and SHA-256 references. Latest evidence survives same-step retry/repair; focus retains selected observations across later steps without rereading originals | Journal growth is proportional to observed evidence |
| Explicit focus | `canti:focus` replaces a selection, including empty selection; malformed, duplicate, unavailable or corrupted references fail. Outcome, context head and progress commit atomically. QA tests cover lost acknowledgement, source removal and context provenance corruption | Context17 replaces the historical carry policy; independent final-candidate gates remain required |
| Retrieval system prompt | Silent retrieval rules remain within the 2,048-byte section budget. Discovery covers purpose as well as command and preview; canti actions and marked discovery steps are skipped | No comparative evaluation gate |
| Provider usage and confirmed overflow | Adapter tests preserve raw responses/usage and reject duplicate or case-variant discriminator signals. Coverage distinguishes unknown, reported zero and lost attempts; checked arithmetic, terminal settlement, budget thresholds and cancellation are tested on real SQLite. Initial orientation-only overflow stops on its first send | Offline HTTP fixtures and SQLite; live overflow is separate evidence |
| Docker executor | Against a fake daemon: missing versus lost jobs, no create once the context is done, no restart of an unlatched or failed start, a start never sent twice, a rejected start as `failed_to_start`, launch outliving shutdown, per-call deadlines, the API version the daemon supports, and a loop that gives up on a hung daemon with an unknown outcome; a bind after the fence recording a cancelled job without creating a container; a record published before its container is released, release retries, failed and interrupted captures, and strict records | Fake daemon |
| SQLite journal | On real database files: durable pragmas, schema refusal, concurrent first opens, sequence compare-and-set, identical-retry confirmation without WAL growth, conflicting content never overwritten, broken chains, rollback of an injected failure after the insert, strict decoding refusing unknown kinds, fields, and semantics, one winner per sequence across two stores, and bounded reads. `TestCommitChecksEffectsAgainstTheirAcceptanceInItsTransaction`, `TestCommitRejectsContradictoryExecutionReceipts`, and `TestAcceptanceIdentityUsesImmutableRunConfiguration` validate receipts and configuration identity atomically without backend queries | Covered in the gates below as well |
| Step records and historical snapshot repair | Published retrieval steps include evidence.json and exact evidence/NNNN files. Native historical reads use an indexed step lookup to repair missing derived snapshots from committed bytes. QA probes evidence/manifest/both loss, removed original, later unrelated steps and reset, same-size conflict, unknown source and cancellation | Shell reads do not trigger repair; conflicting derived content is never overwritten |
| CLI | Usage and exit codes, credential removal, refusal without a credential, missing runs, locked workspaces, state inside the workspace, open-run refusal, crash-and-resume around `response_saved`, a signal during setup, first and second signals, resolution and stops of unknown outcomes without Docker, paged history, status of one commit, and workspace binding, mostly in subprocesses; a run whose stored config holds the removed `history` field, as revisions before `552644d` wrote it, is refused by every command with an unreadable-config error naming the remedy, a fresh `-state`, and nothing is committed or sent (`TestRunsOfAnEarlierRevisionAreRefused`) | Without Docker, a bind fails and becomes an unknown outcome; only that one earlier config is tested |

## Integration gates

`make integration` runs these tests against real SQLite files and a Docker
daemon. Once the run halts, the CLI gates that dispatch jobs check that no
container of their state directory remains and that every bound effect and no
other has a final job record naming the job committed for it and matching the
committed observation; the exceptions are the notes gates, which check only
that no container is left running, and the lost-job gate, which requires that
no record exists. The recovery gates also replay the recorded events
through `core.State.Next` from the fresh state and require every recorded
state in an unbroken sequence. Each gate case logs an `evidence` line with its
injected failure, observed result, and measurements.

The original acceptance runs used Linux (WSL2) with Docker Engine 29.7.
Current C2 results below were rerun on Linux/amd64 with Docker Engine 29.9.1-1,
Go 1.26.10 and runtime/test source `6a6b8ce223dec08713890285e6a660e44ad09bf0`:
`go test -count=1 -race ./...` passed in 307 s, and `make integration` passed in
756 s against real SQLite and Docker, with scripted model HTTP. The later
acceptance-documentation commit changes no runtime or test source. *Passed*
means the current listed tests passed; *Partial* retains the missing evidence
named in Limits. This validation made no live model calls and establishes
neither daemon-restart nor power-loss durability.


| Gate | Status | Tests | What they assert | Limits |
| --- | --- | --- | --- | --- |
| Transactional facts | Passed | `sqlite.TestCommitRollsBackFailureAfterInsert`, `TestCrashKeepsAcknowledgedCommits`, `TestCommitPersistsTransitionsAndTotalsAcrossReopen`, `TestConcurrentStoresCommitOneTransitionPerSeq` | A transition, its facts, and the run totals are one transaction: decision plus effect or return is one `response_loaded` row, observation plus progress one `observed` row. A failure injected after the insert leaves no row and unchanged totals; a child killed with SIGKILL after at least 200 acknowledged commits loses none, keeps at most the one in flight, and replays to the stored state and totals | Failure injected at one point, the totals update; SIGKILL, not power loss |
| Cold restart after every step | Passed | `TestGateColdRestartAfterEveryStep`, `TestRunOnceStepsAcrossProcesses`, `harness.TestLoopColdRestartAfterEveryStepMatchesAnUninterruptedRun` | A new CLI process per current intent for exec, exec, return each commits exactly its intent's transitions from the journal alone; the kinds, phases, and states of the history equal an uninterrupted run's except for IDs, while the in-process harness test compares whole transitions, facts included; the model receives exactly the frozen bytes, identical to the uninterrupted run's except for the run ID, which the prompt's first block names, and effect IDs; each command ran once; a `resume` of the returned run calls no model; both runs' step records hold every step's command and preview and the result | Processes exit normally; crashes are the next gate |
| Crash at commit boundaries | Passed | `TestGateCrashAtCommitBoundaries`, `TestACrashAtACommitResumesFromTheJournal`, `docker.TestExecutorPassesStartLatchCheck` | SIGKILL before and after current exec-step commits (`request_frozen`, `attempt_started`, `response_saved`, `response_loaded`, `observed`), around retained-result release, and before the next request. Restart preserves committed rows and frozen bytes, reuses the accepted owner and records one result. `Drive` owns binding/start recovery; no Run job_bound/job_started commits exist. Fake-daemon tests additionally inject binding and terminal publication failure, lost create acknowledgement, and missing launch/stop witnesses | SIGKILL, not power loss; fake publication injection is distinct from real Docker evidence |
| In-flight job recovery | Passed | `TestGateInFlightJobRecovery`, `docker.TestRestartedExecutorCollectsOriginalJob` | The controller is killed while the accepted effect owns a running container; a fresh controller resumes from effect_saved, inspects that same owner, retains its original output and commits one observation without another launch or model request for the step | Controller restart only; Docker daemon restart and host reboot are not established |
| Unknown outcomes | Passed | `TestGateUnknownOutcomeOfALostJob`, `TestGateUnknownOutcomeOfAnUnavailableStatus`, `TestAnUnknownOutcomeWaitsForAnExplicitResolution`, `docker.TestRejectedStartIsRecordedAndReleased` | A started job's container removed with `docker rm -f` before any record commits `outcome_unknown` with `ErrJobLost`, and no record or container appears afterwards; container inspection failing six times in a row commits `outcome_unknown` while the job keeps running. For the lost job, a second `resume` and a failing `resolve -job` change nothing and call no model; for the running one, `resolve -job` resumes inspection, which then observes its original result; `resolve -execution` records an operator outcome that the next prompt marks as set by explicit resolution. A start the daemon rejects (an unknown user) is reported `failed_to_start`, recorded, and released instead of unknown, and nothing ran | Two injected causes against the daemon; a hung daemon only against a fake daemon; an operator resolution is trusted as given, and `resolve -execution` records the outcome only, leaving a job that may still run, a created container that never started, or the exited container of a killed job in place. At this record's revision a job killed outside canti, as by an abrupt host reboot, was observed as exited with Docker's exit code; later revisions report it unknown (`docker.TestOnlyARecordedExitIsFinal`, `docker.TestAnOutOfMemoryKillIsFinalOnlyWhenRecorded`, `TestGateUnknownOutcomeOfAJobKilledOutsideCanti`, with `docker kill` standing in for a daemon restart or host reboot), keep its container for `docker logs canti-<effect ID>` until it is removed by hand, refuse `resolve -job` for it without a commit, and report a signal the job observes, as from a graceful daemon shutdown, as its exit status |
| Single workspace writer | Passed | `TestGateSingleWorkspaceWriterAndTakeover`, `TestWritersRefuseALockedWorkspace` | Competing commands cannot own the same state directory. After controller SIGKILL, a new owner resumes the accepted effect from effect_saved and collects its original job. Host shutdown retains the lock until operations quiesce; a Stop deadline does not release ownership of post-latch work | flock protects a pathname; same-user replacement and a second state directory for the same workspace remain outside the exclusion guarantee |
| Frozen request replay | Passed | `TestGateFrozenRequestReplay` (3 cases), `TestGateFiniteRuns/attempts` | The first attempt answers with HTTP 529, with `stop_reason` `max_tokens` around a complete decision, or with a body cut mid-stream. `response_incomplete` records the failure and the partial body; the second attempt sends byte-identical bytes as a distinct attempt; the frozen model, protocol, and adapter are recorded; the truncated decision never runs; usage is counted for every response that reports it | Bytes are captured at the scripted server; a provider's retry behavior is not exercised |
| Decision purpose across processes | Passed | `TestDecisionPurposeReachesLaterRequestsAcrossRestarts` (2 cases) | For each decision format, one process per intent: every request is sent with the bytes the journal froze, a repair revision built by a new process keeps the stable blocks, and later requests show each committed purpose, a long one cut at 512 bytes, under the system prompt and schema of the run's transport | Scripted provider |
| Long history | Passed | `harness.TestLoopBoundsRequestsAsHistoryGrows`, `sqlite.TestReadsStayBoundedByTheirLimit`, `sqlite.TestRecentPagesBackwardBySequence` | With budgets derived from the 64 KiB prompt total before measuring, a restarted controller builds the next request through `Loop.Step` on a newly opened store at 50, 500, and 2,000 stored steps: the request stays within the total plus 16 KiB and the prompt within the total; the same number of latest steps is shown at every size, no more than the total can hold; the records read are the whole history or, at every larger size, the same number, at most those steps plus one page of 64, in the same reads; allocations and build time stay within ceilings and ratios while stored history grows to 12 MB of commands and previews; the oldest facts still read back. With no step cap, a loop shows exactly the steps a prompt of the whole history shows, at 16, 32, and 48 KiB and 1 MiB totals over 128 steps, reading one, one, two, and two pages; a failing second page returns its error and freezes and commits nothing. `Recent` returns the right records for every cursor and limit, including resolved steps and a final `steps_exhausted` step, and pages 2,000 steps backward by sequence; recovery queries, including a page deep in the history, use their indexes without a sort | Offline model: the history is seeded through `Runner.Commit` and requests are frozen by the real adapter but never sent; bytes, not tokens; allocations, not resident memory; three sizes of nine repetitions on one machine; measured at the harness default total of 64 KiB, not the CLI's 256 KiB; the allocation ceiling was raised from 4 MiB to 8 MiB after measuring, because each build decodes a whole page of records |
| Large output | Partial | `TestGateLargeOutput` (2 cases), `docker.TestOutputIsBoundedAndVerified` | A 20,000,012-byte output with a 1 MiB per-job limit keeps exactly its first 1 MiB as the artifact, reports `kept 1048576 of 20000012 bytes` and an incomplete output, and previews head and tail within 4 KiB; the next prompt reports the truncation and stays under 64 KiB, and the next job reads the artifact through the read-only mount; a run output budget of 1 MiB stops the run as `budget_exhausted` with the observation kept; a retained terminal receipt keeps its capture-time metadata after external artifact deletion, while an archive read reports the actual missing data | Output is copied from Docker's log once the job finishes, not streamed: until then that log is bounded only by the job's run time. The run budget is checked between steps, so a run can exceed it by one per-job limit. Neither is a filesystem quota: workspace writes, a job's container layer outside the mounts, the journal, and the container and log of a finished job whose removal failed until a later sample of that effect retries it are not limited, and a full disk is not injected; a final job record that cannot be written leaves the job to the inspect retries and then an unknown outcome. Docker's json-file log replaces invalid UTF-8, so artifacts of binary output are not byte-exact |
| Cancellation and timeout | Passed | `TestGateCancellationAndTimeout` (3 cases), `docker.TestCancelStopsProcessTree`, `TestASignalStopsTheRunPersistently`, `TestASignalDuringSetupStopsTheRunPersistently`, `TestASecondSignalAbandonsTheCleanup` | A 2 s job timeout on a two-process tree is observed as `timed_out`, and the job's record shows it ran 2 to 12 s; `stop` after the controller was killed commits `stop_requested` and then the job's cancelled result, which stops the run, within its cleanup deadline; SIGINT to `run` does the same and exits with status 3, and a signal during `run` or `resume` setup stops the run before any model call; `Cancel` of a process tree, including one that ignores SIGTERM, returns within the stop grace plus 5 s, the latter after at least the grace, and leaves the job cancelled; no container remains; a second signal abandons cleanup and a later `stop` finishes | Process-tree cleanup is Docker's container stop and kill, evidenced by the stop returning and the container being removed, not by listing processes; the job timeout, and a restarted controller's choice between `timed_out` and `cancelled`, rely on the daemon sharing the host clock; a durable fence prevents launch; only a stop witness published after observing a running owner permits cancelled classification, including a natural exit racing that request. A first Cancel of an already-exited job preserves its exit result, and later Cancel leaves retained terminal receipts unchanged; a stop's cleanup deadline must also cover copying the job's Docker log into its artifact, so a job with a large output needs a longer `-cleanup`; timing assertions allow 5 s of slack |
| Finite runs | Partial | `TestGateFiniteRuns` (4 cases), `TestGateLargeOutput/disk_budget` | Step, attempt, wall-time, token, and output-byte budgets each stop a run with `steps_exhausted`, `attempts_exhausted`, `timed_out`, or `budget_exhausted`, with failure text on the stop for the measured budgets and on each failed attempt, keep earlier observations, and dispatch nothing after the budget is spent; a wall-time stop cancels the running job and records its result | Cost is not measured; tokens are provider-reported usage, so a run can exceed its token budget by one response; the wall-time stop during a model request is tested only with a fake model |
| Trust boundary | Passed | `TestGateTrustBoundaryKeepsCredentialsAndStateOut`, `TestGateTrustBoundaryProtectsFacts`, `TestGateTrustBoundaryProtectsStepRecords`, `docker.TestTrustBoundary` | A job sees no host environment variable, credential, or injected proxy; runs as the host uid with no capabilities and only a loopback interface, and its running container has network `none`, `CapDrop` `ALL`, and the host uid and gid; cannot reach the model URL; sees the artifacts, and the step records at `/canti/runs`, read-only, and neither the state directory nor the journal; cannot delete or forge committed artifacts or history, and forgeries stay in the workspace. The credential appears in no request body, CLI output, or state file. Notes written by a job reach the next prompt marked unverified; a notes symlink out of the workspace is reported unreadable and its target never enters a request. A job reads its run's step records — a `meta` matching the committed observation, the whole command that the prompt cut, and the preview — and a later run's job reads the earlier run's goal, result, and a step's command; creating a file, a step directory, or a run directory, overwriting the goal or a preview, or removing a step's records under `/canti/runs` fails read-only and leaves the records as published. A configured extra mount is readable and not writable | Docker is the isolation boundary: a shared kernel and the daemon's own security. The test credential is fake. The workspace is writable by the agent by design. A non-default network lets jobs reach the network. Every job can read the step records of every run of its state directory |
| Complete loop | Passed | `TestRunDrivesExecAndReturn`, `TestRunOnceStepsAcrossProcesses`, and every gate above | The concrete adapter, core, SQLite journal, and Docker executor drive exec, observation, a second exec that reads the first one's file, and return in one isolated workspace, in one process or one process per step | Scripted provider |
| Live provider | Partial | `anthropic.TestLiveDecision` and `openai.TestLiveDecision` (`live` tag), manual `canti run` sessions, and the live evaluations under [Live provider findings](#live-provider-findings) | With `canti-anthropic/1`: one real request with structured output decodes to one decision; two CLI runs drive real model decisions through the SQLite journal and Docker executor to `return`, one with structured output and one without, in which the first command failed with exit status 1 and the next decision corrected it. With `canti-anthropic/2`: block prompts with cache breakpoints are accepted and answered through runs of up to 197 steps and 4.5 M input tokens; 502 responses and timeouts of an outage are recorded as incomplete attempts and retried with the frozen bytes; two runs killed with SIGKILL mid-run resume to completion with every effect started once; with `canti-openai-responses/1` and `/1-tool`: live decisions and CLI runs through a relay | One model per evaluation, each through a gateway or relay, not the original platform; the 2026-09-24 evaluation's decisions pass a proxy that enforces the decision schema, because its gateway ignores structured output, and its usage fields do not report caching; the 2026-10-03 gateway reports cache reads, not checked against billing; refusals, fallbacks, and rate limits are not exercised (two 429 responses in a run set aside during an outage were retried as incomplete attempts) |

The live tests are opt-in, name their model, and may be billed; see
[Development](development.md) for their variables:

```sh
ANTHROPIC_API_KEY=... ANTHROPIC_MODEL=... go test -count=1 -tags live -run TestLiveDecision -v ./runtime/internal/harness/anthropic
OPENAI_API_KEY=... OPENAI_MODEL=... go test -count=1 -tags live -run TestLiveDecision -v ./runtime/internal/harness/openai
```

Neither a successful command nor a `return` decision proves the user's task
succeeded. Do not claim exactly-once arbitrary shell effects: job identity
deduplication does not cover effects inside a command or later semantic
repeats. SIGKILL tests do not establish durability across power loss or OS
crashes. The tests keep their files in the platform's temporary directory,
a tmpfs on the verified host; the SQLite suite and the long-history gate also
passed on ext4.

## Live provider findings

Every live run used one model through a gateway or relay, never a provider's
original platform; run IDs, journals, and runner output are kept outside the
repository, and cost is not measured.

- **Earlier adapter `canti-anthropic/1`, 2026-09-23.** One structured-output
  request decoded to one decision, and two `canti run` sessions, one with
  structured output and one without, reached `return` through the SQLite
  journal and Docker executor; in the second, the model corrected a failed
  first command at the next step.
- **Context changes, `canti-anthropic/2`, 2026-09-24.** 50 runs of synthetic
  long-horizon tasks, whose decisions passed a proxy that enforced the
  decision schema because the gateway ignored structured output. With the
  notes file, every current arm passed the deploy task and all but one run met
  the memory outcome, and a 64 KiB context matched or beat 256 KiB on every
  outcome with fewer tokens in three of four pairs; without notes or step
  records, planted facts reached at most a third of the requests that needed
  them and runs took several times the steps and tokens, while step records
  restored them to 96–100%. Consecutive requests
  shared most of their user message as whole leading blocks. Outage failures
  were retried with the frozen bytes, and two runs killed with SIGKILL resumed
  to completion with every effect started once. The gateway's usage measured
  no cache hit.
- **Feedback rounds, `canti-anthropic/3-tool`, 2026-10-03.** Nine sessions
  repairing a Go CLI over several rounds, without a proxy: no protocol
  rejection in 1,823 tool decisions. Specific failing cases, not a bare
  failure signal, let every repair arm fix both root causes. Forced 12-step
  windows multiplied handovers without reducing rereads, so capacity-triggered
  windows stay the default. A host reboot that killed a running job was
  recorded with Docker's exit code; canti now reports such a job unknown.
  Passing acceptance did not mean correctness: held-out checks found a
  symlink escape that every accepted artifact kept.
- **Retrieval system prompt, context versions 7 and 8, 2026-10-08.** Twelve
  paired runs: version 8 passed one task fewer, within the preset criterion,
  and sent 1,996 fewer system bytes per request. Only reads occurred and none
  failed, so the error guidance that replaced the removed rules is untested
  live.
- **Responses adapter, context version 12, 2026-11-07.** Through a
  Responses-compatible relay, the tool transport and text without a schema
  decode live decisions and complete a CLI run across a resume with every
  effect started once. The relay rejects the `json_schema` text format, which
  stops a run as `unrecoverable`.

Label each result as offline fixture, concrete backend integration, or live
provider evidence; a scripted provider tests recovery determinism, not a
model adapter's transport. Fix numeric budgets and acceptance criteria before
an experiment, compare measured peaks as history grows, and report failed
samples with the rest.

## Implementation order

The V1 implementation order is complete: the SQLite journal with recovery
reads, the Docker executor passing `executortest.Check`, the Anthropic adapter
with the bounded input builder, and the loop and CLI exercised by the gates
above.

Beyond V1:

1. Record live provider evidence on Anthropic's API, with structured output
   instead of the evaluation proxy, including whether the provider reuses the
   marked prompt prefix; repeat the live evaluation with more tasks and runs;
   and exercise provider behavior the scripted server cannot: rate limits,
   refusals, and fallbacks.
2. Test OS crash and power loss on a durable filesystem, Docker daemon
   restarts, and host reboots.
3. Stream job output to its artifact while the job runs, so host disk is
   bounded during a job, and add a filesystem quota for the workspace.
4. Measure cost, and budget prompts in tokens rather than bytes.
5. Record the code revision with each run, and replace the pathname lock with
   one a same-user process cannot bypass.

Revisit interface shapes when a concrete transaction requires it. Run rules
stay in the core's transition table; I/O, storage, and execution stay in the
harness. Neither layer restates the other's responsibility.
