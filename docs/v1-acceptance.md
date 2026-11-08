# V1 acceptance

V1 combines a stateless core with a durable, bounded single-machine harness:
a SQLite journal, a Docker executor, model adapters for Anthropic Messages and
OpenAI Responses, and a CLI. Every integration gate below has concrete-backend
evidence except where its status says otherwise. The model in every
end-to-end test is a scripted local HTTP server: the adapter's HTTP path is
exercised, a provider's behavior is not. Live provider evidence is summarized
under [Live provider findings](#live-provider-findings).

Current source uses Context18, semantics7, schema4 and RPC7. Execution is an
explicit choice: ephemeral per-effect containers or an attached persistent
environment. The latter retains its prepared container, private HOME, installed
tools and services between commands. Its concrete acceptance evidence is
recorded under [Persistent environments](#persistent-environments). Historical
live observations below retain the context and adapter versions they name;
there was no new live-provider evaluation for Context18.

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
| Runner cancellation | A cancelled commit context writes nothing. Controller cancellation leaves accepted facts for restart; repeated durable run stop requests remain legal in cancelling and needs_resolution. `TestStopDeadlineKeepsPostLatchWorkAccountedUntilQuiescent` verifies that a Stop deadline can return while Host retains ownership of its cancelled controller | Recording store, real SQLite and deterministic blocked executor I/O |
| Permitted facts | Each event accepts only its own records; missing, mismatched, or extraneous facts write nothing and wrap `ErrFacts`; core sentinels survive `Commit` | Recording store |
| Stable effect and event identity | Effect ID repeatability and ambiguous-separator tests; every event exposes a unique persisted kind, and a test pins current `SemanticsVersion` at 7; the current-version recorded exploration fails until a changed rule is recorded under a new version | A rule outside the explored limits needs its own test; no migration between semantics versions exists |
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
| Explicit focus | `canti:focus` replaces a selection, including empty selection; malformed, duplicate, unavailable or corrupted references fail. Outcome, context head and progress commit atomically. QA tests cover lost acknowledgement, source removal and context provenance corruption | Context17 introduced the replacement for the historical carry policy; Context18 also covers native-only retrieval in attached mode |
| Retrieval system prompt | Silent retrieval rules remain within the 2,048-byte section budget. Discovery covers purpose as well as command and preview; canti actions and marked discovery steps are skipped | No comparative evaluation gate |
| Provider usage and confirmed overflow | Adapter tests preserve raw responses/usage and reject duplicate or case-variant discriminator signals. Coverage distinguishes unknown, reported zero and lost attempts; checked arithmetic, terminal settlement, budget thresholds and cancellation are tested on real SQLite. Initial orientation-only overflow stops on its first send | Offline HTTP fixtures and SQLite; live overflow is separate evidence |
| Ephemeral Docker executor | Against a fake daemon: missing versus lost jobs, no create once the context is done, no restart of an unlatched or failed start, a start never sent twice, a rejected start as `failed_to_start`, launch outliving shutdown, per-call deadlines, the API version the daemon supports, and a loop that gives up on a hung daemon with an unknown outcome; a bind after the fence recording a cancelled job without creating a container; a record published before its container is released, release retries, failed and interrupted captures, and strict records | Fake daemon |
| SQLite journal | On real database files: durable pragmas, schema refusal, concurrent first opens, sequence compare-and-set, identical-retry confirmation without WAL growth, conflicting content never overwritten, broken chains, rollback of an injected failure after the insert, strict decoding refusing unknown kinds, fields, and semantics, one winner per sequence across two stores, and bounded reads. `TestCommitChecksEffectsAgainstTheirAcceptanceInItsTransaction`, `TestCommitRejectsContradictoryExecutionReceipts`, and `TestAcceptanceIdentityUsesImmutableRunConfiguration` validate receipts and configuration identity atomically without backend queries | Covered in the gates below as well |
| Step records and historical snapshot repair | Published retrieval steps include evidence.json and exact evidence/NNNN files. Native historical reads use an indexed step lookup to repair missing derived snapshots from committed bytes. QA probes evidence/manifest/both loss, removed original, later unrelated steps and reset, same-size conflict, unknown source and cancellation | Shell reads do not trigger repair; conflicting derived content is never overwritten |
| CLI | Usage and exit codes, credential removal, refusal without a credential, missing runs, locked workspaces, state inside the workspace, open-run refusal, crash-and-resume around `response_saved`, a signal during setup, first and second signals, resolution and stops of unknown outcomes without Docker, paged history, status of one commit, and workspace binding, mostly in subprocesses; a run whose stored config holds the removed `history` field, as revisions before `552644d` wrote it, is refused by every command with an unreadable-config error naming the remedy, a fresh `-state`, and nothing is committed or sent (`TestRunsOfAnEarlierRevisionAreRefused`) | Without Docker, a bind fails and becomes an unknown outcome; only that one earlier config is tested |

## Integration gates

`make integration` runs tests against real SQLite files and a Docker
daemon. The original per-effect Docker gates in this table use ephemeral mode.
Once a run halts, those CLI gates that dispatch jobs check that no
container of their state directory remains and that every bound effect and no
other has a final job record naming the job committed for it and matching the
committed observation; the exceptions are the notes gates, which check only
that no container is left running, and the lost-job gate, which requires that
no record exists. The recovery gates also replay the recorded events
through `core.State.Next` from the fresh state and require every recorded
state in an unbroken sequence. Each gate case logs an `evidence` line with its
injected failure, observed result, and measurements. Attached environments have
a different lifecycle: successful commands retain the environment and its
services, and explicit environment-wide cancellation stops it; they do not
inherit ephemeral network, capability or cleanup defaults.

The original acceptance runs used Linux (WSL2) with Docker Engine 29.7.
Earlier C2 results below were rerun on Linux/amd64 with Docker Engine 29.9.1-1,
Go 1.26.10 and runtime/test source `6a6b8ce223dec08713890285e6a660e44ad09bf0`:
`go test -count=1 -race ./...` passed in 307 s, and `make integration` passed in
756 s against real SQLite and Docker, with scripted model HTTP. That checkpoint
preceded persistent environments. *Passed* means the listed tests passed at that
checkpoint; *Partial* retains the missing evidence named in Limits. Final-source
results are recorded below. This validation made no live model calls and establishes
neither daemon-restart nor power-loss durability.


| Gate | Status | Tests | What they assert | Limits |
| --- | --- | --- | --- | --- |
| Transactional facts | Passed | `sqlite.TestCommitRollsBackFailureAfterInsert`, `TestCrashKeepsAcknowledgedCommits`, `TestCommitPersistsTransitionsAndTotalsAcrossReopen`, `TestConcurrentStoresCommitOneTransitionPerSeq` | A transition, its facts, and the run totals are one transaction: decision plus effect or return is one `response_loaded` row, observation plus progress one `observed` row. A failure injected after the insert leaves no row and unchanged totals; a child killed with SIGKILL after at least 200 acknowledged commits loses none, keeps at most the one in flight, and replays to the stored state and totals | Failure injected at one point, the totals update; SIGKILL, not power loss |
| Persistent public workflow | Passed, with limits below | Attached controller, host, CLI/TUI, Context18 and RPC7 checks under [Persistent environments](#persistent-environments) | Pure reconstruction and offline replay; validated admission; retained tools, HOME, services and output; native retrieval; stop/restart and explicit retirement | Local scripted providers and concrete Docker; no new live-provider evaluation |
| Ephemeral: Cold restart after every step | Passed | `TestGateColdRestartAfterEveryStep`, `TestRunOnceStepsAcrossProcesses`, `harness.TestLoopColdRestartAfterEveryStepMatchesAnUninterruptedRun` | A new CLI process per current intent for exec, exec, return each commits exactly its intent's transitions from the journal alone; the kinds, phases, and states of the history equal an uninterrupted run's except for IDs, while the in-process harness test compares whole transitions, facts included; the model receives exactly the frozen bytes, identical to the uninterrupted run's except for the run ID, which the prompt's first block names, and effect IDs; each command ran once; a `resume` of the returned run calls no model; both runs' step records hold every step's command and preview and the result | Processes exit normally; crashes are the next gate |
| Ephemeral: Crash at commit boundaries | Passed | `TestGateCrashAtCommitBoundaries`, `TestACrashAtACommitResumesFromTheJournal`, `docker.TestExecutorPassesStartLatchCheck` | SIGKILL before and after current exec-step commits (`request_frozen`, `attempt_started`, `response_saved`, `response_loaded`, `observed`), around retained-result release, and before the next request. Restart preserves committed rows and frozen bytes, reuses the accepted owner and records one result. `Drive` owns binding/start recovery; no Run job_bound/job_started commits exist. Fake-daemon tests additionally inject binding and terminal publication failure, lost create acknowledgement, and missing launch/stop witnesses | SIGKILL, not power loss; fake publication injection is distinct from real Docker evidence |
| Ephemeral: In-flight job recovery | Passed | `TestGateInFlightJobRecovery`, `docker.TestRestartedExecutorCollectsOriginalJob` | The controller is killed while the accepted effect owns a running container; a fresh controller resumes from effect_saved, inspects that same owner, retains its original output and commits one observation without another launch or model request for the step | Controller restart only; Docker daemon restart and host reboot are not established |
| Ephemeral: Unknown outcomes | Passed | `TestGateUnknownOutcomeOfALostJob`, `TestGateUnknownOutcomeOfAnUnavailableStatus`, `TestAnUnknownOutcomeWaitsForAnExplicitResolution`, `docker.TestRejectedStartIsRecordedAndReleased` | A started job's container removed with `docker rm -f` before any record commits `outcome_unknown` with `ErrJobLost`, and no record or container appears afterwards; container inspection failing six times in a row commits `outcome_unknown` while the job keeps running. For the lost job, a second `resume` and a failing `resolve -job` change nothing and call no model; for the running one, `resolve -job` resumes inspection, which then observes its original result; `resolve -execution` records an operator outcome that the next prompt marks as set by explicit resolution. A start the daemon rejects (an unknown user) is reported `failed_to_start`, recorded, and released instead of unknown, and nothing ran | Two injected causes against the daemon; a hung daemon only against a fake daemon; an operator resolution is trusted as given, and `resolve -execution` records the outcome only, leaving a job that may still run, a created container that never started, or the exited container of a killed job in place. At this record's revision a job killed outside canti, as by an abrupt host reboot, was observed as exited with Docker's exit code; later revisions report it unknown (`docker.TestOnlyARecordedExitIsFinal`, `docker.TestAnOutOfMemoryKillIsFinalOnlyWhenRecorded`, `TestGateUnknownOutcomeOfAJobKilledOutsideCanti`, with `docker kill` standing in for a daemon restart or host reboot), keep its container for `docker logs canti-<effect ID>` until it is removed by hand, refuse `resolve -job` for it without a commit, and report a signal the job observes, as from a graceful daemon shutdown, as its exit status |
| Ephemeral: Single workspace writer | Passed | `TestGateSingleWorkspaceWriterAndTakeover`, `TestWritersRefuseALockedWorkspace` | Competing commands cannot own the same state directory. After controller SIGKILL, a new owner resumes the accepted effect from effect_saved and collects its original job. Host shutdown retains the lock until operations quiesce; a Stop deadline does not release ownership of post-latch work | flock protects a pathname; same-user replacement and a second state directory for the same workspace remain outside the exclusion guarantee |
| Frozen request replay | Passed | `TestGateFrozenRequestReplay` (3 cases), `TestGateFiniteRuns/attempts` | The first attempt answers with HTTP 529, with `stop_reason` `max_tokens` around a complete decision, or with a body cut mid-stream. `response_incomplete` records the failure and the partial body; the second attempt sends byte-identical bytes as a distinct attempt; the frozen model, protocol, and adapter are recorded; the truncated decision never runs; usage is counted for every response that reports it | Bytes are captured at the scripted server; a provider's retry behavior is not exercised |
| Decision purpose across processes | Passed | `TestDecisionPurposeReachesLaterRequestsAcrossRestarts` (2 cases) | For each decision format, one process per intent: every request is sent with the bytes the journal froze, a repair revision built by a new process keeps the stable blocks, and later requests show each committed purpose, a long one cut at 512 bytes, under the system prompt and schema of the run's transport | Scripted provider |
| Long history | Passed | `harness.TestLoopBoundsRequestsAsHistoryGrows`, `sqlite.TestReadsStayBoundedByTheirLimit`, `sqlite.TestRecentPagesBackwardBySequence` | With budgets derived from the 64 KiB prompt total before measuring, a restarted controller builds the next request through `Loop.Step` on a newly opened store at 50, 500, and 2,000 stored steps: the request stays within the total plus 16 KiB and the prompt within the total; the same number of latest steps is shown at every size, no more than the total can hold; the records read are the whole history or, at every larger size, the same number, at most those steps plus one page of 64, in the same reads; allocations and build time stay within ceilings and ratios while stored history grows to 12 MB of commands and previews; the oldest facts still read back. With no step cap, a loop shows exactly the steps a prompt of the whole history shows, at 16, 32, and 48 KiB and 1 MiB totals over 128 steps, reading one, one, two, and two pages; a failing second page returns its error and freezes and commits nothing. `Recent` returns the right records for every cursor and limit, including resolved steps and a final `steps_exhausted` step, and pages 2,000 steps backward by sequence; recovery queries, including a page deep in the history, use their indexes without a sort | Offline model: the history is seeded through `Runner.Commit` and requests are frozen by the real adapter but never sent; bytes, not tokens; allocations, not resident memory; three sizes of nine repetitions on one machine; measured at the harness default total of 64 KiB, not the CLI's 256 KiB; the allocation ceiling was raised from 4 MiB to 8 MiB after measuring, because each build decodes a whole page of records |
| Ephemeral: Large output | Partial | `TestGateLargeOutput` (2 cases), `docker.TestOutputIsBoundedAndVerified` | A 20,000,012-byte output with a 1 MiB per-job limit keeps exactly its first 1 MiB as the artifact, reports `kept 1048576 of 20000012 bytes` and an incomplete output, and previews head and tail within 4 KiB; the next prompt reports the truncation and stays under 64 KiB, and the next job reads the artifact through the read-only mount; a run output budget of 1 MiB stops the run as `budget_exhausted` with the observation kept; a retained terminal receipt keeps its capture-time metadata after external artifact deletion, while an archive read reports the actual missing data | Output is copied from Docker's log once the job finishes, not streamed: until then that log is bounded only by the job's run time. The run budget is checked between steps, so a run can exceed it by one per-job limit. Neither is a filesystem quota: workspace writes, a job's container layer outside the mounts, the journal, and the container and log of a finished job whose removal failed until a later sample of that effect retries it are not limited, and a full disk is not injected; a final job record that cannot be written leaves the job to the inspect retries and then an unknown outcome. Docker's json-file log replaces invalid UTF-8, so artifacts of binary output are not byte-exact |
| Ephemeral: Cancellation and timeout | Passed | `TestGateCancellationAndTimeout` (3 cases), `docker.TestCancelStopsProcessTree`, `TestASignalStopsTheRunPersistently`, `TestASignalDuringSetupStopsTheRunPersistently`, `TestASecondSignalAbandonsTheCleanup` | A 2 s job timeout on a two-process tree is observed as `timed_out`, and the job's record shows it ran 2 to 12 s; `stop` after the controller was killed commits `stop_requested` and then the job's cancelled result, which stops the run, within its cleanup deadline; SIGINT to `run` does the same and exits with status 3, and a signal during `run` or `resume` setup stops the run before any model call; `Cancel` of a process tree, including one that ignores SIGTERM, returns within the stop grace plus 5 s, the latter after at least the grace, and leaves the job cancelled; no container remains; a second signal abandons cleanup and a later `stop` finishes | Process-tree cleanup is Docker's container stop and kill, evidenced by the stop returning and the container being removed, not by listing processes; the job timeout, and a restarted controller's choice between `timed_out` and `cancelled`, rely on the daemon sharing the host clock; a durable fence prevents launch; only a stop witness published after observing a running owner permits cancelled classification, including a natural exit racing that request. A first Cancel of an already-exited job preserves its exit result, and later Cancel leaves retained terminal receipts unchanged; a stop's cleanup deadline must also cover copying the job's Docker log into its artifact, so a job with a large output needs a longer `-cleanup`; timing assertions allow 5 s of slack |
| Finite runs | Partial | `TestGateFiniteRuns` (4 cases), `TestGateLargeOutput/disk_budget` | Step, attempt, wall-time, token, and output-byte budgets each stop a run with `steps_exhausted`, `attempts_exhausted`, `timed_out`, or `budget_exhausted`, with failure text on the stop for the measured budgets and on each failed attempt, keep earlier observations, and dispatch nothing after the budget is spent; a wall-time stop cancels the running job and records its result | Cost is not measured; tokens are provider-reported usage, so a run can exceed its token budget by one response; the wall-time stop during a model request is tested only with a fake model |
| Ephemeral: Trust boundary | Passed | `TestGateTrustBoundaryKeepsCredentialsAndStateOut`, `TestGateTrustBoundaryProtectsFacts`, `TestGateTrustBoundaryProtectsStepRecords`, `docker.TestTrustBoundary` | A job sees no host environment variable, credential, or injected proxy; runs as the host uid with no capabilities and only a loopback interface, and its running container has network `none`, `CapDrop` `ALL`, and the host uid and gid; cannot reach the model URL; sees the artifacts, and the step records at `/canti/runs`, read-only, and neither the state directory nor the journal; cannot delete or forge committed artifacts or history, and forgeries stay in the workspace. The credential appears in no request body, CLI output, or state file. Notes written by a job reach the next prompt marked unverified; a notes symlink out of the workspace is reported unreadable and its target never enters a request. A job reads its run's step records — a `meta` matching the committed observation, the whole command that the prompt cut, and the preview — and a later run's job reads the earlier run's goal, result, and a step's command; creating a file, a step directory, or a run directory, overwriting the goal or a preview, or removing a step's records under `/canti/runs` fails read-only and leaves the records as published. A configured extra mount is readable and not writable | Docker is the isolation boundary: a shared kernel and the daemon's own security. The test credential is fake. The workspace is writable by the agent by design. A non-default network lets jobs reach the network. Every job can read the step records of every run of its state directory |
| Ephemeral: Complete loop | Passed | `TestRunDrivesExecAndReturn`, `TestRunOnceStepsAcrossProcesses`, and every gate above | The concrete adapter, core, SQLite journal, and Docker executor drive exec, observation, a second exec that reads the first one's file, and return in one isolated workspace, in one process or one process per step | Scripted provider |
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

## Persistent environments

The accepted effect commits its immutable execution specification before work;
Docker's physical ExecID and the launch witness belong to executor authority,
not the core. Repeated Drive/Inspect calls recover that binding and retained
terminal evidence rather than relaunching the accepted command. The attachment
pins the local Unix daemon origin, exact container ID, authority/incarnation,
helper and mount wiring. Each run separately pins its mapped working directory;
one shared environment can serve different selected workspaces and cwd paths.
Ordinary installed tools, configuration and files remain mutable; the attachment
does not pin a digest of the whole filesystem. `NewAttached` and stored specification reconstruction
remain pure; replay of an already accepted input succeeds without consulting a
missing environment profile. New input requires a live running, unpaused,
non-restarting environment with an open gate and settled retained requests.
Admission is read-only and reserves no foreground slot; dispatch rechecks the
physical contract and acquires the shared environment slot across States.
Execution choice supplies no API or model default. Each command uses a fresh
`/bin/sh -c` with the pinned container user, HOME, PATH and mapped workdir;
shell variables alone do not persist. An explicitly attached `-init` file is
sourced on every command, rather than performed once by admission.

Before creating State, CLI, host, RPC preflight and TUI resolve the prospective
path through its existing parents and use that canonical identity. Missing
children under aliases into a public, spool or workspace bind are refused;
safe outside aliases work. Dangling, looping, inaccessible and non-directory
ancestors fail without a lexical fallback. Actual bind sources must themselves
be canonical, reserved helper/spool destinations cannot be shadowed, and no bind
may expose State or authority. Named-volume internals are not traversed. This
is validation on a trusted local filesystem, not a reservation against later
same-user filesystem replacement or a hostile Docker administrator.

A completed command leaves the prepared environment and background services
running. Stop or timeout is environment-wide: it can end unrelated services
in that environment. A witnessed stop reports physical cessation separately
from the original command's nullable exit result; PID 1 exiting with zero is
not evidence that the command succeeded. The host keeps an in-flight Stop
observable through drive interruption and cleanup, and reports its actual
failure or bounded interruption. An uncertain mutating request quarantines
the incarnation; a stopped sample or elapsed time does not clear that fence.
An acknowledged stop permits explicit same-ID start, retaining files and tools
but ending earlier processes. Unresolved old requests require verified absence
of the exact old container ID and explicit retirement before a new incarnation;
retirement does not settle the old command's unknown result. Invalid terminal
evidence neither frees its slot nor authorizes disruptive cleanup.

Attached mode exposes historical evidence through native `canti:retrieve`
with `read`, `discover`, `list` and `search` operations, plus `canti:focus`,
using logical archive paths; it does not mount
the host journal or archive into the task. Context18 renders that mode explicitly
while preserving ephemeral prompt bytes. Old context and RPC wire records are
append-only. Complete captured output remains complete after restart, with epoch
guidance separate from its capture-time metadata. Bounded helper capture is
not a quota for workspace writes, private HOME, service logs, container layers
or journal growth; agent-writable spool contents alone are not trusted terminal
proof when daemon execution evidence is unavailable.

### Evidence checkpoints

Retained reports are local task artifacts under
`.git/agent-supervision/persistent-environment/qa/`, outside tracked source.
The paths below identify their manifests, exact argv, hashes and raw results;
they are evidence locations, not additional tests shipped with the repository.
All public providers in this section were local scripted HTTP servers.

| Evidence class and exact source | Observed result | Boundary and limit |
| --- | --- | --- |
| Offline/fake HTTP plus real SQLite; `391dd9555581966c299134583a9d9aa0633ff7b4` | The focused command recorded below passed in 2.320 s: 13 parents and 3 subtests. Exact selector and unchanged long TMPDIR are in `public-391dd95/focused-long-manifest.json` | Canonical prospective State, alias-safe TUI startup, pre-creation rejection, pure replay/reconstruction, stop ownership and socket-fixture portability. The focused daemon fixtures are not real Docker evidence |
| Actual controller processes with fake HTTP; `f152bb348683cd2fd41e1b2091945d3449ffed57`, corrected at `bc4f9d30abbc60ea0b312110a742ab3ea6cc9e6e` | `controller-f152bb3.md` records 26 passing offline parent tests, including controller-process SIGKILL with delayed launch/stop. `controller-bc4f9d3/check-manifest.json` records the exact selected uncached race command: 12 parents and 11 subtests passed in 4.875 s | Launch ambiguity, durable fencing, invalid terminal refusal, canonical mounts and origin checks; fake delayed launch is distinct from a real delayed Engine ExecStart |
| Real Docker attached integration; `bc4f9d30abbc60ea0b312110a742ab3ea6cc9e6e` | From `runtime`: `go test -json -count=1 -race -tags integration -timeout=3m -run '^TestAttachedDocker' ./internal/harness/docker` passed in 5.727 s, 2 parents and 2 subtests; independent formerly failing edge groups 7/7 and support probes 5/5 passed | Tools/HOME/background service retention, frozen output, cancel/start, canonical bind rejection and named-volume compatibility; original f152 failures remain recorded, not counted as passes |
| Actual controller + private Engine29.9.3/API1.47 faults; `bc4f9d30abbc60ea0b312110a742ab3ea6cc9e6e` | `controller-bc4f9d3.md` and its manifests record 12/12 assertions: SIGKILL of a Cancel controller while a proxy holds a real stop; stopped same-ID stays fenced; exact old-ID removal and retirement permit new-ID work; released old stop returns 404 without killing replacement work; ordinary acknowledged cancel/start works; captured output stays complete | Physical stop-witness nonce/intent digest checked after Pending clears. Does not establish real delayed ExecStart, power-loss durability or recovery of active work across daemon restart |
| Read-only live admission; `00eadd6ff0a3d75cbf178b5fdc604de95441f008` | `readiness-00eadd6.md`: 8 parents and 15 subtests passed uncached race in 4.424 s, with unchanged authority bytes and no State creation or Docker mutation | Refuses stopped/paused/restarting/quarantined or unsettled environments; recovery mapping remains available for stopped/quarantined cancellation |
| Public CLI + real Docker + scripted HTTP; `e86798fe0ad89825bb1c02208cc47a733c5932df`, `a2363636106bfb6d5f9e5bade9054d42279b87af`, final `391dd9555581966c299134583a9d9aa0633ff7b4` | `public-e86798f/checkpoint.md`: 8 scenario groups passed but auto-host owner exit failed; `public-a236363/checkpoint.md`: all 9 passed after stop ownership correction. Final `public-runs/p8/manifest.json`: all 9 passed, owner and `canti stop -host auto` both exit 3 | Installs jq, writes private HOME config, reuses both in later runs/sessions, retains services and frozen outputs, retrieves/focuses native evidence, handles provider-confirmed overflow/reset, blocks another State's command, observes cessation and explicitly restarts the same ID. p8 uses the existing auto host after restart; earlier fixture mistakes and failed exits are retained separately |
| Public preflight and TUI pipe + real Docker; initial `e86798fe0ad89825bb1c02208cc47a733c5932df`, final `391dd9555581966c299134583a9d9aa0633ff7b4` | Initial `public-e86798f/edges/manifest.json`: 7/7, including read-only preflight, missing-profile input replay with unchanged provider count, and two TUI inputs using an owned runtime. Corrected `state-parent-7/manifest.json` State-parent CLI/TUI matrix: 10/10 passed | Initial e867 direct lexical rejection passed but symlink-parent State exposure failed and was a blocker. The corrected matrix exercises public/spool alias rejection before any State files and safe outside aliases. Later ba7faa5/391dd95 changes only shorten test-fixture sockets; final long-TMPDIR regressions pass above |
| Actual ordinary Dev Container; `e86798fe0ad89825bb1c02208cc47a733c5932df` | `examples-e86798f/ordinary02/manifest.json` and its `public-package/manifest.json`: actual Dev Containers CLI0.90.0 + Compose5.7.0 up/attach/exec, package install and another-session reuse passed | UID1000, writable private HOME, sudo/network/Python/Go; the tested CLI invocation used isolated CLI state and no host HOME, SSH, Git credentials or Docker socket forwarding. IDE credential-sharing defaults were not independently tested or overridden. Command-local `COMPOSE_BAKE=false devcontainer up ...` avoids the tested generated-Dockerfile build-context restriction; default Bake failure is retained. This workaround is specific to the Compose-backed invocation |
| Actual official DinD Feature4.1.3; `e86798fe0ad89825bb1c02208cc47a733c5932df`, final `391dd9555581966c299134583a9d9aa0633ff7b4` | Actual `devcontainer up`, attachment and `devcontainer exec ... make integration` passed initially in 411.857 s. Final `private-integration-g391/manifest.json`: actual up reused the same outer container, then `make integration` passed in 412.744 s: 856 parents and 1,250 subtests, 3 parent skips (two optional external corpora and the fixture-only direct invocation of `TestAcceptanceReleasedResources`, which root acceptance separately invokes), 19 successful packages and 4 no-test packages; no unfinished tests | Privileged/init example uses private Docker/containerd volumes, no outer socket mount; inner Engine29.9.3-1. Initial nonverbose run did not expose skip counts. Private inner inventory was empty of containers/volumes after initial tests. This is concrete backend + scripted-provider evidence, not a live model or general nested-Docker isolation claim |

At f152bb3, fresh-process Drive/Inspect repetition sampled ten times retained
the same ExecID and acknowledgement hash; Engine events recorded one physical
`exec_start` and the command's marker was written once. This counts physical
starts, not raw HTTP sends. Loss of all authority/helper/spool contents, even
with mount-root inodes retained, produced a fresh authority identity and refused
reattachment of the old container. These results are in `controller-f152bb3.md`.

The final focused command, with the original long task TMPDIR unchanged, was:

```sh
go test -json -count=1 -race -timeout=3m -run '^(TestSetupAdoptsCanonicalStateBeforeConnecting|TestCLIUsesCanonicalProspectiveState|TestResolveStateReturnsCanonicalProspectiveDirectory|TestHostCanonicalStateSurvivesAliasRetargeting|TestPersistentPreflightChecksActualMountsBeforeCreatingState|TestPersistentReplayUsesStoredAttachmentWithoutEnvironmentAccess|TestPersistentReconstructionRejectsEveryMountExposingState|TestNewPersistentAdmissionRequiresLiveValidationBeforeRunCommit|TestPersistentAcceptancePinsTheStoredPhysicalExecutionContract|TestStopOwnsProgressThroughDriveHandoff|TestExternalStopKeepsOwnerAttached|TestRunExecutionModeAndProvisioningFlagsFailBeforeStateCreation|TestChatRequiresExplicitExecutionModeAndRejectsProvisioningFlags)$' ./runtime/internal/host ./runtime/cmd/canti ./runtime/internal/harness/docker ./apps/tui/internal/session ./apps/tui/cmd/canti-tui
```

The actual public scenario command at the final source was
`python3 <qa>/public-scenarios.py --repo <qa-worktree> --commit
391dd9555581966c299134583a9d9aa0633ff7b4 --canti
<qa>/public-391dd95/bin/canti --run-id p8 --docker-host
unix:///var/run/docker.sock --execute`; the retained manifest gives each expanded
`canti run`, `stop`, `environment start`, history and status argv. The examples
used the checked-in `.devcontainer/devcontainer.json` files via actual
`devcontainer up --workspace-folder <qa-worktree> --config <example-config>`
and `canti environment attach`; task-specific selectors and absolute paths are
recorded in their manifests. Container provisioning and initial package setup
are operator/example actions; admission does not provision an environment.
The ordinary example permits networking and sudo; official DinD intentionally
adds privilege. Both example results describe the recorded CLI invocations with
isolated CLI state, not IDE-managed credential sharing. Neither has the ephemeral `network=none` or
`CapDrop=ALL` guarantee.

### Final source gates

QA completed the following commands on Linux/amd64, Go1.26.10 and host
Engine29.9.1-1 at exact source
`391dd9555581966c299134583a9d9aa0633ff7b4` (tree
`441f757c5439d4f865549137c002ce11221bfbc7`). Each listed command exited zero;
`gate-runs/g391/manifest.json`, its JSON events and the separate
`check-lint-corrected/manifest.json` retain the results. The consolidated
`acceptance-391dd95.md` and `.json` record completion and the final resource
audit: all 18 task containers stopped, all 9 preexisting container states and
the shared daemon identity unchanged; task images, seven new volumes and the
ordinary network are retained and inventoried rather than claimed removed.
This acceptance update
changes documentation only; these timings describe the tested source, not a
rerun on its documentation successor.

| Exact command from the repository root | Observed result |
| --- | --- |
| `make check lint` (corrected observer) | Passed, 199.365 s; zero lint issues; includes native dependency checks: 1,021 parent and 4,879 subtest passes, 2 optional external-corpus parent skips and 1 upstream Ultraviolet styled-wide-cell fixture subtest skip |
| `go test -count=1 -race ./runtime/... ./sdk/go/... ./apps/tui/...` | Passed, 125.345 s; 753 parents and 1,165 subtests; 2 optional external-corpus skips; 18 successful packages and 4 no-test packages |
| `make integration` (host Docker) | Passed, 417.801 s; 856 parents and 1,250 subtests; 3 parent skips; 19 successful packages and 4 no-test packages; real SQLite/Docker with local scripted model HTTP |
| `git diff --check` | Passed, 0.008 s |
| `make commits BASE=4bd3f3fc7a5436d1b1940b42d93d00c9aa4a7e4b` | Passed, 58.229 s; build/vet over 21 commits |
| `make isolation build-smoke` | Passed, 77.172 s; isolated runtime+SDK tests with TUI absent, then independent module builds and runtime/TUI version/help plus helper-protocol smoke checks |

The two external-corpus skips are `TestRetrievalCasefoldQuality` and
`TestRetrievalTermsCorpusExperiment`. Integration's third parent skip is the
fixture-only direct invocation of `TestAcceptanceReleasedResources`; the root
acceptance runner separately invokes that fixture. The private official DinD
integration reported the same test counts as host integration. No suite left an
unfinished test or reported a race. The first check/lint attempt failed because
a QA observer added JSON output to `go test -list`, breaking sequential test-name
enumeration; that failed attempt remains retained and excluded. The corrected
observer leaves listing alone and its independent passing command supplies the
check/lint acceptance evidence. No source assertion was weakened to obtain it.

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
2. Test OS crash and power loss on a durable filesystem, recovery of active
   work across Docker daemon restarts, and host reboots. Private-daemon raw
   probes and controller SIGKILL/delayed-stop tests above cover narrower cases;
   a daemon restart with an already stopped target is not active-run recovery.
3. Stream ephemeral job output to its artifact while the job runs; it is
   currently collected from Docker logs after completion. Attached helper
   capture is already bounded, but neither mode has a workspace or general
   filesystem quota.
4. Measure cost, and budget prompts in tokens rather than bytes.
5. Record the code revision with each run, and replace the pathname lock with
   one a same-user process cannot bypass.

Revisit interface shapes when a concrete transaction requires it. Run rules
stay in the core's transition table; I/O, storage, and execution stay in the
harness. Neither layer restates the other's responsibility.
