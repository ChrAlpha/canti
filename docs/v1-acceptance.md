# V1 acceptance

V1 combines a stateless core with a durable, bounded single-machine harness:
a SQLite journal, a Docker executor, an Anthropic Messages adapter, and a CLI.
Every integration gate below has concrete-backend evidence except where its
status says otherwise. The model in every end-to-end test is a scripted local
HTTP server speaking the Messages format: the adapter's HTTP path is
exercised, a provider's behavior is not. Live provider evidence is limited to
the three manual samples recorded under
[Live provider samples](#live-provider-samples-2026-09-23): one model through
an Anthropic Messages-compatible gateway, with the earlier adapter
`canti-anthropic/1`. None is recorded yet for `canti-anthropic/2`, the only
adapter the current code sends.

## Offline evidence

These tests run in `make check`, and under the race detector in
`go test -count=1 -race ./...`; they use no Docker daemon or credential.

| Property | Evidence | Limit |
| --- | --- | --- |
| Strict decision protocol | Valid/invalid input tests in `core/decode_test.go` | Parsing only |
| Stateless calls | Concurrent decode tests and race checks | Does not test model or runtime behavior |
| Complete transition table | Every legal phase/event pair asserts the full resulting state and every other pair is rejected unchanged in `core/state_test.go` | Values only |
| Causal event identity | Attempt/response events name their step and attempt; effect events name their effect; echoes from an earlier attempt, step, or effect are invalid, and trajectory tests walk two real steps plus a protocol repair | Values only |
| State invariants | `State.Validate` rejects unreachable projections, including a `job_bound` state with a committed start; legal transitions preserve the invariants; the runner refuses commit/bind/start for invalid states, and the loop validates every snapshot it loads | Values only |
| Budgets and stop reasons | Transport, lost-send, protocol, and step exhaustion in the core; the loop stops on spent wall-time (before a step, during a model request, during a job), token, and output-byte budgets, and cancels a job past its job timeout as `timed_out` | Fake model and executor |
| Counted attempts | `AttemptStarted` in `request_sent` consumes an attempt and stops at `MaxAttempts`; protocol repair returns to `idle` and freezes a new revision under the same budget | Values and fakes |
| Sequenced commits | Every accepted event advances `Snapshot.Seq` even on state self-loops; a stale snapshot cannot commit after a self-loop; a `From` outside the committed chain is rejected with `ErrBrokenChain`; an identical retry after a lost acknowledgement is confirmed without a second record | Recording store; the SQLite rows below repeat these on a real file |
| Commit before bind, bind before start | A shared event log shows the effect commit before `Bind`; `Start` runs only from a committed `job_bound` snapshot; a store failure after either call leaves the prior phase, and also wraps the executor's error when the executor failed | Recording store and executor |
| Runner cancellation | Cancel before commit, before bind/start, and around either call leaves the snapshot unchanged and writes nothing; repeated stop requests in `cancelling` and `needs_resolution` are recorded without changing phase | Recording store and executor |
| Permitted facts | Each event accepts only its own records; missing, mismatched, or extraneous facts write nothing and wrap `ErrFacts`; core sentinels survive `Commit` | Recording store |
| Stable effect and event identity | Effect ID repeatability and ambiguous-separator tests; every event exposes a unique persisted kind, and a test pins `SemanticsVersion` at 1 | No test forces a version bump when a rule changes; no migration between semantics versions exists |
| Executor start latch | `executortest.Check` passes an in-memory reference and rejects each single-clause mutant with the cases it violates | Offline reference; the Docker run is in the crash-boundary gate |
| Loop | `harness.Loop` over a real SQLite journal with a fake model and executor: exact commit sequences for exec, observation, and return; frozen-byte resends; protocol repair; budgets; unknown outcomes halting until `ResolveJob` or `Resolve`; persistent stop; controller shutdown committing nothing; a new loop and store for every step committing the same 24 transitions as an uninterrupted run | Restarts are in-process |
| Bounded input | `Frame.Validate` positions and required fields; per-section and total byte limits; drop order under the total; fences and markers for untrusted text; UTF-8 safety; 10,000-step frames render the same prompt as 1,000-step frames; `ReadNotes` refuses escaping symlinks and FIFOs | Bytes, not tokens |
| Whole goal and constraints | A goal of 32 KiB and constraints of 16 KiB after trimming, each ending in a one- to four-byte rune, are rendered whole, also beside the environment, notes, and a rejected response at their limits within the 64 KiB total; one byte more fails with the limit named and no prompt (`TestPromptKeepsTheGoalAndConstraintsWhole`). The loop stops such a run as `unrecoverable` with one `stop_requested` transition and no send (`TestLoopStopsWhenRequestCannotBeBuilt`), and `canti run` exits with status 2 before creating the state directory or calling the model, while a run at the limits sends both whole (`TestRunRefusesAGoalOrConstraintsOverTheirLimit`) | Bytes, not tokens |
| Prompt capacity | No step count caps the steps shown: over 128 steps a loop shows exactly what a prompt of the whole history shows at four totals, and `Recent` pages backward by sequence (`TestLoopShowsTheLatestStepsThatFitThePrompt`, `sqlite.TestRecentPagesBackwardBySequence`; see the long-history gate). Through the CLI, a run with a 32 KiB goal, 16 KiB constraints, and six steps each resolved with a 4,000-byte note shows step 1 in the step 7 prompt, built by a later `resume` process, at the default `-context-bytes` of 256 KiB, and counts it as not shown at 65,536; a smaller total is refused (`TestTheContextBytesOfTheRunBoundEveryPrompt`, `TestUsageAndVersion`) | Bytes, not tokens; the CLI test checks only whether step 1 is shown or counted as not shown |
| Stable prompt prefix | Over six steps, with and without two earlier steps not shown, and with notes, attempts, and rejected responses changing, each prompt's stable blocks extend the previous prompt's byte for byte, a completed step appending its own block; when the total drops step 1, or shorter notes let a dropped step be shown again, the first block is kept and the blocks after it change (`TestPromptAppendsEachCompletedStepToItsStablePrefix`, `TestPromptShowsADroppedStepAgainWhenTheNotesShrink`). The first block, which names the artifact directory and the run's step records, is the same at steps 1, 2, and 13 and in a repair at attempt 2 of step 44, and no later block names the records (`TestPromptNamesTheStepRecordsInItsFirstBlock`) | Rendering only; whether a provider reuses the prefix is not tested |
| Cache breakpoints | The frozen body holds one user message with a text block per prompt block, in order; the first and the last stable block, and no other, carry an `ephemeral` `cache_control`, also for prompts rendered at steps 1 to 3; none does without a stable block, and one does when a single block is stable (`anthropic.TestFreezeMarksTheFirstAndLastStableBlocks`) | Request bytes only: no provider cache hit or cache-read usage has been measured, and the test gateway used for live evaluation ignores `cache_control` |
| Anthropic adapter | Against a local HTTP server: frozen body sent verbatim with the right headers; status, stop-reason, truncation, size, timeout, redirect, and context handling; decision extraction; a request frozen with fallbacks keeps its beta header when a model configured without them sends it, and one whose protocol is unknown or whose adapter is not `canti-anthropic/2`, the earlier `canti-anthropic/1` included, stops as `unrecoverable` without a send (`TestCompleteHonorsFrozenProtocol`) | No live provider; no live sample used `canti-anthropic/2` |
| Docker executor | Against a fake docker CLI: missing versus lost jobs, no create once the context is done, no restart of an unlatched or failed start, a rejected start as `failed_to_start`, launch outliving shutdown, per-call deadlines, and a loop that gives up on a hung daemon with an unknown outcome; a bind after the fence creating a cancelled job that never starts; a record published before its container is released, release retries, failed and interrupted captures, and strict records | Fake CLI |
| SQLite journal | On real database files: durable pragmas, schema refusal, concurrent first opens, sequence compare-and-set, identical-retry confirmation without WAL growth, conflicting content never overwritten, broken chains, rollback of an injected failure after the insert, strict decoding refusing unknown kinds, fields, and semantics, one winner per sequence across two stores, and bounded reads | Covered in the gates below as well |
| Step records | Over a real SQLite journal, `PublishHistory` lays out the goal, constraints, every completed step's `meta`, whole command, and preview — exited, failed to start, timed out, and resolved — and a returned or stopped run's result; publishing again rewrites nothing; a lost run or `steps` directory is rebuilt by paging backward; every publication removes stale temporaries, and otherwise only the latest step and a missing goal, constraints, or result are added; a history root that is a file, a failing read, a stale temporary it cannot remove, an unlistable run directory, a missing run, and the run IDs `..` and `a/b` fail it, and so does a symlink in place of any record it looks at, a directory where it expects a file, or a file where it expects a directory, leaving a link's target untouched (`TestPublishHistory*`, 6 tests). Every request is frozen with exactly the earlier steps published and no result, and the halt publishes the result (`TestLoopPublishesEveryStepBeforeTheNextRequest`); while nothing can be published, `Step` returns an error and freezes and commits nothing, and once the history root is restored the next `Step` freezes the request (`TestLoopFreezesNothingWhileStepRecordsCannotBePublished`); `Run` publishes a final `steps_exhausted` step and its result, and a failed publication at a halt is logged without changing the result and repaired by a later `Run` (`TestLoopRunPublishesAHaltedRun`). `canti run` restores the previous run's removed final step and result before it starts, and refuses, creating no run and calling no model, when they cannot be published (`TestRunPublishesThePreviousRunFirst`) | A projection nothing in canti reads back; concurrent publications of one run are unsupported and untested; a record that a same-user process edits or removes is repaired only where a publication would add it |
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

*Passed* means every listed test passed at the revision of the
[evidence record](#revision-d93633e-2026-09-23), against real SQLite files
and, where jobs run, the Docker daemon recorded there, with the scripted
provider. *Partial* means part of the required evidence is missing; the
Limits column names it. Results apply only to that revision and environment.

| Gate | Status | Tests | What they assert | Limits |
| --- | --- | --- | --- | --- |
| Transactional facts | Passed | `sqlite.TestCommitRollsBackFailureAfterInsert`, `TestCrashKeepsAcknowledgedCommits`, `TestCommitPersistsTransitionsAndTotalsAcrossReopen`, `TestConcurrentStoresCommitOneTransitionPerSeq` | A transition, its facts, and the run totals are one transaction: decision plus effect or return is one `response_loaded` row, observation plus progress one `observed` row. A failure injected after the insert leaves no row and unchanged totals; a child killed with SIGKILL after at least 200 acknowledged commits loses none, keeps at most the one in flight, and replays to the stored state and totals | Failure injected at one point, the totals update; SIGKILL, not power loss |
| Cold restart after every step | Passed | `TestGateColdRestartAfterEveryStep`, `TestRunOnceStepsAcrossProcesses`, `harness.TestLoopColdRestartAfterEveryStepMatchesAnUninterruptedRun` | A new CLI process per intent — 15 processes for exec, exec, return — each commits exactly its intent's transitions from the journal alone; the kinds, phases, and states of the history equal an uninterrupted run's except for IDs, while the in-process harness test compares whole transitions, facts included; the model receives exactly the frozen bytes, identical to the uninterrupted run's except for the run ID, which the prompt's first block names, and effect IDs; each command ran once; a `resume` of the returned run calls no model; both runs' step records hold every step's command and preview and the result | Processes exit normally; crashes are the next gate |
| Crash at commit boundaries | Passed | `TestGateCrashAtCommitBoundaries` (16 cases), `TestACrashAtACommitResumesFromTheJournal`, `docker.TestExecutorPassesStartLatchCheck`, `docker.TestBindReleasesAnEarlierJobWhoseRemovalFailed` | SIGKILL immediately before and after the first commit of each kind of an exec step (`request_frozen`, `attempt_started`, `response_saved`, `response_loaded`, `job_bound`, `job_started`, `observed`), inside the `docker rm` that releases a recorded job, and right after step 1's `observed` and then, in the `resume` that follows, right before step 2's `request_frozen`; `resume` then returns. Confirmed transitions are unchanged; the history matches the expected sequence exactly, with an attempt interrupted after its commit counted as a second `attempt_started`; a request is sent again, byte-identical, only when the killed process had sent it; each step's command ran exactly once; and a job recorded but not yet released is released without running again. After the kill that follows step 1's `observed`, step 1 has no records; the `resume` killed before step 2's `request_frozen` has published its command and preview and committed nothing; in every case the goal, each step's command and preview, and the result match the history. Against the daemon, `executortest.Check` drives every call through a new executor, as a restarted or competing controller would: a bind never runs its command, a start runs it at most once even when concurrent, a changed command is refused, and a fence stops a later bind or start; and a removal that failed is retried by the next `Bind` of another effect | The commit hook fires only on the first commit of each kind in a process, so every commit crash is in step 1 except the one before step 2's `request_frozen`; no crash is injected inside a publication of step records; `executortest.Check` injects no crash and sees a start racing a cancel only when the calls overlap; `response_incomplete`, `outcome_unknown`, `stop_requested`, and `resolved` are not crashed; SIGKILL, not power loss |
| In-flight job recovery | Passed | `TestGateInFlightJobRecovery`, `docker.TestRestartedExecutorCollectsOriginalJob` | The controller is killed after `job_started` while its container runs; `resume` finds the same job, collects its original output, commits one observation, binds no second job, sends no new request for the step, and the command ran once | Controller restart only; Docker daemon restarts and host reboots are not tested |
| Unknown outcomes | Passed | `TestGateUnknownOutcomeOfALostJob`, `TestGateUnknownOutcomeOfAnUnavailableStatus`, `TestAnUnknownOutcomeWaitsForAnExplicitResolution`, `docker.TestRejectedStartIsRecordedAndReleased` | A started job's container removed with `docker rm -f` before any record commits `outcome_unknown` with `ErrJobLost`, and no record or container appears afterwards; `container inspect` failing six times in a row commits `outcome_unknown` while the job keeps running. For the lost job, a second `resume` and a failing `resolve -job` change nothing and call no model; for the running one, `resolve -job` resumes inspection, which then observes its original result; `resolve -execution` records an operator outcome that the next prompt marks as set by explicit resolution. A start the daemon rejects (an unknown user) is reported `failed_to_start`, recorded, and released instead of unknown, and nothing ran | Two injected causes against the daemon; a hung daemon only against a fake CLI; an operator resolution is trusted as given, and `resolve -execution` records the outcome only, leaving a job that may still run, or a created container that never started, in place |
| Single workspace writer | Passed | `TestGateSingleWorkspaceWriterAndTakeover`, `TestWritersRefuseALockedWorkspace` | While a run's job is running, concurrent `resume` and `stop` exit with status 6 and change no history, container, or model request; after SIGKILL of the writer, `resume` takes over from `awaiting_observation`, observes the same job before any new decision, and the command ran once | The lock is a `flock` on a pathname in the state directory. A same-user process that replaces the lock file is not excluded; the sequence check still rejects its stale commits, but not its executor actions. A second state directory for the same workspace has its own journal and lock, so nothing detects the conflict and both runs can write the workspace |
| Frozen request replay | Passed | `TestGateFrozenRequestReplay` (3 cases), `TestGateFiniteRuns/attempts` | The first attempt answers with HTTP 529, with `stop_reason` `max_tokens` around a complete decision, or with a body cut mid-stream. `response_incomplete` records the failure and the partial body; the second attempt sends byte-identical bytes as a distinct attempt; the frozen model, protocol, and adapter are recorded; the truncated decision never runs; usage is counted for every response that reports it | Bytes are captured at the scripted server; a provider's retry behavior is not exercised |
| Long history | Passed | `harness.TestLoopBoundsRequestsAsHistoryGrows`, `harness.TestLoopShowsTheLatestStepsThatFitThePrompt`, `sqlite.TestReadsStayBoundedByTheirLimit`, `sqlite.TestRecentPagesBackwardBySequence`, `harness.TestPromptBoundsLongHistory` | With budgets derived from the 64 KiB prompt total before measuring, a restarted controller builds the next request through `Loop.Step` on a newly opened store at 50, 500, and 2,000 stored steps: the request stays within the total plus 16 KiB and the prompt within the total; the same number of latest steps is shown at every size, no more than the total can hold; the records read are the whole history or, at every larger size, the same number, at most those steps plus one page of 64, in the same reads; allocations and build time stay within ceilings and ratios while stored history grows to 12 MB of commands and previews; the oldest facts still read back. With no step cap, a loop shows exactly the steps a prompt of the whole history shows, at 16, 32, and 48 KiB and 1 MiB totals over 128 steps, reading one, one, two, and two pages; a failing second page returns its error and freezes and commits nothing. `Recent` returns the right records for every cursor and limit, including resolved steps and a final `steps_exhausted` step, and pages 2,000 steps backward by sequence; recovery queries, including a page deep in the history, use their indexes without a sort | Offline model: the history is seeded through `Runner.Commit` and requests are frozen by the real adapter but never sent; bytes, not tokens; allocations, not resident memory; three sizes of nine repetitions on one machine; measured at the harness default total of 64 KiB, not the CLI's 256 KiB; the allocation ceiling was raised from 4 MiB to 8 MiB after measuring, because each build decodes a whole page of records |
| Large output | Partial | `TestGateLargeOutput` (2 cases), `docker.TestOutputIsBoundedAndVerified` | A 20,000,012-byte output with a 1 MiB per-job limit keeps exactly its first 1 MiB as the artifact, reports `kept 1048576 of 20000012 bytes` and an incomplete output, and previews head and tail within 4 KiB; the next prompt reports the truncation and stays under 64 KiB, and the next job reads the artifact through the read-only mount; a run output budget of 1 MiB stops the run as `budget_exhausted` with the observation kept; a deleted artifact is reported unavailable | Output is copied from Docker's log once the job finishes, not streamed: until then that log is bounded only by the job's run time. The run budget is checked between steps, so a run can exceed it by one per-job limit. Neither is a filesystem quota: workspace writes, a job's container layer outside the mounts, the journal, and the container and log of a finished job whose removal failed until a later report or `Bind` retries it are not limited, and a full disk is not injected; a final job record that cannot be written leaves the job to the inspect retries and then an unknown outcome. Docker's json-file log replaces invalid UTF-8, so artifacts of binary output are not byte-exact |
| Cancellation and timeout | Passed | `TestGateCancellationAndTimeout` (3 cases), `docker.TestCancelStopsProcessTree`, `TestASignalStopsTheRunPersistently`, `TestASignalDuringSetupStopsTheRunPersistently`, `TestASecondSignalAbandonsTheCleanup` | A 2 s job timeout on a two-process tree is observed as `timed_out`, and the job's record shows it ran 2 to 12 s; `stop` after the controller was killed commits `stop_requested` and then the job's cancelled result, which stops the run, within its cleanup deadline; SIGINT to `run` does the same and exits with status 3, and a signal during `run` or `resume` setup stops the run before any model call; `Cancel` of a process tree, including one that ignores SIGTERM, returns within the stop grace plus 5 s, the latter after at least the grace, and leaves the job cancelled; no container remains; a second signal abandons cleanup and a later `stop` finishes | Process-tree cleanup is Docker's container stop and kill, evidenced by the stop returning and the container being removed, not by listing processes; the job timeout, and a restarted controller's choice between `timed_out` and `cancelled`, rely on the daemon sharing the host clock; the cancel marker labels a job cancelled even if it exited on its own just before the marker was written or after a `docker stop` cut short by the deadline; a stop's cleanup deadline must also cover copying the job's Docker log into its artifact, so a job with a large output needs a longer `-cleanup`; timing assertions allow 5 s of slack |
| Finite runs | Partial | `TestGateFiniteRuns` (4 cases), `TestGateLargeOutput/disk_budget` | Step, attempt, wall-time, token, and output-byte budgets each stop a run with `steps_exhausted`, `attempts_exhausted`, `timed_out`, or `budget_exhausted`, with failure text on the stop for the measured budgets and on each failed attempt, keep earlier observations, and dispatch nothing after the budget is spent; a wall-time stop cancels the running job and records its result | Cost is not measured; tokens are provider-reported usage, so a run can exceed its token budget by one response; the wall-time stop during a model request is tested only with a fake model |
| Trust boundary | Passed | `TestGateTrustBoundaryKeepsCredentialsAndStateOut`, `TestGateTrustBoundaryNotes` (2 cases), `TestGateTrustBoundaryProtectsFacts`, `TestGateTrustBoundaryProtectsStepRecords`, `docker.TestTrustBoundary` | A job sees no host environment variable, credential, or injected proxy; runs as the host uid with no capabilities and only a loopback interface, and its running container has network `none`, `CapDrop` `ALL`, and the host uid and gid; cannot reach the model URL; sees the artifacts, and the step records at `/canti/runs`, read-only, and neither the state directory nor the journal; cannot delete or forge committed artifacts or history, and forgeries stay in the workspace. The credential appears in no request body, CLI output, or state file. Notes written by a job reach the next prompt marked unverified; a notes symlink out of the workspace is reported unreadable and its target never enters a request. A job reads its run's step records — a `meta` matching the committed observation, the whole command that the prompt cut, and the preview — and a later run's job reads the earlier run's goal, result, and a step's command; creating a file, a step directory, or a run directory, overwriting the goal or a preview, or removing a step's records under `/canti/runs` fails read-only and leaves the records as published. A configured extra mount is readable and not writable | Docker is the isolation boundary: a shared kernel and the daemon's own security. The test credential is fake. The workspace is writable by the agent by design. A non-default network lets jobs reach the network. Every job can read the step records of every run of its state directory |
| Complete loop | Passed | `TestRunDrivesExecAndReturn`, `TestRunOnceStepsAcrossProcesses`, and every gate above | The concrete adapter, core, SQLite journal, and Docker executor drive exec, observation, a second exec that reads the first one's file, and return in one isolated workspace, in one process or one process per step | Scripted provider |
| Live provider | Partial | `anthropic.TestLiveDecision` (`live` tag) and two manual `canti run` sessions ([samples](#live-provider-samples-2026-09-23)) | One real request with structured output decodes to one decision; two CLI runs drive real model decisions through the SQLite journal and Docker executor to `return`, one with structured output and one without, in which the first command failed with exit status 1 and the next decision corrected it | One model through a gateway, not Anthropic's API; manual runs, not a repeatable test; rate limits, refusals, fallbacks, and long runs are not exercised; three samples, recorded at `ee968c6` with adapter `canti-anthropic/1`, which the current code no longer sends; none with `canti-anthropic/2` |

The live test is opt-in and may be billed:

```sh
ANTHROPIC_API_KEY=... go test -count=1 -tags live -run TestLiveDecision -v ./internal/harness/anthropic
```

`ANTHROPIC_BASE_URL` and `ANTHROPIC_MODEL` select another Messages endpoint
and model; `CANTI_LIVE_STRUCTURED=0` sends the request without the decision
schema.

Neither a successful command nor a `return` decision proves the user's task
succeeded. Do not claim exactly-once arbitrary shell effects: job identity
deduplication does not cover effects inside a command or later semantic
repeats. SIGKILL tests do not establish durability across power loss or OS
crashes. The tests keep their files in the platform's temporary directory,
which in the recorded run was a tmpfs; the SQLite suite and the long-history
gate were repeated once on ext4.

## Evidence record

### Revision d93633e, 2026-09-23

Label: concrete backend integration with a scripted provider; the long-history
gate is an offline model over the concrete store.

| Item | Value |
| --- | --- |
| Revision | `d93633eaa2390da8cbcdea2100eeeb64771f1065`; the commit that adds this record changes only documentation |
| Platform | Linux 6.18.33.2-microsoft-standard-WSL2 x86_64 (WSL2), `GOMAXPROCS=16` |
| Go | go1.25.6 linux/amd64 |
| Docker | Engine 29.7.2-2 (API 1.55), containerd 2.3.4-2, runc 1.5.1-7, docker-init 0.19.0 |
| Test image | `alpine:3.23`, `sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11` |
| SQLite | 3.53.4 through `modernc.org/sqlite` v1.59.0 |
| Test files | `t.TempDir()` under `/tmp`, a tmpfs, on which fsync does not reach a disk; the ext4 runs used a directory on an ext4 volume of the WSL2 virtual disk |
| Environment | `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, and `ANTHROPIC_BASE_URL` unset; the fixtures use a random fake key |
| Concurrency | Go test defaults; the 16 crash cases run in parallel |

| Command | Result | Wall time |
| --- | --- | --- |
| `go clean -testcache; make check` | Passed: formatting, both vets, all unit tests | 2 s |
| `go test -count=1 -race ./...` | Passed, 7 packages | 11 s |
| `make integration` | Passed, 7 packages; `cmd/canti` 41.7 s, `internal/harness` 26.4 s, `docker` 17.9 s, `sqlite` 10.0 s | 43 s |
| `go test -count=1 -race -tags integration -v ./...` | Passed: 243 top-level tests and 557 subtests, none failed or skipped | 42 s |
| `docker ps -aq --filter label=canti.effect` before and after the runs | No container | |
| `go test -count=1 -race -tags integration -run TestLoopBoundsRequestsAsHistoryGrows -v ./internal/harness/` | Passed on tmpfs | 19 s |
| `go test -count=1 -tags integration -run TestLoopBoundsRequestsAsHistoryGrows -v ./internal/harness/` | Passed on tmpfs | 2 s |
| `TMPDIR=<ext4 directory> go test -count=1 -tags integration -run TestLoopBoundsRequestsAsHistoryGrows -v ./internal/harness/` | Passed on ext4 | 35 s |
| `TMPDIR=<ext4 directory> go test -count=1 -v ./internal/harness/sqlite/` | Passed on ext4 | 6 s |
| `git diff --check` | Clean | |

Measurements come from the verbose run unless a row names another run above:
the `evidence` lines of the gate tests, the log line of
`sqlite.TestCrashKeepsAcknowledgedCommits` for the SIGKILL row, and the
table logged by `harness.TestLoopBoundsRequestsAsHistoryGrows` for the
long-history rows.

| Case | Measured |
| --- | --- |
| SIGKILL of a committing child | 201 transitions recovered after 201 acknowledgements (tmpfs, race); 201 after 200 on ext4 |
| Cold restart | 15 processes, 18 transitions, 3 model requests |
| Crash at commit boundaries | 16 cases returned after `resume`: 18 transitions and 3 requests each, except 19 transitions after `after:attempt_started`, and 19 transitions with 4 requests after `before:response_saved` |
| Frozen request replay | Requests of 3,060 bytes with sha256 prefixes `8b3f25b5223897e9`, `d2c4edb4f62df8e4`, and `138df43cb848b96d`, one per case, each naming its case's run ID, each sent twice byte-identical; partial bodies of 75 (HTTP 529), 256 (`max_tokens`), and 125 (cut) bytes kept |
| Large output | 20,000,012 bytes written; artifact 1,048,576 bytes; preview 4,096 bytes; next request 7,536 bytes |
| Job timeout of 2 s | The job ran 2.22 s against a 12 s bound; exit code 143 |
| `stop` after SIGKILL of the controller | 166 ms against a 20 s cleanup deadline |
| SIGINT to `run` | 157 ms to exit against a 30 s cleanup deadline |
| Wall time of 5 s | The run took 5.59 s against a 15 s bound; the job ran 5.31 s |
| Token budget of 100 | Stopped at 110 tokens with the effect fenced before bind |
| Trust probe | 6,825 bytes of job output; uid 1000; artifacts and step records `ro`, workspace `rw`; interfaces `lo` only |
| Step records read by a job | A 3,030-byte command read whole; a 198-byte `meta`; the second run listed 2 runs under `/canti/runs` |
| Long history, budgets set before measuring | Request ≤ 81,920 bytes, growth ≤ 64 bytes between the two largest histories; ≤ 10 steps shown; ≤ 74 records in ≤ 1 page; ≤ 4 reads; ≤ 8 MiB and 1.5× allocation; ≤ 250 ms and 4× build time |
| Long history at 50, 500, 2,000 steps, in all four runs | Store 5.3, 21.9, and 78.5 MB; request 72,338, 72,348, and 72,358 bytes with prompts of 59,800, 59,810, and 59,820; 8 steps shown, against 9 at `552644d`; 50, 64, and 64 records in one page and 4 reads |
| Long history allocations per build | 3.70–4.33 MB in 3,514–4,353 allocations under the race detector (verbose and dedicated runs), 3.51–4.14 MB in 3,432–4,258 without it (tmpfs and ext4) |
| Long history median build at 50, 500, 2,000 steps | Under the race detector on tmpfs 26.8, 33.3, and 33.4 ms in the dedicated run and 27.4, 35.8, and 34.6 ms in the verbose run; without it 3.4, 3.7, and 4.0 ms on tmpfs and 3.7, 4.6, and 4.4 ms on ext4 |

Run, effect, and job IDs appear in the test log of this run, which is not
committed. Not run at this revision: `anthropic.TestLiveDecision` and any
`canti run` against a live provider. Unmet: live provider evidence for
`canti-anthropic/2`, a measured provider cache hit, the partial conditions
above, OS crash or power loss, a Docker daemon restart, and retention: the
gates' output and error artifacts are deleted with their temporary
directories, and no run, effect, or job identity is committed.

### Earlier record: revision ee968c6, 2026-09-23

Superseded by the [`d93633e` record](#revision-d93633e-2026-09-23); the
gates, the prompt, and request construction changed after it.

Label: concrete backend integration with a scripted provider; the long-history
gate is an offline model over the concrete store.

| Item | Value |
| --- | --- |
| Revision | `ee968c692ab7ee6f93d0aea2e3382c2f9f3378af`; the commits after it up to `53a4f11` change only documentation |
| Platform | Linux 6.18.33.2-microsoft-standard-WSL2 x86_64 (WSL2), `GOMAXPROCS=16` |
| Go | go1.25.6 linux/amd64 |
| Docker | Engine 29.7.2-2 (API 1.55), containerd 2.3.4-2, runc 1.5.1-7, docker-init 0.19.0 |
| Test image | `alpine:3.23`, `sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11` |
| SQLite | 3.53.4 through `modernc.org/sqlite` v1.59.0 |
| Test files | `t.TempDir()` under `/tmp`, a tmpfs, on which fsync does not reach a disk |
| Environment | `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, and `ANTHROPIC_BASE_URL` unset; the fixtures use a random fake key |
| Concurrency | Go test defaults; the 15 crash cases run in parallel |

| Command | Result | Wall time |
| --- | --- | --- |
| `go clean -testcache && make check` | Passed: formatting, both vets, all unit tests | 2 s |
| `go test -count=1 -race ./...` | Passed, 7 packages | 9 s |
| `make integration` | Passed, 7 packages; `cmd/canti` 37.3 s, `internal/harness` 18.9 s, `docker` 17.8 s, `sqlite` 9.4 s | 37 s |
| `go test -count=1 -race -tags integration -v ./...` | Passed: 223 top-level tests and 517 subtests, none failed or skipped | 38 s |
| `docker ps -aq --filter label=canti.effect` after the runs | No container | |
| `TMPDIR=<ext4 directory> go test -count=1 -v ./internal/harness/sqlite/` | Passed on ext4 | 2 s |
| `TMPDIR=<ext4 directory> go test -count=1 -tags integration -run TestLoopBoundsRequestsAsHistoryGrows -v ./internal/harness/` | Passed on ext4 | 21 s |
| `git diff --check` | Clean | |

Measurements from the verbose run: the `evidence` lines of the gate tests, the
log line of `sqlite.TestCrashKeepsAcknowledgedCommits` for the SIGKILL row,
and the table logged by `harness.TestLoopBoundsRequestsAsHistoryGrows` for the
long-history rows.

| Case | Measured |
| --- | --- |
| SIGKILL of a committing child | 201 transitions recovered after 201 acknowledgements (tmpfs, race); 201 after 200 on ext4 |
| Cold restart | 15 processes, 18 transitions, 3 model requests |
| Crash at commit boundaries | 15 cases returned after `resume`: 18 transitions and 3 requests each, except 19 transitions after `after:attempt_started`, and 19 transitions with 4 requests after `before:response_saved` |
| Frozen request replay | Request sha256 prefix `b9e9cc345f6fdb52`, 2,513 bytes, sent twice byte-identical in each case; partial bodies of 75 (HTTP 529), 256 (`max_tokens`), and 125 (cut) bytes kept |
| Long history, budgets set before measuring | Request ≤ 81,920 bytes, growth ≤ 64 bytes between the two largest histories, ≤ 8 reads, ≤ 4 MiB and 1.5× allocation, ≤ 250 ms and 4× build time |
| Long history at 50, 500, 2,000 steps | Store 5.3, 21.9, and 78.5 MB; request 71,517, 71,527, and 71,537 bytes; 8 steps shown and 4 reads each; 1.62 MB in 911–945 allocations per build under the race detector, 1.51–1.53 MB in 872–898 without it; median build 6.4, 8.1, and 6.5 ms under the race detector on tmpfs, 1.02, 1.07, and 0.66 ms without it on ext4 |
| Large output | 20,000,012 bytes written; artifact 1,048,576 bytes; preview 4,096 bytes; next request 6,926 bytes |
| Job timeout of 2 s | The job ran 2.21 s against a 12 s bound; exit code 143 |
| `stop` after SIGKILL of the controller | 137 ms against a 20 s cleanup deadline |
| SIGINT to `run` | 150 ms to exit against a 30 s cleanup deadline |
| Wall time of 5 s | The run took 5.59 s against a 15 s bound; the job ran 5.30 s |
| Token budget of 100 | Stopped at 110 tokens with the effect fenced before bind |
| Trust probe | 6,695 bytes of job output; uid 1000; artifacts `ro`, workspace `rw`; interfaces `lo` only |

Run, effect, and job IDs appear in the test log of this run, which is not
committed. Unmet: live provider evidence beyond the samples below, the partial
conditions above, OS
crash or power loss, a Docker daemon restart, and retention: the gates' output
and error artifacts are deleted with their temporary directories, and no
run, effect, or job identity is committed.

### Earlier record: long history at 552644d, 2026-09-23

Superseded by the [`d93633e` record](#revision-d93633e-2026-09-23); request
construction changed after it.

Offline model over the concrete store, measured at the commit that bounds the
shown history by the prompt's byte capacity instead of a step count (parent
`767c7fc`), on the platform, Go, and SQLite recorded above, with the
Anthropic variables unset. The long-history rows of the `ee968c6` record
measured the earlier test, which showed at most 8 steps.

| Command | Result | Wall time |
| --- | --- | --- |
| `make check`, `go test -count=1 -race ./...`, `make integration` | Passed, 7 packages each | 3 s, 11 s, 38 s |
| `go test -count=1 -race -tags integration -run TestLoopBoundsRequestsAsHistoryGrows -v ./internal/harness/` | Passed on tmpfs | 17 s |
| `go test -count=1 -tags integration -run TestLoopBoundsRequestsAsHistoryGrows -v ./internal/harness/` | Passed on tmpfs | 2 s |
| `TMPDIR=<ext4 directory> go test -count=1 -tags integration -run TestLoopBoundsRequestsAsHistoryGrows -v ./internal/harness/` | Passed on ext4 | 24 s |

| Case | Measured |
| --- | --- |
| Budgets from the 64 KiB total | Request ≤ 81,920 bytes, growth ≤ 64 bytes between the two largest histories; ≤ 10 steps shown; ≤ 74 records in ≤ 1 page; ≤ 4 reads; ≤ 8 MiB and 1.5× allocation; ≤ 250 ms and 4× build time |
| At 50, 500, 2,000 steps | Store 5.3, 21.9, and 78.5 MB; request 79,167, 79,178, and 79,189 bytes with prompts of 65,487, 65,498, and 65,509; 9 steps shown; 50, 64, and 64 records in one page and 4 reads; 4.13–4.76 MB in 3,487–4,338 allocations per build under the race detector, 4.01–4.64 MB in 3,410–4,238 without it; median build 28.3, 34.0, and 33.6 ms under the race detector on tmpfs, 3.4, 3.8, and 4.0 ms without it on tmpfs, and 4.1, 4.0, and 4.7 ms on ext4 |

### Live provider samples, 2026-09-23

Live provider evidence, run by hand from code revision `ee968c6` with the
binary built from `53a4f11` (documentation changes only since `ee968c6`),
the `live` test at `ee968c6` plus a change that reads `ANTHROPIC_BASE_URL`,
on the platform above. Model `grok-4.7` through an Anthropic
Messages-compatible gateway, protocol `anthropic-messages/2023-06-01`, adapter
`canti-anthropic/1`, image `alpine:3.23`, `-fallbacks=false`,
`-max-tokens 4096`, `-max-attempts 3`, `-job-timeout 1m`. The credential was
passed in `ANTHROPIC_API_KEY` only; afterwards no file in the state
directories, workspaces, or captured output contained it, and no container of
either run remained. Usage is provider-reported; cost was not measured.

| Sample | Budgets | Result | Usage (input / output tokens) | Time |
| --- | --- | --- | --- | --- |
| `TestLiveDecision`, structured output | 1,024 output tokens | `{"kind":"return","payload":"ok"}` | 3,368 / 316 | 10.8 s |
| `canti run`, structured output: write `hello.txt`, verify it, return it | 6 steps, 5 min, 200,000 tokens | Exit status 0, stdout `canti live check`; one exec (`printf ... > /workspace/hello.txt && cat ...`, exit 0) then `return`; 11 transitions, one attempt per step; the file holds the line | 7,799 / 466 | 10 s |
| `canti run`, `-structured=false`: write, run, and note `count.sh`, return its output | 8 steps, 6 min, 300,000 tokens | Exit status 0, stdout `17`; step 1 failed with exit status 1 (`can't create /workspace/.canti/notes.md`), step 2 added `mkdir -p` and printed `17`, step 3 returned; 18 transitions, one attempt per step; `17` equals the line count of `/etc/passwd` in the image, and the notes file holds it | 12,093 / 542 | 11 s |

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

The V1 implementation order is complete: the SQLite journal with recovery
reads, the Docker executor passing `executortest.Check`, the Anthropic adapter
with the bounded input builder, and the loop and CLI exercised by the gates
above.

Beyond V1:

1. Record live provider evidence for `canti-anthropic/2`, including whether a
   provider reuses the marked prompt prefix; extend it to Anthropic's API and
   repeatable runs under finite budgets, and exercise provider behavior the
   scripted server cannot: rate limits, refusals, fallbacks, and long runs.
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
