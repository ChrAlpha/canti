# V1 acceptance

V1 combines a stateless core with a durable, bounded single-machine harness:
a SQLite journal, a Docker executor, an Anthropic Messages adapter, and a CLI.
Every integration gate below has concrete-backend evidence except where its
status says otherwise. The model in every end-to-end test is a scripted local
HTTP server speaking the Messages format: the adapter's HTTP path is
exercised, a provider's behavior is not. No live provider run has been
recorded.

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
| Anthropic adapter | Against a local HTTP server: frozen body sent verbatim with the right headers; status, stop-reason, truncation, size, timeout, redirect, and context handling; decision extraction | No live provider |
| Docker executor | Against a fake docker CLI: missing versus lost jobs, no create once the context is done, no restart of an unlatched or failed start, a rejected start as `failed_to_start`, launch outliving shutdown, per-call deadlines, and a loop that gives up on a hung daemon with an unknown outcome; a bind after the fence creating a cancelled job that never starts; a record published before its container is released, release retries, failed and interrupted captures, and strict records | Fake CLI |
| SQLite journal | On real database files: durable pragmas, schema refusal, concurrent first opens, sequence compare-and-set, identical-retry confirmation without WAL growth, conflicting content never overwritten, broken chains, rollback of an injected failure after the insert, strict decoding refusing unknown kinds, fields, and semantics, one winner per sequence across two stores, and bounded reads | Covered in the gates below as well |
| CLI | Usage and exit codes, credential removal, refusal without a credential, missing runs, locked workspaces, state inside the workspace, open-run refusal, crash-and-resume around `response_saved`, a signal during setup, first and second signals, resolution and stops of unknown outcomes without Docker, paged history, status of one commit, and workspace binding, mostly in subprocesses | Without Docker, a bind fails and becomes an unknown outcome |

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

*Passed* means every listed test passed at the revision in the
[evidence record](#evidence-record), against real SQLite files and, where jobs
run, the Docker daemon recorded there, with the scripted provider. *Partial*
means part of the required evidence is missing; the Limits column names it.
Results apply only to that revision and environment.

| Gate | Status | Tests | What they assert | Limits |
| --- | --- | --- | --- | --- |
| Transactional facts | Passed | `sqlite.TestCommitRollsBackFailureAfterInsert`, `TestCrashKeepsAcknowledgedCommits`, `TestCommitPersistsTransitionsAndTotalsAcrossReopen`, `TestConcurrentStoresCommitOneTransitionPerSeq` | A transition, its facts, and the run totals are one transaction: decision plus effect or return is one `response_loaded` row, observation plus progress one `observed` row. A failure injected after the insert leaves no row and unchanged totals; a child killed with SIGKILL after at least 200 acknowledged commits loses none, keeps at most the one in flight, and replays to the stored state and totals | Failure injected at one point, the totals update; SIGKILL, not power loss |
| Cold restart after every step | Passed | `TestGateColdRestartAfterEveryStep`, `TestRunOnceStepsAcrossProcesses`, `harness.TestLoopColdRestartAfterEveryStepMatchesAnUninterruptedRun` | A new CLI process per intent — 15 processes for exec, exec, return — each commits exactly its intent's transitions from the journal alone; the kinds, phases, and states of the history equal an uninterrupted run's except for IDs, while the in-process harness test compares whole transitions, facts included; the model receives exactly the frozen bytes, identical to the uninterrupted run's except for effect IDs; each command ran once | Processes exit normally; crashes are the next gate |
| Crash at commit boundaries | Passed | `TestGateCrashAtCommitBoundaries` (15 cases), `TestACrashAtACommitResumesFromTheJournal`, `docker.TestExecutorPassesStartLatchCheck`, `docker.TestBindReleasesAnEarlierJobWhoseRemovalFailed` | SIGKILL immediately before and after the first commit of each kind of an exec step (`request_frozen`, `attempt_started`, `response_saved`, `response_loaded`, `job_bound`, `job_started`, `observed`), and inside the `docker rm` that releases a recorded job; `resume` then returns. Confirmed transitions are unchanged; the history matches the expected sequence exactly, with an attempt interrupted after its commit counted as a second `attempt_started`; a request is sent again, byte-identical, only when the killed process had sent it; each step's command ran exactly once; and a job recorded but not yet released is released without running again. Against the daemon, `executortest.Check` drives every call through a new executor, as a restarted or competing controller would: a bind never runs its command, a start runs it at most once even when concurrent, a changed command is refused, and a fence stops a later bind or start; and a removal that failed is retried by the next `Bind` of another effect | The commit hook fires only on the first commit of each kind, so every commit crash is in step 1; `executortest.Check` injects no crash and sees a start racing a cancel only when the calls overlap; `response_incomplete`, `outcome_unknown`, `stop_requested`, and `resolved` are not crashed; SIGKILL, not power loss |
| In-flight job recovery | Passed | `TestGateInFlightJobRecovery`, `docker.TestRestartedExecutorCollectsOriginalJob` | The controller is killed after `job_started` while its container runs; `resume` finds the same job, collects its original output, commits one observation, binds no second job, sends no new request for the step, and the command ran once | Controller restart only; Docker daemon restarts and host reboots are not tested |
| Unknown outcomes | Passed | `TestGateUnknownOutcomeOfALostJob`, `TestGateUnknownOutcomeOfAnUnavailableStatus`, `TestAnUnknownOutcomeWaitsForAnExplicitResolution`, `docker.TestRejectedStartIsRecordedAndReleased` | A started job's container removed with `docker rm -f` before any record commits `outcome_unknown` with `ErrJobLost`, and no record or container appears afterwards; `container inspect` failing six times in a row commits `outcome_unknown` while the job keeps running. For the lost job, a second `resume` and a failing `resolve -job` change nothing and call no model; for the running one, `resolve -job` resumes inspection, which then observes its original result; `resolve -execution` records an operator outcome that the next prompt marks as set by explicit resolution. A start the daemon rejects (an unknown user) is reported `failed_to_start`, recorded, and released instead of unknown, and nothing ran | Two injected causes against the daemon; a hung daemon only against a fake CLI; an operator resolution is trusted as given, and `resolve -execution` records the outcome only, leaving a job that may still run, or a created container that never started, in place |
| Single workspace writer | Passed | `TestGateSingleWorkspaceWriterAndTakeover`, `TestWritersRefuseALockedWorkspace` | While a run's job is running, concurrent `resume` and `stop` exit with status 6 and change no history, container, or model request; after SIGKILL of the writer, `resume` takes over from `awaiting_observation`, observes the same job before any new decision, and the command ran once | The lock is a `flock` on a pathname in the state directory. A same-user process that replaces the lock file is not excluded; the sequence check still rejects its stale commits, but not its executor actions. A second state directory for the same workspace has its own journal and lock, so nothing detects the conflict and both runs can write the workspace |
| Frozen request replay | Passed | `TestGateFrozenRequestReplay` (3 cases), `TestGateFiniteRuns/attempts` | The first attempt answers with HTTP 529, with `stop_reason` `max_tokens` around a complete decision, or with a body cut mid-stream. `response_incomplete` records the failure and the partial body; the second attempt sends byte-identical bytes as a distinct attempt; the frozen model, protocol, and adapter are recorded; the truncated decision never runs; usage is counted for every response that reports it | Bytes are captured at the scripted server; a provider's retry behavior is not exercised |
| Long history | Passed | `harness.TestLoopBoundsRequestsAsHistoryGrows`, `sqlite.TestReadsStayBoundedByTheirLimit`, `harness.TestPromptBoundsLongHistory` | With budgets chosen before measuring, a restarted controller builds the next request through `Loop.Step` on a newly opened store at 50, 500, and 2,000 stored steps: request and prompt size, reads, records read, allocations, and build time stay within fixed ceilings and ratios while stored history grows to 12 MB of commands and previews; the oldest facts still read back; recovery queries use their indexes without a sort | Offline model: the history is seeded through `Runner.Commit` and requests are frozen by the real adapter but never sent; bytes, not tokens; allocations, not resident memory; three sizes of nine repetitions on one machine |
| Large output | Partial | `TestGateLargeOutput` (2 cases), `docker.TestOutputIsBoundedAndVerified` | A 20,000,012-byte output with a 1 MiB per-job limit keeps exactly its first 1 MiB as the artifact, reports `kept 1048576 of 20000012 bytes` and an incomplete output, and previews head and tail within 4 KiB; the next prompt reports the truncation and stays under 64 KiB, and the next job reads the artifact through the read-only mount; a run output budget of 1 MiB stops the run as `budget_exhausted` with the observation kept; a deleted artifact is reported unavailable | Output is copied from Docker's log once the job finishes, not streamed: until then that log is bounded only by the job's run time. The run budget is checked between steps, so a run can exceed it by one per-job limit. Neither is a filesystem quota: workspace writes, a job's container layer outside the mounts, the journal, and the container and log of a finished job whose removal failed until a later report or `Bind` retries it are not limited, and a full disk is not injected; a final job record that cannot be written leaves the job to the inspect retries and then an unknown outcome. Docker's json-file log replaces invalid UTF-8, so artifacts of binary output are not byte-exact |
| Cancellation and timeout | Passed | `TestGateCancellationAndTimeout` (3 cases), `docker.TestCancelStopsProcessTree`, `TestASignalStopsTheRunPersistently`, `TestASignalDuringSetupStopsTheRunPersistently`, `TestASecondSignalAbandonsTheCleanup` | A 2 s job timeout on a two-process tree is observed as `timed_out`, and the job's record shows it ran 2 to 12 s; `stop` after the controller was killed commits `stop_requested` and then the job's cancelled result, which stops the run, within its cleanup deadline; SIGINT to `run` does the same and exits with status 3, and a signal during `run` or `resume` setup stops the run before any model call; `Cancel` of a process tree, including one that ignores SIGTERM, returns within the stop grace plus 5 s, the latter after at least the grace, and leaves the job cancelled; no container remains; a second signal abandons cleanup and a later `stop` finishes | Process-tree cleanup is Docker's container stop and kill, evidenced by the stop returning and the container being removed, not by listing processes; the job timeout, and a restarted controller's choice between `timed_out` and `cancelled`, rely on the daemon sharing the host clock; the cancel marker labels a job cancelled even if it exited on its own just before the marker was written or after a `docker stop` cut short by the deadline; a stop's cleanup deadline must also cover copying the job's Docker log into its artifact, so a job with a large output needs a longer `-cleanup`; timing assertions allow 5 s of slack |
| Finite runs | Partial | `TestGateFiniteRuns` (4 cases), `TestGateLargeOutput/disk_budget` | Step, attempt, wall-time, token, and output-byte budgets each stop a run with `steps_exhausted`, `attempts_exhausted`, `timed_out`, or `budget_exhausted`, with failure text on the stop for the measured budgets and on each failed attempt, keep earlier observations, and dispatch nothing after the budget is spent; a wall-time stop cancels the running job and records its result | Cost is not measured; tokens are provider-reported usage, so a run can exceed its token budget by one response; the wall-time stop during a model request is tested only with a fake model |
| Trust boundary | Passed | `TestGateTrustBoundaryKeepsCredentialsAndStateOut`, `TestGateTrustBoundaryNotes` (2 cases), `TestGateTrustBoundaryProtectsFacts`, `docker.TestTrustBoundary` | A job sees no host environment variable, credential, or injected proxy; runs as the host uid with no capabilities and only a loopback interface, and its running container has network `none`, `CapDrop` `ALL`, and the host uid and gid; cannot reach the model URL; sees the artifacts read-only and neither the state directory nor the journal; cannot delete or forge committed artifacts or history, and forgeries stay in the workspace. The credential appears in no request body, CLI output, or state file. Notes written by a job reach the next prompt marked unverified; a notes symlink out of the workspace is reported unreadable and its target never enters a request | Docker is the isolation boundary: a shared kernel and the daemon's own security. The test credential is fake. The workspace is writable by the agent by design. A non-default network lets jobs reach the network |
| Complete loop | Passed | `TestRunDrivesExecAndReturn`, `TestRunOnceStepsAcrossProcesses`, and every gate above | The concrete adapter, core, SQLite journal, and Docker executor drive exec, observation, a second exec that reads the first one's file, and return in one isolated workspace, in one process or one process per step | Scripted provider |
| Live provider | Pending | `anthropic.TestLiveDecision` (`live` tag) | One real request decodes to one decision | Not run; no live loop exists as a test |

The live test is opt-in and may be billed:

```sh
ANTHROPIC_API_KEY=... go test -count=1 -tags live -run TestLiveDecision -v ./internal/harness/anthropic
```

Neither a successful command nor a `return` decision proves the user's task
succeeded. Do not claim exactly-once arbitrary shell effects: job identity
deduplication does not cover effects inside a command or later semantic
repeats. SIGKILL tests do not establish durability across power loss or OS
crashes. The tests keep their files in the platform's temporary directory,
which in the recorded run was a tmpfs; the SQLite suite and the long-history
gate were repeated once on ext4.

## Evidence record

### Revision ee968c6, 2026-09-23

Label: concrete backend integration with a scripted provider; the long-history
gate is an offline model over the concrete store.

| Item | Value |
| --- | --- |
| Revision | `ee968c692ab7ee6f93d0aea2e3382c2f9f3378af`; later commits change only documentation |
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
committed. Unmet: the live provider gate, the partial conditions above, OS
crash or power loss, a Docker daemon restart, and retention: the gates' output
and error artifacts are deleted with their temporary directories, and no
run, effect, or job identity is committed.

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

1. Record a live provider run under finite budgets, and exercise provider
   behavior the scripted server cannot: rate limits, refusals, fallbacks, and
   structured output.
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
