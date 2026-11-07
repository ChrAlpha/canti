# Development

## Prerequisites

- Go 1.25 or newer; `go.mod` declares the minimum version.
- Make and a POSIX shell for the check targets; Bash and `jq` for the `ci-`
  targets.
- A C compiler and a supported platform for Go's race detector.
- A Unix-like OS: the CLI and executor use `flock` and process groups. Only
  Linux has been verified.
- For `make integration` and for running agents, a Docker daemon the `docker`
  CLI can reach. The integration tests run the image `alpine:3.23`;
  `CANTI_TEST_IMAGE` selects another image, which must provide `/bin/sh`, a
  `/tmp` the test uid can write, and the common shell tools the tests call,
  including `wget`.

The module depends on `modernc.org/sqlite`, a cgo-free SQLite, pinned by the
committed `go.sum`. `make check` needs no credentials, `.env` file, database
server, container runtime, or Memoh service. Run commands in this repository,
not its parent workspace. If a parent `go.work` includes other modules, use
`GOWORK=off`.

```sh
git clone https://github.com/ChrAlpha/canti.git
cd canti
make check
```

Cloning requires repository access while the repository is private.

## Commands

| Command | Purpose |
| --- | --- |
| `make check` | Read-only formatting validation, vet, the test-name check, and all unit tests |
| `make fmt` | Check `gofmt` output; does not rewrite files |
| `make vet` | `go vet ./...`, then `go vet -tags integration,live ./...` so tagged test files compile |
| `make test` | `go test ./...` |
| `make integration` | `go test -count=1 -race -tags integration ./...`: the unit tests plus the Docker-backed tests and the long-history measurement |
| `make ci-unit` | CI's race-enabled unit tests, without the `PTY_TESTS` of the Makefile |
| `make ci-integration` | CI's `-tags integration` run without the `LONG_TESTS` and `PTY_TESTS` |
| `make ci-long` | The slow acceptance tests listed in `LONG_TESTS`; needs Docker |
| `make ci-pty` | The tests listed in `PTY_TESTS`, which open a pseudo-terminal, alone in one test binary; needs Docker |
| `make test-names` | Check the test names of `LONG_TESTS`, `PTY_TESTS`, and the docs; needs Linux, where every test file builds |
| `make lint` | The cognitive complexity limit of `.golangci.yml`, through a pinned golangci-lint fetched by `go run` |
| `make commits BASE=main` | Build and vet, without and with the tags, every commit after `BASE` in a temporary worktree; `scripts/check-commits.sh BASE` takes a base whose name contains `$` |
| `go test -count=1 -race ./...` | Fresh race-enabled unit test run |
| `go test -count=1 -race -tags integration -run 'TestGate' -v ./cmd/canti` | The CLI gate tests with their `evidence` log lines; `make integration` runs every gate test |
| `go test ./core -run TestDecodeRejectsInvalidProtocol -v` | Focused protocol regression cases |
| `go test ./core -run 'TestNext\|TestIntent\|TestValidate\|TestTrajectory\|TestIllegal' -v` | Transition table, budgets, invariants, trajectories, and rejected events |
| `go test ./internal/harness -run 'TestCommit\|TestBind\|TestStart\|TestRunner\|TestStale\|TestOffline' -v` | Commit protocol, bind/start, failure, and cancellation cases |
| `go test ./internal/harness/sqlite -run TestCrashKeepsAcknowledgedCommits -v` | SIGKILL of a committing process and recovery of every acknowledged commit |
| `git diff --check` | Check pending patches for whitespace errors |

Use `gofmt -w path/to/changed.go` on changed files to resolve a formatting
failure. Do not run a repository-wide rewrite as part of an unrelated change.

Build tags keep slow and paid tests out of the default run. `integration`
marks tests that need Docker or take tens of seconds; CI runs them in
separate jobs. `live` marks the `live_test.go` of each model adapter, which
sends one real request and is never part of `make check`, `make integration`,
or CI:

```sh
ANTHROPIC_API_KEY=... ANTHROPIC_MODEL=... go test -count=1 -tags live -run TestLiveDecision -v ./internal/harness/anthropic
OPENAI_API_KEY=... OPENAI_MODEL=... go test -count=1 -tags live -run TestLiveDecision -v ./internal/harness/openai
```

Each skips without its credential and model, as canti has no default model.
`ANTHROPIC_BASE_URL` or `OPENAI_BASE_URL` selects another provider of the
API, `CANTI_LIVE_EFFORT` an effort level, `CANTI_LIVE_DECISION_FORMAT=tool`
the tool transport, and `CANTI_LIVE_STRUCTURED=0` omits the decision schema.
A request is capped at 1,024 output tokens and may be billed. It checks one
decision, not a loop.

## CLI

```sh
go build -o bin/canti ./cmd/canti
./bin/canti -version
./bin/canti -h
./bin/canti run -h
```

`-version` (or `version`) writes `dev` to standard output and exits with
status 0. `-h` writes usage to standard output and exits with status 0.
Invoking the binary with no arguments writes usage to standard error and exits
with status 2. `go run` itself reports a non-zero wrapper status for a failing
program, so use the binary to inspect its exact exit code.

| Exit status | Meaning |
| --- | --- |
| 0 | The run returned, or the command succeeded |
| 1 | Error |
| 2 | Usage error |
| 3 | The run stopped; standard error names the reason |
| 4 | The run needs resolution |
| 5 | The run is still active after `-once` |
| 6 | Another canti process holds the workspace lock |

Outside the TUI, a blocked standard error does not delay a run or its cleanup:
diagnostics and progress lines that overflow a bounded queue are dropped and
counted in a later notice, while a flag error with its usage, a command's
outcome and recovery advice, and its final error line are written with the
workspace unlocked, each write waiting at most 1 s.

A run names its model API with `-api`, `anthropic-messages` or
`openai-responses`, and its model with `-model`; neither has a default, and
`-api openai-completions` is refused. `run`, `chat`, and `resume` read the
credential of the run's API, `ANTHROPIC_API_KEY` or else
`ANTHROPIC_AUTH_TOKEN` for `anthropic-messages` and `OPENAI_API_KEY` for
`openai-responses`, and the API base URL from `-model-url`, or else
`ANTHROPIC_BASE_URL` or `OPENAI_BASE_URL`, or else the API's public endpoint;
neither is saved with the run, and the URL must use `https` unless it is a
loopback address. Every command removes all three credential variables from
its environment before it starts a process. `-effort` takes a level of the
run's API and is sent only when set; `-fallbacks` requires
`anthropic-messages`. The goal, constraints, and the image, limit, model,
prompt, and budget flags of
`run` are stored with the run and reused by every later command; `-once`,
`-model-url`, `-state`, and `-log-level` apply to one invocation, and
`-workspace` must name the run's workspace. Defaults: image
`debian:bookworm-slim`, network `none`, memory `2g`, no CPU limit, 512
processes, 8 MiB retained output per job, 16,000 output tokens per
response, no effort, structured output on, fallbacks off, 256 KiB per
prompt (`-context-bytes`, including handover reserve; it must hold the goal,
constraints, and the reserves for one more step and a failed handover, about
68 KiB with a short goal and the default handover; `run` refuses a smaller
value with the total it needs), 20 steps, 3 attempts per step, 30 min wall
time, no token budget, 256 MiB of output per run, and 10 min per job.

`chat` takes the flags of `run` except `-goal`, `-input`, and `-once`. Each
accepted input becomes the next run of one serial session. `-session ID`
continues a session; each new run uses the current launch flags. The log level
defaults to `warn`.

**Terminal input (TTY):** `chat` uses a full-screen TUI on the alternate screen,
rendered to standard error. There is currently no plain interactive terminal
mode. Enter submits, Alt+Enter inserts a newline, and PgUp/PgDn scroll the
retained display, including while a run is running or stopping. Text, paste,
and Enter received during a run are discarded; they cannot queue another run.
An incomplete input sequence or paste is drained before editing reopens.
Ctrl+C stops the current run and waits for cleanup before returning to the
editor; a second Ctrl+C abandons that cleanup wait, leaving recovery to
`canti resume` or `canti stop`. Ctrl+C while idle exits.

The TUI combines answers, diagnostics, and committed step output in one
viewport. Its pending output buffer and transcript each retain at most 64 KiB
of text, with omission notices when older text is dropped. Only text from this
process invocation is displayed: reopening with `-session ID` does **not** load
previous conversation or execution history. On exit, the retained transcript
is printed to standard error. The alternate screen and this bounded tail do
not preserve a complete native terminal or tmux scrollback. Use `canti history`
for a run's full recorded transitions. Display history is separate from the
session context supplied to the model; the next input still receives earlier
dialogue as far as it fits `-context-bytes`.

**Pipe or file input (non-TTY):** inputs are read one line at a time, or as a
block after `<<WORD` or `<<'WORD'`, ending at a line exactly equal to `WORD`.
`WORD` consists of ASCII letters, digits, underscores, or hyphens. `<<-WORD`
also strips leading tabs. An input that starts with `<<` must be sent in a
block. These block markers are not special in the TUI editor. Piped inputs are
processed sequentially; the TTY busy-input discard rule does not apply.
Answers go to standard output; prompts, commands, output previews, execution
status, and diagnostics go to standard error. End of input exits with status
0, or 1 inside an unfinished block, which is not submitted.

Both modes display command previews of up to 8 lines of 200 bytes each, output
tails, and execution status. Lines containing control characters are quoted
as Go strings after `(quoted)`; answers and output are cleaned of control
characters. A stopped run reports its stop reason and recorded steps. SIGTERM,
or Ctrl+C with non-TTY input, stops the run and ends chat with the run's status:
3 if stopped, 0 if it had already returned. A TUI input or output failure,
including a terminal hangup (SIGHUP), requests a stop and cleanup, then exits
with an error; abrupt process loss may still require `canti resume` or
`canti stop`.

Chat never changes the file status flags of the descriptors it inherits. It
writes to a pipe or terminal through its own reopened description, so that a
display write waits at most 250 ms, and the TUI reads its terminal the same
way. When that description cannot be opened, as for a pipe or terminal of
another user, without `/proc`, or for a pseudo-terminal master, chat says so
once on standard error for an output and writes to the inherited descriptor,
which can then block it, and the TUI reads the inherited terminal after a
poll; a hung-up terminal output still ends chat with an error before it
touches any state. The TUI must be the only reader of its terminal: another
process reading it can take or delay input, including input typed during a
run that chat would discard.

Empty input is skipped. The TUI editor caps inserted text at its character
limit; bracketed paste exceeding the goal's byte limit is rejected. Submitted
input that fails validation (length, UTF-8, or `-context-bytes`) is reported and
editing continues. A malformed block opener in non-TTY mode is also reported
without submitting it. A refused input, a run that needs resolution, or
constraints and flags that leave no room for any input end chat with the status
and recovery advice `run` would print.

The state directory defaults to
`$XDG_STATE_HOME/canti/<first 16 hex digits of the SHA-256 of the workspace's
resolved path>`, with `~/.local/state` when `XDG_STATE_HOME` is unset or
relative; `-state` overrides it, and it must not overlap the workspace or
resolve to a path with a colon, which Docker cannot mount. It
holds `canti.db` with its WAL files, the `lock` file, per-effect latches and
final job records under `jobs/`, output artifacts under `artifacts/`, and the
read-only step records of every run under `history/<run ID>/`, which jobs see
at `/canti/runs/<run ID>/`.
Each run is bound to the workspace it was created in. Handover windows are
always enabled; `-window-steps` and `-window-bytes` optionally request an
earlier handover, while zero leaves the automatic capacity trigger active.
Each handover drops what the window read, and the next window rereads it: in
one live coding task with one model, `-window-steps 12` runs used 11 to 34
windows, and with failing cases fed back they took three to six times the steps
of capacity-triggered runs, so keep zero for long work.
`-handover-bytes` bounds the continuation text (2048 bytes by default).
History retrieval is always available; `-history-imports` adds a read-only
archive. Workspace notes are ordinary files and are not automatically read.

Each run stores the context version of the canti that created it, which names
the rules that render and account for its prompts. `resume` refuses an open
run of another context version, or of none, with status 1 before it calls the
model or commits anything, and reports a halted one as it is. Such a run stays
readable through `status`, `history`, and `context`; `stop` closes it, after
`resolve` if it needs resolution, and the state directory then takes new runs,
unless the run ended with an outcome resolved as unknown, which keeps blocking
them. A later run of its session still quotes its dialogue. A run whose stored
configuration this canti cannot decode is refused by every command and
requires a fresh state directory.

A CLI check without Docker or credentials proves only that the command builds
and parses its arguments; the integration tests drive real runs with a
scripted model.

## Where to work

Read [architecture](architecture.md) for interfaces and ownership. Read
[V1 acceptance](v1-acceptance.md) before changing a concrete adapter or a
gate. Keep tests beside the package they verify: `core/*_test.go`,
`internal/harness/*_test.go` (the loop tests use a fake model and executor
over a real SQLite journal), `internal/harness/sqlite`, `internal/harness/docker`
(a fake daemon for unit tests, the daemon for `integration_test.go`),
`internal/harness/anthropic` and `internal/harness/openai` (a local HTTP
server), and `cmd/canti`, whose tests run the test binary as the CLI in
subprocesses against a scripted server of both model APIs. The gate tests are `cmd/canti/gate_*_test.go`.

Protocol and transition tests should stay deterministic and require no
credentials. A new run rule belongs in `core.State.Next` with a row in the
transition table test; the illegal-pair sweep must keep rejecting everything
else. Backend tests use temporary, isolated workspaces and state directories
and remove their containers; the CLI gate fixtures also fail on a container
left running. Live model checks must be explicit opt-ins with finite budgets;
do not make paid provider calls part of default tests.

`TestLoopScenariosKeepTheirCommittedRecords` compares what its scenarios commit
with `internal/harness/testdata/frozen-requests/<scenario>`: the exact body of
every frozen model request and, in `journal.txt`, the event ID and facts of
every transition. It also fails on a directory
that no scenario records. Only for an intended prompt, adapter, or record
change, rewrite them with
`go test ./internal/harness -run TestLoopScenariosKeepTheirCommittedRecords -update-requests`
and review the diff. `TestContextVersionPinsItsRules` digests these goldens
into the rules of the current context version, which
`cmd/canti/testdata/context-v<version>.txt` records once: rewriting the
goldens of a scenario, or adding one, which may observe a changed rule,
requires a new context version, recorded with
`go test ./cmd/canti -run TestContextVersionPinsItsRules -record-context`.

`TestLegacyJournals` reads every journal under
`internal/harness/testdata/legacy-journals/<producer SHA>/<case>`, which an
unmodified historical `canti` binary wrote and whose `manifest.json` records
how, read-only and after the migration a writable open performs, and
requires every recorded row to stay as it was; a writable open adds only the
index of performed steps, and to a schema-1 journal the session table.
`TestLegacyRunsAreClosedAndFollowedByNewRuns` recreates each one at its
recorded paths and requires that its open run, of another context version,
is refused by `canti resume`, which with `status`, `history`, and `context`
leaves every row and schema object but those as it was, closed by
`canti stop`, and followed by a new run. A case is never rewritten. The generator that wrote the corpus,
`internal/harness/legacy_journal_gen_test.go` and
`legacy_journal_export_test.go` behind the `legacygen` build tag, was
removed once no test continued these runs; restore it from the history of
`main` to add a case.

When adding dependencies, use Go modules and commit the resulting `go.sum`.
Record the minimum toolchain or platform change and update CI in the same unit.
Keep large outputs, credentials, databases, and local task state outside Git.

## CI and delivery

The CI workflow runs on pushes to `main` and pull requests in parallel jobs.
`check` runs `make fmt vet`, `make test-names`, `make lint`, `make ci-unit`,
and a CLI version smoke test. `make lint` runs golangci-lint's `gocognit` at
a cognitive complexity of 30 over the production code, without and with the
`integration,live` tags; `.golangci.yml` lists the only exceptions, core's
`State.Next` and `State.Validate` and the chat input decoder. For a pull
request, `commits` runs `scripts/check-commits.sh` with the base branch,
which builds and vets every commit after it in a temporary worktree.
`integration`, `long`, and `pty` each pull
`alpine:3.23`, run `make ci-integration`, `make ci-long`, or `make ci-pty`,
and fail if any container labelled `canti.effect` is left; together they run
each test of `make integration` once. The Makefile lists the tests of the last
two jobs: `LONG_TESTS` are the slow acceptance tests, and `PTY_TESTS` are the
tests that open a pseudo-terminal, which wait for terminal output within fixed
deadlines; `ci-pty` runs them alone so that no other test binary competes for
the CPU. Add a new test of either kind to its list. `make test-names` fails
when a listed name is not a test, when a test that opens a pseudo-terminal is
not in `PTY_TESTS`, or when `README.md`, `AGENTS.md`, `CONTRIBUTING.md`, or a
document under `docs/` cites a test that does not exist; the dated evidence
records, which cite the tests of their revision, are not checked.

Each `ci-` target pipes `go test -json` through `scripts/test-report.sh`,
which prints each package result as it finishes, then the output of every
failed or unfinished test and the 20 slowest top-level tests, and exits with
the status of `go test`; set `TEST_JSON` to keep the events. The `-timeout`
of each target bounds one package: 15 min for `ci-integration`, 30 min for
`ci-long`, and 5 min for `ci-pty`. CI configuration is versioned in
`.github/workflows/ci.yml`; workflow success is separate evidence from checks
run locally. Repository hosting settings such as branch protection, reviewer
assignment, and private vulnerability reporting are not configured by
committing workflow files.

Before delivery, inspect the diff, run the relevant tests and documented
checks, and record the actual outcomes. Explain unrun checks. Use the
[contribution guide](../CONTRIBUTING.md) and PR template to provide a reviewer
with the problem, resulting behavior, limits, and evidence. Passing tests do
not establish durability beyond what [V1 acceptance](v1-acceptance.md)
records.
