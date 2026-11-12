# Development

## Prerequisites

- Go 1.26 or newer for the workspace and TUI; runtime and SDK declare their independent minimums in `go.mod`.
- Make, Python 3 for dependency provenance, CI policy/sharding and commit checks,
  and a POSIX shell; Bash and `jq` for the `ci-` targets.
- A C compiler and a supported platform for Go's race detector.
- A Unix-like OS: the CLI and executor use `flock` and process groups. Only
  Linux has been verified.
- For `make integration` and for running agents, a Docker daemon the `docker`
  CLI can reach. The integration tests run the image `alpine:3.23`;
  `CANTI_TEST_IMAGE` selects another image, which must provide `/bin/sh`, a
  `/tmp` the test uid can write, and the common shell tools the tests call,
  including `wget`.

The runtime module depends on `modernc.org/sqlite`, a cgo-free SQLite, pinned by the
committed `runtime/go.sum`. `make check` needs no credentials, `.env` file, database
server, container runtime, or Memoh service. Run commands in this repository,
not its parent workspace. Root `go.work` enumerates all three modules. To verify a module independently,
run its commands inside that module with `GOWORK=off`. Runtime and TUI use
relative SDK replacements; standalone builds require the monorepo checkout.

```sh
git clone https://github.com/ChrAlpha/canti.git
cd canti
make check
```

Cloning requires repository access while the repository is private.

## Commands

| Command | Purpose |
| --- | --- |
| `make check` | Formatting, module/test boundaries, vet, test names, unit and offline executable acceptance, and native dependency regressions |
| `make fmt` | Check `gofmt` output; does not rewrite files |
| `make vet` | `go vet` across all modules, then with `-tags integration,live` so tagged test files compile |
| `make test` | Unit tests in every module; no sibling executable builds |
| `make acceptance` | Offline separate-executable tests; root builds and supplies runtime, TUI and runtime fault-fixture binaries |
| `make native-deps` | Verify replacement provenance/reconstruction and run both upstream race suites |
| `make isolation` | Copy runtime and SDK alone, then build and run their default tests without TUI source |
| `make integration` | `go test -count=1 -race -tags integration ./...`: unit, Docker-backed, long-history and separate-executable acceptance tests |
| `make ci-unit` | CI's race-enabled unit tests, without the `PTY_TESTS` of the Makefile |
| `make ci-integration` | CI's `-tags integration` run without the `LONG_TESTS` and `PTY_TESTS` |
| `make ci-long` | The slow acceptance tests listed in `LONG_TESTS`; needs Docker |
| `make ci-pty` | Serial native terminal and executable acceptance packages selected by `PTY_TESTS`; needs Docker |
| `make test-names` | Check the test names of `LONG_TESTS`, `PTY_TESTS`, and the docs; needs Linux, where every test file builds |
| `make lint` | The cognitive complexity limit of `.golangci.yml`; defaults to pinned golangci-lint through `go run`, with `GOLANGCI_LINT` selecting an installed binary |
| `make commits BASE=main` | Build and vet, without and with the tags, every commit after `BASE` in a temporary worktree; `scripts/check-commits.sh BASE` takes a base whose name contains `$` |
| `go test -count=1 -race ./runtime/... ./sdk/go/... ./apps/tui/...` | Fresh race-enabled unit test run |
| `go test -count=1 -race -tags integration -run 'TestGate' -v ./runtime/cmd/canti` | The CLI gate tests with their `evidence` log lines; `make integration` runs every gate test |
| `go test ./runtime/core -run TestDecodeRejectsInvalidProtocol -v` | Focused protocol regression cases |
| `go test ./runtime/core -run 'TestNext\|TestIntent\|TestValidate\|TestTrajectory\|TestIllegal' -v` | Transition table, budgets, invariants, trajectories, and rejected events |
| `go test ./runtime/internal/harness -run 'TestCommit\|TestLoop\|TestEffect\|TestRunner\|TestStale\|TestOffline' -v` | Commit protocol, accepted-effect execution, failure, and cancellation cases |
| `go test ./runtime/internal/harness/sqlite -run TestCrashKeepsAcknowledgedCommits -v` | SIGKILL of a committing process and recovery of every acknowledged commit |
| `git diff --check` | Check pending patches for whitespace errors |

Use `gofmt -w path/to/changed.go` on changed files to resolve a formatting
failure. Do not run a repository-wide rewrite as part of an unrelated change.

Build tags keep slow and paid tests out of the default run. `integration`
marks tests that need Docker or take tens of seconds; CI runs them in
separate jobs. `live` marks the `live_test.go` of each model adapter, which
sends one real request and is never part of `make check`, `make integration`,
or CI:

```sh
ANTHROPIC_API_KEY=... ANTHROPIC_MODEL=... go test -count=1 -tags live -run TestLiveDecision -v ./runtime/internal/harness/anthropic
OPENAI_API_KEY=... OPENAI_MODEL=... go test -count=1 -tags live -run TestLiveDecision -v ./runtime/internal/harness/openai
```

Each skips without its credential and model, as canti has no default model.
`ANTHROPIC_BASE_URL` or `OPENAI_BASE_URL` selects another provider of the
API, `CANTI_LIVE_EFFORT` an effort level, `CANTI_LIVE_DECISION_FORMAT=tool`
the tool transport, and `CANTI_LIVE_STRUCTURED=0` omits the decision schema.
A request is capped at 1,024 output tokens and may be billed. It checks one
decision, not a loop.

## CLI

```sh
go build -o bin/canti ./runtime/cmd/canti
go build -o bin/canti-job ./runtime/cmd/canti-job
go build -o bin/canti-tui ./apps/tui/cmd/canti-tui
./bin/canti -version
./bin/canti -h
./bin/canti run -h
```

`-version` (or `version`) writes `dev` to standard output and exits with
status 0. `-h` writes usage to standard output and exits with status 0.
Invoking the binary with no arguments writes usage to standard error and exits
with status 2. `go run` itself reports a non-zero wrapper status for a failing
program, so use the binary to inspect its exact exit code.

`canti environment init -environment DIRECTORY` prepares private controller state,
a read-only helper tree, and a writable spool, without creating a run journal.
Its JSON result supplies `authority_id`. Provision Docker/Compose/Dev Containers
with labels `io.canti.control=<canonical directory>` and
`io.canti.authority=<authority_id>`, and separate mounts of `DIRECTORY/public` at
`/canti:ro` and `DIRECTORY/spool` at `/var/lib/canti/jobs:rw`. Mount selected
workspaces separately; keep run state and model credentials outside every mount.
`environment attach -environment DIRECTORY -container NAME` pins the real ID and
selected local Unix Docker endpoint. It refuses a container user that cannot write
the spool, which stays private to the host user: run the container as that UID,
or as root without user-namespace remapping. `inspect`, `stop`, `start` and `detach` use
that same directory. Stop affects services as well as the foreground command.

An ambiguous lifecycle request keeps the environment quarantined. `environment
retire` closes admission and tells the operator to remove the exact old container
with existing provisioning tools; retry retirement after its absence is confirmed,
then explicitly attach the replacement. A new container loses manual system-layer
installations; only designated volumes survive. Neither elapsed time nor a stopped
sample clears an old unacknowledged request. `canti-job` must be packaged beside
`canti`, or named explicitly with `environment init -helper PATH`. Initialization
publishes that helper once and never replaces it, so an environment keeps the
helper of the canti that initialized it. To adopt a newer helper, initialize a new
directory and attach a container prepared for its labels and mounts.

### Persistent development environments

New `run` and `canti-tui` invocations require `-environment DIRECTORY` or
`-ephemeral`. Persistent environments are prepared with ordinary Docker,
Compose or Dev Containers tooling. No canti provisioning daemon is needed.
Run state and the attachment authority must remain outside every environment
mount; attachment and admission reject exposure through additional mounts,
including read-only mounts. Only local Linux Unix Docker endpoints with coherent
host bind paths are supported. Attach pins the selected endpoint, daemon ID and
actual container ID; changing Docker context cannot redirect a resumed run.

The ordinary example uses a developer UID/GID matching the host, private HOME,
Go 1.26, normal networking and passwordless sudo *inside the environment* for
package installation. It forwards no host HOME, Git credentials, SSH agent or
Docker socket. Prepare it from the repository root, with `canti` and `canti-job`
already built beside each other:

```sh
export CANTI_WORKSPACE="$HOME/canti-demo/workspace"
export CANTI_ENVIRONMENT="$HOME/canti-demo/environment"
mkdir -p "$CANTI_WORKSPACE" "$CANTI_ENVIRONMENT"
export CANTI_WORKSPACE="$(realpath "$CANTI_WORKSPACE")"
export CANTI_ENVIRONMENT="$(realpath "$CANTI_ENVIRONMENT")"
export CANTI_UID="$(id -u)" CANTI_GID="$(id -g)"
export CANTI_AUTHORITY_ID="$(bin/canti environment init -environment "$CANTI_ENVIRONMENT" |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["authority_id"])')"
docker compose --env-file /dev/null \
  -f examples/persistent-environment/compose.yaml up -d --build
container=$(docker compose --env-file /dev/null \
  -f examples/persistent-environment/compose.yaml ps -q dev)
bin/canti environment attach -environment "$CANTI_ENVIRONMENT" -container "$container"
bin/canti run -workspace "$CANTI_WORKSPACE" -state "$HOME/canti-demo/state" \
  -environment "$CANTI_ENVIRONMENT" -api openai-responses -model MODEL \
  -goal 'Install jq with sudo apt-get, create a private HOME config, then use jq'
bin/canti-tui -workspace "$CANTI_WORKSPACE" -state "$HOME/canti-demo/state" \
  -environment "$CANTI_ENVIRONMENT" -api openai-responses -model MODEL
```

The Compose file names its project `canti-dev`, which the Dev Containers CLI
honors as well; an exported `COMPOSE_PROJECT_NAME` overrides it for both. Either
tool therefore manages the same container instead of a second claimant of the
control label, which attachment would refuse. The same Compose service can be
opened using `examples/persistent-environment/.devcontainer/devcontainer.json`.
Set the same variables, then run:

```sh
COMPOSE_BAKE=false devcontainer up --workspace-folder "$CANTI_WORKSPACE" \
  --config examples/persistent-environment/.devcontainer/devcontainer.json
```

Attach the returned container ID. Dev Containers adds its own labels, so switching
an attached container between `docker compose up` and `devcontainer up` can
replace it: follow the rebuild procedure below. On the verified Dev Containers CLI
0.90.0 and Compose 5.7.0 combination, disabling Bake for this Compose-backed
invocation avoids its rejection of the generated Dockerfile outside the build
context (`fs.read` privilege request). The direct Compose invocation above and the
Dockerfile-based DinD invocation below use their own build paths; the recorded QA
runs required this workaround only for the ordinary Dev Container command. Use a
non-root host UID with these examples.

Commands use the pinned container user, HOME and PATH, `/bin/sh -c`, and a fresh
shell each time. `environment attach -init /absolute/file` sources an optional
initialization file on each command. The default cwd maps the exact selected
host workspace's unique writable bind; `-environment-workdir /workspace/subdir`
selects a directory within that mapping. Multiple deliberately related workspace
binds can share the environment. Put background-service output in separate log
files and redirect stdin, stdout and stderr so command output can finish. Shell
variables alone do not survive the next command.

A return or context reset leaves packages and services intact.
`environment stop` ends foreground work and services; `environment start` retains
the same filesystem with a new start epoch. Cancellation and job timeout also stop
the whole environment, so explicitly start it before another command. Inspect the
environment gate before start: an unacknowledged mutation stays quarantined and
requires the retirement/replacement procedure above. Ordinary acknowledged
cancellation does not require a rebuild. For a deliberate rebuild, including `up
--build` after a configuration change, stop and detach before replacing the old
container with Compose or Dev Containers, then explicitly attach the new ID;
compare `ps -q dev` with the `container_id` that `environment attach` printed.
Once the attached container is gone, `environment inspect` fails with Docker's
missing-container error instead of a status. Detach needs the old container, so if
it was replaced first, run `environment retire`, which confirms the old ID is
gone, and then attach the new ID. The workspace bind survives; manually installed
system packages and private HOME in the container layer are replaced. Add private
volumes deliberately when those files should survive rebuilds.

For canti development that needs a private Docker daemon, the optional
`examples/persistent-environment-dind/.devcontainer/devcontainer.json` uses the
[official Docker-in-Docker Feature](https://github.com/devcontainers/features/tree/main/src/docker-in-docker).
It explicitly enables privileged mode and gives Docker and containerd separate
private persistent named volumes. The Feature runs an inner daemon, without
forwarding the outer host socket. Privileged execution grants a broader host
kernel/device boundary than the ordinary example; use it for trusted development
on a suitable host. The volumes retain inner images and data across outer
container replacement; they do not preserve the outer system layer.

Use a separate initialized `CANTI_ENVIRONMENT`, set `CANTI_WORKSPACE` to this
repository's canonical path, and keep `CANTI_UID`, `CANTI_GID` and the new
`CANTI_AUTHORITY_ID` exported. Then provision and attach:

```sh
devcontainer up --workspace-folder "$CANTI_WORKSPACE" \
  --config examples/persistent-environment-dind/.devcontainer/devcontainer.json
# Use the containerId from that command's JSON output.
bin/canti environment attach -environment "$CANTI_ENVIRONMENT" -container CONTAINER_ID
# Inside the prepared environment, verify the inner daemon and canti integration.
devcontainer exec --workspace-folder "$CANTI_WORKSPACE" \
  --config examples/persistent-environment-dind/.devcontainer/devcontainer.json \
  sh -c 'docker info && make integration'
```

Both examples use mutable image tags and standard tooling. No image digest is
required. Provisioning, output retention and journaling do not impose filesystem
quotas; size your workspace and private Docker volumes for the workload.

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
`-api openai-completions` is refused. `canti run`, `canti-tui`, and `canti resume` read the
credential of the run's API, `ANTHROPIC_API_KEY` or else
`ANTHROPIC_AUTH_TOKEN` for `anthropic-messages` and `OPENAI_API_KEY` for
`openai-responses`. A new run stores its provider, the API base URL from
`-model-url`, or else `ANTHROPIC_BASE_URL` or `OPENAI_BASE_URL`, or else the
API's public endpoint; the URL must use `https` unless it is a loopback
address and hold no user information, query, or fragment. A later command
drives the run against the stored provider only. A credential is bound to
the exact provider its command resolves, from `-model-url`, the base URL
variable, or the default, so a command whose resolution names another
provider than the run stores is refused before any request, and its key is
never sent; a run stored before runs kept their provider takes the
command's, as `run` does. The credential is never saved. Every command removes all three credential variables from
its environment before it starts a process. `-effort` takes a level of the
run's API and is sent only when set; `-fallbacks` requires
`anthropic-messages`. The goal, constraints, and the image, limit, model,
prompt, and budget flags of
`run` are stored with the run and reused by every later command; `-once`,
`-state`, and `-log-level` apply to one invocation, and
`-workspace` must name the run's workspace. For explicit `-ephemeral`, defaults are image
`debian:bookworm-slim`, network `none`, memory `2g`, no CPU limit, 512
processes, 8 MiB retained output per job, 16,000 output tokens per
response, no effort, structured output on, fallbacks off, 8 MiB per rendered
request and serialized provider body (`-request-bytes`), 20 steps, 3 attempts per
step, 30 min wall time, no token budget, 256 MiB of output per run, and 10 min per
job. `-token-budget` stops after the threshold of reported input plus output usage
is reached. Unknown usage is retained as unknown; no counts are estimated.
Status and TUI reports show coverage and lost attempts alongside reported totals.

`run -input ID` names the input: a later `run` with the same input ID names
the same input instead of admitting another, reporting its current disposition
and run, and fails when the text, constraints, delivery, or freshness differ.
An input without `-input` always creates new work. Use the same ID to retry a
submission whose outcome was lost.

While the agent is working, a principal input must name `-delivery steer` to
join the open run or `-delivery follow_up` to wait for its return. A steer is
acknowledged, not driven: the printed acknowledgement names its input position
and disposition. A follow-up opens after the current run returns, in the same
state directory, and is driven by the host that holds its submission's
credential or by a host credential for the run's pair. When the agent is
quiescent, delivery is ignored and the input opens a run. Every input belongs
to the state directory's one line; `-fresh` starts a new line, with no link to
the previous run and no inherited constraints. A non-fresh input inherits the
latest run's constraints unless `-constraints` is given.

`run` accepts at most 32 KiB per steer and 32 KiB of bound steers per run;
observations carry at most 16 KiB and name a source and subject of at most 256
bytes. A new principal input is refused with `held` while a stopped run holds
inputs; `canti withdraw` returns those texts, and they must be resubmitted.
`canti observe -source S -subject K` admits evidence without touching a run;
`canti cancel` cancels the latest or named run's current effect and leaves the
run to continue.

`canti-tui` takes the flags of `run` except `-goal`, `-input`, and `-once`. Its
inputs and runs form the state directory's one line; `-fresh` applies to the
first input of the invocation. Each new run uses the current launch flags. The
log level defaults to `warn`.

**Terminal input (TTY):** `canti-tui` uses a full-screen TUI on the alternate screen,
rendered to standard error. There is currently no plain interactive terminal
mode. Enter submits, Alt+Enter inserts a newline, and PgUp/PgDn scroll the
retained display, including while submitting, running or stopping. Text and paste always edit the
in-memory draft. Enter sends a new input when the agent is ready and submits the
draft as a steer while a run is active; Ctrl+F submits it as a follow-up.
Ctrl+X cancels the current effect and leaves the run continuing. Alt+Enter
continues to insert newlines. A submitted snapshot is separate from newer
edits: a rejected input can be restored with Ctrl+R into an empty draft. If
acknowledgement remains unknown, Ctrl+R reconciles the same InputID while
preserving the new draft. After a stop with held inputs, the footer notices
them and Ctrl+R withdraws their texts into the draft.
Ctrl+C stops the current run and waits for cleanup before returning to the
editor; a second Ctrl+C abandons that cleanup wait, leaving recovery to
`canti resume` or `canti stop`. Ctrl+C while idle exits.

The TUI combines answers, diagnostics, and committed step output in one
viewport. Its typed progress queue retains at most 128 entries and 64 KiB; acceptance and
final outcome have separate bounded protocol slots. The transcript retains at
most 256 entries and 64 KiB, with omission notices. Final outcome and recovery
guidance survive progress eviction. Only text from this
process invocation is displayed: a new invocation does **not** load previous
conversation or execution history. On exit, the retained transcript
is printed to standard error. The alternate screen and this bounded tail do
not preserve a complete native terminal or tmux scrollback. Use `canti history`
for a run's full recorded transitions. Display history is separate from the
previous runs supplied to the model; the next request names the closed runs of
the line and their archive paths. Retrieve earlier dialogue explicitly.

**Host process.** Operational commands and `canti-tui` default to `-host auto`,
which attaches to the shared host of the state directory and leaves it driving
when the client detaches. Operational commands also support `-host embedded`,
which runs a host in that command's process.

`canti-tui -host owned` launches a separate runtime process exclusively for each
input and reaps it before enabling the next submission. The editor remains
editable throughout cleanup. It rejects any existing host,
including a different process winning a startup race; every successful Hello
must report the PID of the child it launched. Use `-host auto` to attach to a
shared host. The setup briefly opens an owned host to initialize the state
layout, then releases it; between inputs no host or admission lock is held.
Ctrl+C requests persistent stop and waits for cleanup. A second signal abandons
the wait and shuts down the owned host, returning error status 1; unfinished
cleanup remains recoverable through `canti stop`.
Terminal hangup or display failure stops the active run before owned shutdown.
An abrupt client death sends SIGTERM to its child, which shuts down its drive;
it does not invent a successful result or a persistent stop. Unknown effects
retain the normal inspection/resolution contract. Auto mode instead detaches
on terminal hangup or a second signal, leaving the shared host driving.

The TUI always uses the public protocol and never opens the journal. `-runtime
PATH` selects the runtime executable; otherwise it finds `canti` on PATH.
Before creating state it calls the runtime's versioned `preflight` interface,
which validates configuration, layout, credentials, execution resource settings
and context admission without writes. A live host binds validation to its own
state/workspace and supplies the constraints a non-fresh input inherits from
the latest run, unless the submission states its own. Validation reserves no
admission or input identity; Submit rechecks everything under the host lock.
Preflight requires exactly one JSON request of at most 1 MiB. The client caps
stdout at 1 MiB and stderr at 16 KiB, kills the process group on overflow or
cancellation, limits total time to 30 seconds and inherited-pipe cleanup to one
second, and rejects trailing/unknown response fields. During startup, the first
signal cancels preflight or connection setup, and a second signal forces owned
child cleanup. Test fixtures select
embedded operational commands and owned TUI clients unless a test names auto.

A client that finds no host and an unlocked
state directory starts `canti host` in a session of
its own, with no credential in its environment and logging to `host.log` in
the state directory, and waits up to `-host-wait` (10 s) for it to serve.
Each client waits for its launched process to exit before launching a
replacement, even before that process holds the state lock. It refuses a host
of another protocol version, naming its process and how to
stop it, the canti that started it or SIGTERM, since it is never replaced
while it may drive a run, and exits with status 6 when an
embedded command holds the state directory. An autostarted host exits after
10 minutes without clients or drives; `canti host` in a terminal serves
until a signal or `canti host -stop`. A client gives the host the key of its
run's API with each submission or resume, and the host keeps it only while it
drives that run. A `canti host` started in a terminal binds each key of its
own environment to the provider its base URL variable names, or the
default, prints each pair without the key, and drives with it a run of that
exact pair whose client gives no key; when it starts it continues the open
run it can continue by itself, as it finishes a pending stop. A host that a
client starts holds no key. `run` without `-input` names its submission with a new
input ID, which it prints, and sends it again while an answer is lost, to a
new host if the old one went away; when the answer then names the run its
lost attempt admitted, it drives that run, once, and follows it. `-input ID`
retries it later, reporting the run as it is. A client that arrives while a
host exits waits for it and starts the next one. Without a key for the run's
API, a client with `-host auto` may still submit or resume a run whose API
and provider the serving host holds a credential for. An attached client's first signal stops the run, the
host bounding the cleanup; a second signal, or the end of the client,
detaches: the host keeps driving the run, which `resume -host auto` follows
again. The socket path must fit a Unix socket, 107 bytes.

A host process runs the `docker` CLI with the environment it started in,
for its whole lifetime: `DOCKER_HOST`, `DOCKER_CONTEXT`, `DOCKER_CONFIG`,
`DOCKER_DEFAULT_PLATFORM`, and the `PATH` it finds `docker` and its
credential helpers on. An autostarted host has the environment of the
client that started it, without its credential variables. A later client's
environment does not rebind a running host. Only the environment is fixed:
the CLI still reads its context and configuration files, and resolves its
binaries and helpers, on each call, so a change to those files or to the
binaries on that `PATH` reaches the host's next call. To use another Docker
environment, stop the host once it drives nothing, with `canti host -stop`
or SIGTERM to the process it reports, and run the next command from that
environment, which starts a new host; or run the command with
`-host embedded` (operational CLI) or `-host owned` (TUI), which uses its own environment. The model provider is not
resolved this way: each run stores its own.

**Pipe or file input (non-TTY):** inputs are read one line at a time, or as a
block after `<<WORD` or `<<'WORD'`, ending at a line exactly equal to `WORD`.
`WORD` consists of ASCII letters, digits, underscores, or hyphens. `<<-WORD`
also strips leading tabs. An input that starts with `<<` must be sent in a
block. These block markers are not special in the TUI editor. Piped inputs are
processed sequentially. Oversized lines and blocks are rejected in full; their
boundaries are consumed with bounded storage, so following inputs remain intact.
Answers go to standard output; prompts, commands, output previews, execution
status, and diagnostics go to standard error. End of input exits with status
0, or 1 inside an unfinished block, which is not submitted.

Both modes display command previews of up to 8 lines of 200 bytes each, output
tails, and execution status. Lines containing control characters are quoted
as Go strings after `(quoted)`; answers and output are cleaned of control
characters. A stopped run reports its stop reason and recorded steps. SIGTERM,
or Ctrl+C with non-TTY input, stops the run and ends chat with the run's status:
3 if stopped, 0 if it had already returned. A terminal UI input or output failure,
including a terminal hangup (SIGHUP), detaches in auto mode or requests stop
and cleanup in owned mode, then exits with an error; abrupt process loss may still require `canti resume` or
`canti stop`. Pipe SIGHUP uses the same ownership policy; an auto detach
reports status 5 without requesting persistent stop.

The client preserves inherited file-status flags. Output to a pipe or terminal
uses its own reopened description and a 250 ms write deadline; socket output
uses per-call nonblocking writes. A pipe or terminal that cannot support bounded
output (including a pseudo-terminal master) is refused before host startup or
state creation. Ordinary pipe and file presentation remains supported. Native
Bubble Tea owns the exclusive input reader, decoding, raw mode and restoration;
no output operation consumes input or changes the draft. SIGKILL cannot restore
terminal modes.

Empty input is skipped. Editor admission and native paste storage use the
runtime's goal-byte limit. Pending escape sequences are capped at 64 KiB.
Oversized paste preserves the draft and recovers at its end marker, allowing at
most another 64 KiB or two seconds. Exhausting either recovery bound, or an
unterminated control sequence, ends input through the same ownership policy.
Tea 2.1.1 and its pinned ultraviolet revision are local, licensed source
replacements under `apps/tui/third_party`; their manifest and patch are verified
against upstream archives by `make native-deps`.

Input that fails validation (length, UTF-8 or `-request-bytes` admission) is reported;
the terminal retains one rejected snapshot for explicit restoration. Malformed
pipe block openers are reported without submission. A run that needs resolution,
or constraints and flags leaving no room for any input, ends the client with
recovery advice. If acceptance remains unknown at exit, both presenters print
the pending InputID and require the original goal and launch flags for
reconciliation before new input. Neither mode claims a `return` proves business
success.

The state directory defaults to
`$XDG_STATE_HOME/canti/<first 16 hex digits of the SHA-256 of the workspace's
resolved path>`, with `~/.local/state` when `XDG_STATE_HOME` is unset or
relative; `-state` overrides it, and it must not overlap the workspace or
resolve to a path with a colon, which Docker cannot mount. Existing State or
parent symlinks are resolved before any state files are created. Safe outside
aliases use one canonical state and socket identity; dangling, looping or
unresolvable ancestors are refused. Persistent admission compares that effective
path against every environment mount before creating state. It holds `canti.db` with its WAL files, the `lock` file, per-effect latches and
final job records under `jobs/`, output artifacts under `artifacts/`, and the
read-only step records of every run under `history/<run ID>/`, which ephemeral jobs see
at `/canti/runs/<run ID>/`; persistent commands use these logical addresses
through native `canti:retrieve`.
Each run is bound to the workspace it was created in. A request keeps every step
completed since its current anchor in full, with the steps before it reachable
through the read-only archive. `canti:handover`, optionally followed by a note,
closes a phase: the steps before it leave later requests, and the earlier-steps
index shows the newest steps before the anchor within its 16-entry and
2,048-byte bounds, the handover step included; it is omitted under `Minimal`,
and older records stay in the archive. The note is archived with its step and
not rendered automatically; ordinary retrieval can read it. Confirmed provider
overflow opens a new anchor at the same unfinished step when retry remains
possible; the exact goal, constraints and archive navigation survive. A minimal
context stays minimal through further resets until a successful handover. A
rejection of a minimal head is terminal only when the rejected request had no
resident epoch step and no protocol repair; either one opens another reset that
keeps `Minimal`. A rejection of authoritative orientation alone is terminal. No
summary is required. As Bub's handoff does, a handover moves only the read
boundary: it generates no summary and deletes no history. Exact historical
snapshots remain readable from step
`evidence.json` and `evidence/0000` files. Removed focus/handover flags and
actions are rejected. See [Input](architecture.md#input).
History retrieval is always available; `-history-imports` adds a read-only
archive, a directory named relative to the command's working directory and
stored canonical. A new run needs it to exist; a retry of an input names its
admitted run even after the directory is gone, and names `history_imports`
among its differences when it names another one. Workspace notes are
ordinary files and are not automatically read.

Each run stores current context version 20. Schema 6 keeps one agent log of
transitions and admitted inputs, semantics 8 bounds and supersedes principal
instructions, ephemeral executor record format 2, attached executor format 1 and
host protocol 9 form one current contract. Unsupported formats and context
versions are rejected; use a fresh state directory. No migration or old-run
settlement path is supported. Same-version frozen bytes, provider bindings,
accepted identities and results remain immutable.

`canti status` and progress pages read only the journal. `canti inspect` performs
one bounded non-starting physical inspection and reports its sample timestamp
and any unavailability. `canti resolve -job` records active recovery or inspected
terminal evidence; an operator assertion uses `-execution`, a required `-note`,
and `-owner` for exited/timed-out work. Resolution never stops a running owner.

A CLI check without Docker or credentials proves only that the command builds
and parses its arguments; the integration tests drive real runs with a
scripted model.

## Where to work

Read [architecture](architecture.md) for interfaces and ownership. Read
[V1 acceptance](v1-acceptance.md) before changing a concrete adapter or a
gate. Keep tests beside the package they verify: `runtime/core/*_test.go`,
`runtime/internal/harness/*_test.go` (the loop tests use a fake model and executor
over a real SQLite journal), `runtime/internal/harness/sqlite`, `runtime/internal/harness/docker`
(a fake daemon for unit tests, the daemon for `integration_test.go`),
`runtime/internal/harness/anthropic` and `runtime/internal/harness/openai` (a local HTTP
server), and `runtime/cmd/canti`, whose tests run the test binary as the CLI in
subprocesses against a scripted server of both model APIs. The gate tests are `runtime/cmd/canti/gate_*_test.go`.

Protocol and transition tests should stay deterministic and require no
credentials. A new run rule belongs in `core.State.Next` with a row in the
transition table test; the illegal-pair sweep must keep rejecting everything
else. Backend tests use temporary, isolated workspaces and state directories
and remove their containers; the CLI gate fixtures also fail on a container
left running. Live model checks must be explicit opt-ins with finite budgets;
do not make paid provider calls part of default tests.

`TestLoopScenariosKeepTheirCommittedRecords` compares what its scenarios commit
with `runtime/internal/harness/testdata/frozen-requests/<scenario>`: the exact body of
every frozen model request and, in `journal.txt`, the event ID and facts of
every transition. It also fails on a directory
that no scenario records. Only for an intended prompt, adapter, or record
change, rewrite them with
`go test ./runtime/internal/harness -run TestLoopScenariosKeepTheirCommittedRecords -update-requests`
and review the diff. `TestContextVersionPinsItsRules` digests these goldens
into the rules of the current context version, which
`runtime/cmd/canti/testdata/context-v<version>.txt` records once: rewriting the
goldens of a scenario, or adding one, which may observe a changed rule,
requires a new context version, recorded with
`go test ./runtime/cmd/canti -run TestContextVersionPinsItsRules -record-context`.
Review changes to current request bytes separately from journal identity changes.
The current corpus verifies same-version frozen requests, including cold restart
and lost acknowledgements. Historical files remain audit evidence; current
readers do not interpret old semantics or migrate old databases. Rejection tests
cover unsupported formats. Same-version corruption tests cover accepted identity,
handover and rejection provenance, the resident epoch capture and exact immutable
request bytes.

When adding dependencies, use Go modules and commit the resulting `go.sum`.
Record the minimum toolchain or platform change and update CI in the same unit.
Keep large outputs, credentials, databases, and local task state outside Git.

## CI and delivery

The CI workflow runs on pushes to `main` and pull requests. A new PR update
cancels the previous run of that PR; main runs have unique concurrency groups
so that every commit can finish, including runs still pending. `changes`
validates the CI policy and selects the workload from both sides of the diff,
including deleted and renamed paths. Only top-level Markdown and Markdown
under `docs/` skip heavy jobs; runtime prompts, fixtures, scripts, workflows,
and unknown paths run the full workload. Documentation changes still run the
test-name/reference check, and every PR still builds and vets every commit.

`quality` checks test names and documentation references; for code changes it
also runs `make fmt graph vet` and `make build-smoke`, which checks all modules
and exercises the freshly built executables without linking them twice.
Independent `lint` uses the checksum-verified prebuilt golangci-lint at the same pinned version as
the Makefile. It runs `gocognit` at a cognitive complexity of 30 over production
code, without and with `integration,live` tags, plus the app’s
`acceptance,integration` tests; `.golangci.yml` lists the
exceptions. `commits` builds and vets every commit after the base branch in a
temporary worktree, using the actual PR head rather than GitHub's synthetic
merge commit. Commits with identical non-documentation trees reuse an earlier
successful build/vet in that invocation; each distinct code tree is checked,
including a broken intermediate commit later reverted. The native
`actions/setup-go` cache uses all three module sum files, the native dependency
module sums, and `go.work.sum`.
The workspace jobs use Go 1.26. `native-deps` verifies patch provenance and
runs the native dependency upstream race suites once, on its own cached runner.
`module-minimum` builds/tests an isolated runtime+SDK copy on Go 1.25 with
automatic toolchain upgrades disabled and no TUI source.
Each workload has a stable `GOCACHE` path, which is part of the cache archive
version. Quality and commits have their own scopes so an earlier lint publisher
cannot exclude their compiler outputs; lint keeps the standard scope and PTY
is separate from full integration.
Cache snapshots are immutable until dependencies change; Go still checks current
source and rebuilds affected packages on every run. Only Go modules and compiler
outputs are cached, within GitHub's repository cache budget of 10 GB.

`unit` and `long-shards` each run four independent shards of `make ci-unit` or
`make ci-long`; `integration-shards` runs six shards of `make ci-integration`.
Six-way integration is a measured trial: compare the actual critical path and
runner cost, and retain the count only if the results justify it.
`CI_SHARD_COUNT` and `CI_SHARD_INDEX` partition the names discovered by
`go test -list .`; unit and integration use a stable hash. Long tests are placed
longest first on the least loaded shard, using measured duration estimates in
`scripts/ci-shard.py` and a 40-second estimate for newly discovered tests.
Estimates affect placement only; every selected top-level test, fuzz seed, and
example belongs to exactly one shard, and its subtests stay together. The
partition supports any positive shard count, including empty shards.
Without shard variables the targets run their complete original suites.
Discovery uses the same race and build-tag flags and fails on a compilation
error. Root-orchestrated executable acceptance uses the same shard runner and
filters as its unit/integration/long lane; it is partitioned once across that lane,
not repeated in full by each shard. Root builds the separate runtime and TUI
binaries and the runtime-owned fault fixture; default module tests build no
sibling source. The module graph check includes test dependencies.
`pty` runs `internal/terminal` and app acceptance sequentially with `-p 1` on its
own runner, explicitly disabling shard variables. Docker jobs pull
`alpine:3.23` and fail on any leftover container labelled `canti.effect`.

The stable `integration` and `long` jobs require all their shards. `check`
always runs and requires all selected work to succeed, including native
provenance, module minimums, and `commits` for PRs; failure, cancellation,
a missing selection, or an unexpected skipped job
fails the gate. Docs-only runs retain these stable result paths. The Makefile
lists `LONG_TESTS` and `PTY_TESTS`; add each new slow or pseudo-terminal test to
its corresponding list. `make test-names` rejects missing or overlapping names,
unlisted tests that open a pseudo-terminal, and nonexistent test references in
`README.md`, `AGENTS.md`, `CONTRIBUTING.md`, or `docs/`. Dated evidence records
cite their own revision and are excluded from the reference check.

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
