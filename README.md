# canti

`canti` is named after *canti*, the plural of *canto*, and focuses on composing
the right information for each agent step.

canti is a single-machine durable agent: a stateless Go decision core and a
harness that commits every fact to a SQLite journal before acting on it, runs
each command in its own Docker container, and asks a model for one decision
per step through the Anthropic Messages API or the Responses API; the run
names its model API and model, and canti assumes neither. Any later process resumes a run from the journal.
It is an architectural experiment for a future Memoh agent runtime.

[Architecture](docs/architecture.md) | [Development](docs/development.md) |
[V1 acceptance](docs/v1-acceptance.md) | [Contributing](CONTRIBUTING.md)

## Why two layers?

The project keeps the smallest possible decision semantics separate from the
long-running work required to make an agent useful and recoverable.

```text
committed state -> core.State.Intent -> harness I/O -> core.State.Next -> commit
```

The core decides, from committed facts alone, what the harness may do next and
how each reported fact changes the run. The harness owns model I/O, history,
persistence, execution, and the boundaries where external side effects become
observable.

## Components

- `runtime/core` accepts exactly one JSON decision, `exec` or `return`, and holds the
  complete run projection and its pure transition rules. It imports only the
  standard library and keeps no state between calls.
- `runtime/internal/harness` builds each request from committed facts within byte
  limits, commits one transition per fact, reads each accepted decision once
  into an action whose recovery contract it commits with it, and drives
  committed intents to a halt with step, attempt, wall-time, reported-token,
  output-byte, and job-time budgets.
- `runtime/internal/archive` reads the step records, artifacts, and imported history
  that jobs see, for the harness's retrieval action, without following a
  symlink below its root directories or reading environment files.
- `runtime/internal/harness/sqlite` is the journal: one row per transition with its
  facts, a sequence compare-and-set, idempotent retries, and strict decoding,
  on WAL with `synchronous=FULL`.
- `runtime/internal/harness/docker` runs each effect in one container named from its
  effect ID behind a durable start latch, so a restart never starts a job
  twice, and keeps bounded output artifacts and previews.
- `runtime/internal/harness/anthropic` and `runtime/internal/harness/openai` are the Messages
  and Responses adapters; `runtime/internal/harness/provider` sends the frozen request
  bytes of each attempt once over `net/http` for both. The Chat Completions
  API is not supported.
- `runtime/cmd/canti` is the operational CLI and host executable.
- `sdk/go/hostrpc` is the independent protocol and client module.
- `apps/tui/cmd/canti-tui` is the independent terminal and pipe chat client.

## Running an agent

Requirements: Go 1.25 or newer, Linux (the only verified platform), a Docker
daemon the `docker` CLI can reach, and the credential of the model API:
`ANTHROPIC_API_KEY` (or `ANTHROPIC_AUTH_TOKEN`) for `anthropic-messages`,
`OPENAI_API_KEY` for `openai-responses`. `-model-url`, or else
`ANTHROPIC_BASE_URL` or `OPENAI_BASE_URL`, names another provider of the same
API. The CLI reads every credential variable once and removes them all from
its environment before starting any child process; no credential enters the
journal.

```sh
go build -o bin/canti ./runtime/cmd/canti
go build -o bin/canti-tui ./apps/tui/cmd/canti-tui
export OPENAI_API_KEY=...   # or ANTHROPIC_API_KEY with -api anthropic-messages
mkdir -p ~/demo
bin/canti run -workspace ~/demo -api openai-responses -model MODEL \
  -goal "Write hello.txt containing hello, then return"
```

By default each command the agent chooses runs in a new
`debian:bookworm-slim` container with the workspace mounted at `/workspace`,
no network, and the host user's uid. It can read, but not change, earlier
outputs under `/canti/artifacts` and a record of every completed step of
every run of the state directory, its own included, under
`/canti/runs/<run ID>/`.
An image must provide `/bin/sh` and a `/tmp` that the job's uid can write,
where each job's shell records its exit status; without it the first step
already ends with an unknown outcome. `canti run -h` lists the image, limit,
model, and budget flags. Every canti command takes `-workspace` (default:
the current directory). The journal lives in a state directory that must
not overlap the workspace, neither inside it nor containing it; a workspace
that contains the default state root, such as `$HOME`, needs `-state`.

| Command | Effect |
| --- | --- |
| `canti run -goal TEXT` | Start a run and drive it until it returns or stops |
| `canti-tui [-session ID]` | Talk with the agent: each input becomes the next run of one session |
| `canti resume` | Continue the latest run, for example after a crash |
| `canti status`, `canti history` | Print journal-only run state as JSON, or transitions as JSON Lines |
| `canti stop` | Stop the run, cancelling its job and recording the job's result |
| `canti resolve -job` or `-execution STATE` | Recover an inspected effect, or record an operator assertion with `-note` and, for exited/timed-out work, `-owner` |
| `canti host` | Serve the state directory to the commands that drive runs, which start one on demand unless an operational command uses `-host embedded` or the TUI uses `-host owned`, until a signal or `canti host -stop` |

Only the current schema, semantics, executor record and context versions are
supported; an unsupported state directory is rejected with a fresh-state remedy.
Same-version restarts preserve committed facts and exact frozen request bytes.
Requests use a stable receipt prefix and selected frozen evidence. Confirmed
provider overflow resets the active context at the same unfinished step, retaining
the exact goal, constraints and archive navigation. `canti:focus` replaces the
selected evidence; historical snapshots remain retrievable. `-request-bytes` is
a resource ceiling, and `-token-budget` uses only reported usage. Status and TUI
reports distinguish unknown usage, reported zero and lost attempts.
An unknown outcome requires explicit inspection or operator resolution and never
authorizes rerunning the command.

The first SIGINT or SIGTERM stops the run the same way as `canti stop`. For
`run` and `resume`, exit status 0 means the agent returned, 3 that the run
stopped, and 4 that it needs resolution; see the
[development guide](docs/development.md#cli) for the rest.

## Design rules

1. The core stays stateless and free of external I/O.
2. The harness persists facts before dispatching external effects.
3. A timeout or unknown external result is not silently treated as success or
   as permission to run the effect again; it waits for inspection or an
   explicit resolution.
4. Interfaces are added at external boundaries; local implementations stay
   concrete until a second implementation needs the abstraction.
5. A `return` decision is a control-flow result, not a business-success flag.
6. The execution state of a process is kept separate from the business effect
   that process may have caused, and an action the harness performs itself
   records an outcome, never a process execution.
7. Run rules live in `core.State.Next`. The harness executes intents and
   never restates a transition; adding a capability adds no event kind.

## Status

Verified on Linux: offline tests of the core, harness, and adapters; tests
against real SQLite files, including SIGKILL recovery and request
construction over 2,000 stored steps; and Docker Engine 29.7 integration
tests in which CLI processes drive runs end to end.
Those cover cold restarts after every step, SIGKILL before and after each
commit of an exec step, a job that outlives its controller, unknown outcomes,
competing writers, retries with frozen bytes, large outputs, cancellation,
budgets, and the container trust boundary. The model in every end-to-end test
is a scripted local HTTP server speaking the Messages format.
[V1 acceptance](docs/v1-acceptance.md) records each gate, its tests, and their
limits; three gates are partial.

Live provider evidence is three manual samples with the earlier adapter
`canti-anthropic/1`; a live evaluation in 50 runs, 44 of them with
`canti-anthropic/2`, whose decisions passed a proxy that enforces the decision
schema; a feedback-round evaluation in nine sessions with
`canti-anthropic/3-tool` and no proxy; and a comparison of the retrieval system
prompts of context versions 7 and 8 in twelve runs. Each used one model through
an Anthropic Messages-compatible gateway; the last two reported cache reads for
most input tokens, not checked against their billing. SIGKILL tests do not
establish durability across power loss or OS crashes. Docker daemon restarts
are not tested. Of two host reboots met live, a job killed by one was recorded
with Docker's exit code 255; canti now reports a job killed before its shell
records an exit status unknown rather than exited, tested with `docker kill`,
while a signal the job observes, as from a graceful daemon shutdown, is
reported as its exit status. The container of an unknown job is kept for
`docker logs canti-<effect ID>` and must be removed by hand. Several hosts are
out of scope. Cost is not measured. Only retained job output is bounded on
disk, per job and per run; workspace writes, the journal, and a running job's
Docker log and container layer are not, and there is no filesystem quota.

Distributed scheduling, multi-tenant policy, generic workflow graphs,
automatic planning, and automatic memory compaction are outside this scope.

## Contribution shape

Keep changes focused on one boundary or one verification unit. New core
behavior should come with deterministic protocol tests. New harness behavior
should state its durable commit point, restart behavior, and treatment of
unknown external state.

Before opening a pull request, run `make check`, `go test -count=1 -race ./runtime/... ./sdk/go/... ./apps/tui/...`,
and `git diff --check`, plus `make integration` when a change touches storage,
execution, the model adapter, or the CLI. The CI runs the checks and the
integration tests on pushes to `main` and on pull requests.

Read [CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md) for ownership,
verification, and review rules. Report vulnerabilities using [SECURITY.md](SECURITY.md).

## License

Copyright 2026 ChrAlpha. Licensed under [Apache-2.0](LICENSE).
