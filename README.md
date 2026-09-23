# canti

`canti` is named after *canti*, the plural of *canto*, and focuses on composing
the right information for each agent step.

canti is a single-machine durable agent: a stateless Go decision core and a
harness that commits every fact to a SQLite journal before acting on it, runs
each command in its own Docker container, and asks an Anthropic Messages model
for one decision per step. Any later process resumes a run from the journal.
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

- `core` accepts exactly one JSON decision, `exec` or `return`, and holds the
  complete run projection and its pure transition rules. It imports only the
  standard library and keeps no state between calls.
- `internal/harness` builds each request from committed facts within byte
  limits, commits one transition per fact, and drives committed intents to a
  halt with step, attempt, wall-time, token, output-byte, and job-time budgets.
- `internal/harness/sqlite` is the journal: one row per transition with its
  facts, a sequence compare-and-set, idempotent retries, and strict decoding,
  on WAL with `synchronous=FULL`.
- `internal/harness/docker` runs each effect in one container named from its
  effect ID behind a durable start latch, so a restart never starts a job
  twice, and keeps bounded output artifacts and previews.
- `internal/harness/anthropic` sends the frozen request bytes of each attempt
  once over `net/http`.
- `cmd/canti` is the CLI.

## Running an agent

Requirements: Go 1.25 or newer, Linux (the only verified platform), a Docker
daemon the `docker` CLI can reach, and `ANTHROPIC_API_KEY` (or
`ANTHROPIC_AUTH_TOKEN`). The CLI reads the credential once and removes it from
its environment before starting any child process; it never enters the
journal.

```sh
go build -o bin/canti ./cmd/canti
export ANTHROPIC_API_KEY=...
mkdir -p ~/demo
bin/canti run -workspace ~/demo -goal "Write hello.txt containing hello, then return"
```

By default each command the agent chooses runs in a new
`debian:bookworm-slim` container with the workspace mounted at `/workspace`,
no network, and the host user's uid; `canti run -h` lists the image, limit,
model, and budget flags. Every canti command takes `-workspace` (default: the
current directory). The journal lives in a state directory that must not
overlap the workspace, neither inside it nor containing it; a workspace that
contains the default state root, such as `$HOME`, needs `-state`.

| Command | Effect |
| --- | --- |
| `canti run -goal TEXT` | Start a run and drive it until it returns or stops |
| `canti resume` | Continue the latest run, for example after a crash |
| `canti status`, `canti history` | Print the run state as JSON, or its transitions as JSON Lines |
| `canti stop` | Stop the run, cancelling its job and recording the job's result |
| `canti resolve -job` or `-execution STATE` | Record the job, or the outcome, of an effect whose result is unknown |

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
   that process may have caused.
7. Run rules live in `core.State.Next`. The harness executes intents and
   never restates a transition; adding a capability adds no event kind.

## Status

Verified on Linux at the revision in the
[evidence record](docs/v1-acceptance.md#evidence-record): offline tests of the
core, harness, and adapters; tests against real SQLite files, including
SIGKILL recovery and request construction over 2,000 stored steps; and Docker
Engine 29.7 integration tests in which CLI processes drive runs end to end.
Those cover cold restarts after every step, SIGKILL before and after each
commit of an exec step, a job that outlives its controller, unknown outcomes,
competing writers, retries with frozen bytes, large outputs, cancellation,
budgets, and the container trust boundary. The model in every end-to-end test
is a scripted local HTTP server speaking the Messages format.
[V1 acceptance](docs/v1-acceptance.md) records each gate, its tests, and their
limits; two gates are partial.

Not verified: no run against a live provider has been recorded. SIGKILL tests
do not establish durability across power loss or OS crashes. Docker daemon
restarts and host reboots are not tested, and several hosts are out of scope.
Cost is not measured. Only retained job output is bounded on disk, per job and
per run; workspace writes, the journal, and a running job's Docker log and
container layer are not, and there is no filesystem quota.

Distributed scheduling, multi-tenant policy, generic workflow graphs,
automatic planning, and automatic memory compaction are outside this scope.

## Contribution shape

Keep changes focused on one boundary or one verification unit. New core
behavior should come with deterministic protocol tests. New harness behavior
should state its durable commit point, restart behavior, and treatment of
unknown external state.

Before opening a pull request, run `make check`, `go test -count=1 -race ./...`,
and `git diff --check`, plus `make integration` when a change touches storage,
execution, the model adapter, or the CLI. The CI runs the checks and the
integration tests on pushes to `main` and on pull requests.

Read [CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md) for ownership,
verification, and review rules. Report vulnerabilities using [SECURITY.md](SECURITY.md).

## License

Copyright 2026 ChrAlpha. Licensed under [Apache-2.0](LICENSE).
