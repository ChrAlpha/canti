# canti

`canti` is named after *canti*, the plural of *canto*, and focuses on composing
the right information for each agent step.

This repository contains a stateless Go decision core and the boundaries for a
durable single-machine harness. It is an architectural experiment for a future
Memoh agent runtime. **The current implementation is a skeleton, not a complete
durable agent.**

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

## Current repository

The `core` package is a stateless semantic kernel:

- `core.Decode` accepts exactly one complete JSON decision; the only decisions
  are `exec` and `return`.
- `core.State` is the complete projection needed to continue a run: run, step,
  attempt, request revision, phase, limits, last decision, effect and job
  identity, committed start, stop reason. `State.Validate` rejects projections
  no legal run reaches.
- `State.Intent` names the single action the harness may take next; closed and
  unknown phases halt.
- `State.Next` applies one reported fact and returns the new state, or the
  unchanged state with an error for invalid or illegal events. Events name the
  step, attempt, or effect they belong to; a result from an earlier position
  is invalid, not late.
- Binding a backend job and authorizing its start are separate committed
  phases, so an unstarted bind can never execute.
- It owns no model client, history, session, filesystem, clock, or executor,
  and imports only the standard library.

The `internal/harness` package interprets those intents:

- `Runner.Commit` derives the next state and hands one `Transition` with its
  attached facts to the `Store` at the next sequence of a `Snapshot`, with a
  deterministic event ID so an identical retry after a lost acknowledgement
  is confirmed, not repeated, and a stale controller cannot commit.
- `Runner.Bind` runs after the effect is committed and records the job that
  owns it; `Runner.Start` runs after the binding is committed and records
  that the start was issued; anything uncertain becomes an unknown outcome.
- `Model`, `Executor`, `Frame`, and `Observation` define the remaining
  boundaries as interfaces and record types.

Every I/O follows the committed phase that authorizes it, so a restart resumes
from the last committed state by asking the core for its intent again. The
recovery and fresh-step paths share the same transition code.

SQLite storage, a concrete model adapter, a queryable execution backend, and
the loop that drives intents are not implemented. `return` means control was
handed back; it does not claim that the task succeeded.

## Development

Requirements:

- Go 1.25 or newer
- Make and a POSIX shell for the Makefile targets

Run the full local check:

```sh
make check
```

The check runs:

1. `gofmt` validation for all Go files;
2. `go vet ./...`;
3. `go test ./...`.

The command entry point currently exposes only a version/help surface:

```sh
go run ./cmd/canti -version
```

There is no runnable model loop yet. See the [development guide](docs/development.md)
for build, race-check, and CLI smoke commands.

## Design rules

1. The core stays stateless and free of external I/O.
2. The harness persists facts before dispatching external effects.
3. A timeout or unknown external result is not silently treated as success or
   as permission to run the effect again.
4. Interfaces are added at external boundaries; local implementations should
   remain concrete until a second implementation needs the abstraction.
5. A `return` decision is a control-flow result, not a business-success flag.
6. The execution state of a process is kept separate from the business effect
   that process may have caused.
7. Run rules live in `core.State.Next`. The harness executes intents and
   never restates a transition; adding a capability adds no event kind.

## Initial scope

The first useful integration target is one workspace with one writer, serial
decisions, bounded model input, restart recovery, and fault-injection tests.
The harness should be able to distinguish recoverable states from
`needs_resolution` without guessing whether an uncertain external action ran.

Distributed scheduling, multi-tenant policy, generic workflow graphs,
automatic planning, and automatic memory compaction are outside this initial
scope. They can be added after the explicit commit boundaries have been tested
with a concrete store and executor.

## Contribution shape

Keep changes focused on one boundary or one verification unit. New core
behavior should come with deterministic protocol tests. New harness behavior
should state its durable commit point, restart behavior, and treatment of
unknown external state.

Before opening a pull request, run `make check` and describe any additional
fault or recovery cases exercised. The repository CI runs the same command on
pushes to `main` and on pull requests.

Read [CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md) for ownership,
verification, and review rules. Report vulnerabilities using [SECURITY.md](SECURITY.md).

## License

Copyright 2026 ChrAlpha. Licensed under [Apache-2.0](LICENSE).
