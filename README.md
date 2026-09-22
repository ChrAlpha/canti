# canti

`canti` is named after the plural of *canto*: the project explores how to
compose the right information for each agent step.

This repository is an experimental Go implementation of a deliberately thin
agent core and a durable single-machine harness. It is an architectural probe
for a future Memoh agent runtime, not a drop-in replacement for one.

## Why two layers?

The project keeps the smallest possible decision semantics separate from the
long-running work required to make an agent useful and recoverable.

```text
model adapter -> harness input / facts -> core.Decode -> harness effect manager
```

The core interprets one complete model response. The harness owns model I/O,
history, persistence, execution, cancellation, recovery, and the boundaries
where external side effects become observable.

## Current repository

The `core` package is a pure protocol kernel:

- `core.Decode` accepts exactly one complete JSON decision.
- The only decisions are `exec` and `return`.
- It owns no model client, history, session, filesystem, clock, or executor.
- It rejects unknown fields, duplicate fields, trailing values, and invalid
  decision kinds.

The `internal/harness` package defines the long-running boundaries around that
kernel:

- independent model requests and raw response provenance;
- durable fact records and bounded input frames;
- stable effect identities and an external job backend;
- recovery phases, output references, and explicit commit boundaries.

`Runner.ApplyDecision` demonstrates the essential ordering. It decodes and
persists a decision first. A `return` decision is then persisted as a handoff.
An `exec` decision is persisted as an effect before the executor receives it.
The same effect ID can therefore be used by a concrete executor to look up an
existing job after a harness restart.

This is a boundary skeleton. SQLite storage, a concrete model adapter, and a
queryable execution backend are intentionally separate follow-up units.
`return` means control was handed back; it does not claim that the task
succeeded.

## Development

Requirements:

- Go 1.25 or newer
- a POSIX shell for the Makefile targets

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

It intentionally does not pretend that the durable runner is implemented
before its storage and execution contracts have a concrete implementation.

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
