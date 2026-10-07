# Working on canti

## Start here

This is an independent Go module, even when checked out inside Memoh. Run all
commands from this repository. No Memoh services, UI, or parent workspace tools
are required. Read `README.md` for current scope and `CONTRIBUTING.md` for the
contribution workflow.

Inspect `git status --short`, relevant code, tests, and `go.mod` before editing.
Preserve unrelated changes. State the affected boundary, expected files, and
verification before a non-trivial change. Keep one coherent intent per commit.

## Ownership and complexity

- `core/` owns the stateless run semantics: decoding an `exec` or `return`
  decision, the `State` projection, and the pure `Next`/`Intent` transition
  rules. It holds no per-run state between calls and must not acquire model
  I/O, history, storage, clocks, execution, or dependencies outside the
  standard library. Adding a capability must not add an event kind.
- `internal/harness/` owns durable facts, input construction, model calls,
  persistence, execution, and the interpretation of intents. It must not
  restate transition rules that `core.State.Next` defines. Its concrete
  adapters are `sqlite`, `docker`, and `anthropic`; storage, execution, and
  provider guarantees require the integration evidence recorded in
  `docs/v1-acceptance.md`.
- `internal/archive/` owns read-only access to the archive that jobs see:
  path safety, the retrieval operations, their continuations, and the
  layout and meta encoding of published step records. It imports only the
  standard library and knows no action protocol; the harness supplies what
  discovery skips.
- `cmd/canti/` is thin CLI wiring. Keep reusable behavior in the Go packages.
- Keep one module. Add interfaces at external boundaries; start other behavior
  with concrete types. No plugin registry, generic workflow engine, or Memoh
  internal package dependency is needed for this experiment.

## Runtime invariants

Retry, termination, and cancellation rules live in `core.State.Next`; change
them there with a transition-table test. The transitions a bounded
exploration from `core.NewState` reaches are recorded per semantics version
in `core/testdata/semantics-v<N>.txt`, so a change to any of them fails
until `core.SemanticsVersion` is bumped and its transitions are recorded
with `-record-semantics`; a rule outside the explored limits needs its own
test. Before changing a harness boundary, specify its durable commit point,
attached facts, and behavior after restart.
Model request bytes and provenance must be committed before sending; decisions
and effects before binding; the bound job before starting; observations
together with run progress in one `Transition`. Events carry the step,
attempt, or effect they belong to, and commits are sequenced: changing either
identity scheme is a semantics change and must bump `core.SemanticsVersion`.

`return` hands control back; it does not assert success. An unknown execution
result must remain uncertain until inspected or explicitly resolved. A stable
effect ID does not make arbitrary commands exactly-once.

Keep trusted facts separate from agent-editable notes. Credentials must never
enter fixtures, durable request records, logs, or Git. Do not access `.env`
files. Use structured logging for runtime diagnostics.

## Verification and handoff

Use focused tests while iterating. Before delivery, run:

```sh
make check
go test -count=1 -race ./...
git diff --check
```

Also run `make integration`, which needs a Docker daemon, when a change
touches storage, execution, the model adapter, or the CLI.

Format only changed Go files with `gofmt -w`. `make check` checks formatting
without rewriting files. Add regression tests for behavior changes; test
failure and cancellation paths when modifying effect or persistence boundaries.
Do not weaken tests to make an implementation pass.

Report the commands and observed results. Distinguish offline fixtures, real
storage or executor integration, and live model runs. State what remains
unimplemented. Keep restart, failure injection, and resource limits explicit
when adding concrete adapters.

For extended work, leave a concise handoff with the objective, current commit,
pending files, checks, and next step in local task state outside tracked source.
Review the final diff for defects and missing evidence, independently when a
reviewer is available.

Use Conventional Commits. Do not skip hooks, alter Git signing settings, add AI
co-authorship metadata, or use destructive Git operations without authorization.
Obtain the user's explicit approval before posting issues, PRs, reviews, or
messages on their behalf. Repository guidance does not authorize publication.
