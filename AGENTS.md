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
  standard library. Its events name recovery semantics, not capabilities: a
  capability, such as retrieval or handover, adds no event kind or core
  rule, and a new event kind needs a recovery semantic that no existing
  event expresses. The core never interprets an exec payload, an archive
  query, or a context window.
- `internal/harness/` owns durable facts, input construction, model calls,
  persistence, execution, and the interpretation of intents. It must not
  restate transition rules that `core.State.Next` defines. It reads an
  accepted exec decision once into a normalized action with the recovery
  contract its trusted implementation guarantees, a repeatable read, a local
  change, or an external effect, commits it with the acceptance, and routes,
  performs, cancels, and recovers by that action, never by payload prefixes
  or physical owner IDs. Its concrete
  adapters are `sqlite`, `docker`, and the model adapters `anthropic`
  (Messages) and `openai` (Responses), which share `provider`; storage,
  execution, and provider guarantees require the integration evidence
  recorded in `docs/v1-acceptance.md`. Model APIs are peers: no default
  API, provider, or model, and no Chat Completions adapter.
- `internal/archive/` owns read-only access to the archive that jobs see:
  path safety, the retrieval operations, their continuations, and the
  layout and meta encoding of published step records. It imports only the
  standard library and knows no action protocol; the harness supplies what
  discovery skips.
- `internal/host/` owns the runtime host of one state directory: its lock
  and journal for the host's lifetime, admission and input identity, the
  drives of runs, stop, resolution, shutdown, and the progress projection
  that readers page and wait on. It holds no run fact of its own, and what it
  keeps in memory ends with each drive. It owns the configuration a run is
  created and rebuilt with: its stored config and provider, the context
  version, and the construction of its loop, model adapter, and executor. It
  reads no environment: a credential is given to each drive, or to a
  foreground host for an exact model API and provider, and never stored.
- `internal/hostrpc/` owns the host protocol: the Unix socket, the wire
  messages, which embed no journal, core, or host type and are pinned per
  protocol version in `testdata/v<N>`, and the bounds of a connection. A
  change to a message needs the next `Protocol` and its own record, which
  `-record-wire` writes once and never rewrites; the records of earlier
  versions stay as recorded. The protocol's words for host enumerations are
  its own tables, and its sample messages use fixed version values, so a
  context or semantics version change moves no wire record.
- `cmd/canti/` owns flags, exit codes, process signals, the terminal, the
  environment, and the composition of the packages. Run rules, admission, and context fit belong
  to `core` and `internal/harness`. A command checks its flags, the layout of
  the workspace and state directories, and the credential before it creates
  the state directory.
- Keep one module. Add interfaces at external boundaries; start other behavior
  with concrete types. No plugin registry, generic workflow engine, or Memoh
  internal package dependency is needed for this experiment.

## Runtime invariants

Retry, termination, and cancellation rules live in `core.State.Next`; change
them there with a transition-table test. A harness policy, such as a budget,
the context window, or handover, ends a run only by committing
`StopRequested` with its reason and a `Failure` that names the cause. The
transitions a bounded exploration from `core.NewState` reaches are recorded
per semantics version in `core/testdata/semantics-v<N>.txt`, so a change to
any of them fails until `core.SemanticsVersion` is bumped and its
transitions are recorded with `-record-semantics`; a rule outside the
explored limits needs its own test. Before changing a harness boundary,
specify its durable commit point, attached facts, and behavior after restart.
Model request bytes and provenance must be committed before sending; a
decision and its normalized action before it is performed or bound; the executor publishes its durable owner binding before consuming its launch
latch; a result together with run progress, and with any
change of harness state it causes, such as the context window, in one
`Transition`. Events carry the step, attempt, or effect they belong to, and
commits are sequenced: changing either identity scheme is a semantics change
and must bump `core.SemanticsVersion`.

The context version (`ContextVersion` in `internal/host/config.go`) names the
rules that render and account for a request. Give the next one when, for the
same committed facts, a rendered byte, a reserve, a cap, an admission floor,
the window or session selection, or the evidence a request shows changes;
`TestContextVersionPinsItsRules` fails until it is recorded. A refactor that
the frozen-request corpus proves byte-identical keeps the version, and a new
version never rewrites frozen bytes.

The system prompt holds the decision protocol and, for each capability, its
syntax and the rules whose violation no error would report. Leave other
details to the result or error of the action that needs them;
`TestRetrievalSystemKeepsSilentRulesWithinItsBudget` bounds the retrieval
section.

`return` hands control back; it does not assert success. An unknown execution
result must remain uncertain until inspected or explicitly resolved. A stable
effect ID does not make arbitrary commands exactly-once.

Keep trusted facts separate from agent-editable notes. Credentials must never
enter fixtures, durable request records, logs, or Git. Do not access `.env`
files. Use structured logging for runtime diagnostics.

## Verification and handoff

Use focused tests while iterating. Before delivery, run:

```sh
make check lint
go test -count=1 -race ./...
git diff --check
make commits BASE=<branch base>
```

Also run `make integration`, which needs a Docker daemon, when a change
touches storage, execution, the model adapter, or the CLI.

Format only changed Go files with `gofmt -w`. `make check` checks formatting
without rewriting files. `make lint` limits the cognitive complexity of
production functions; split a function instead of adding an exception.
`make commits` requires every commit to build and vet. Add regression tests
for behavior changes, in the existing test file of the contract they protect
and named for the behavior; test failure and cancellation paths when
modifying effect or persistence boundaries. Do not weaken tests to make an
implementation pass.

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
