# Contributing to canti

canti explores how a stateless decision protocol and a recoverable harness can
compose the right information for each agent step. It is experimental: protocol
and harness APIs may change before a stable release.

## Local setup

Use Go 1.25 or newer, Make, and a POSIX shell. Current checks need no credentials,
model access, database, container runtime, or Memoh checkout.

```sh
make check
go test -race ./...
go run ./cmd/canti -version
```

`make check` validates formatting, runs `go vet`, and runs the tests. See
[AGENTS.md](AGENTS.md) for package ownership and agent contribution rules.

## Changes and review

Keep one behavior or boundary per change. Before a non-trivial feature, describe
its module owner, observable contract, test strategy, and complexity budget in
the issue or PR. A small fix can explain these in its description. New external
dependencies should have a concrete purpose and a compatible license.

1. Work on a focused branch from `main` and preserve unrelated changes.
2. Add a regression test for changed behavior when practical. Core protocol
   tests must be deterministic. Harness tests should exercise affected commit,
   cancellation, unknown-result, and restart boundaries.
3. Format changed Go files with `gofmt -w`. Run relevant tests during iteration,
   then `make check`, `go test -race ./...`, and `git diff --check` for delivery.
4. Inspect the diff and use a Conventional Commit subject such as
   `fix(core): reject duplicate decision fields`.
5. Fill in the PR template with the problem, resulting behavior, commands and
   outcomes, and remaining limits. Include failure or recovery evidence for
   affected boundaries.

An in-memory store establishes call ordering, not durable transactions. A mock
executor does not prove restart recovery or exactly-once effects. Label evidence
as offline, concrete backend integration, or live provider verification. Record
the code revision and relevant provider, protocol, adapter, and run configuration
for live experiments; specify finite concurrency, cost, token, and time budgets.

Agent-assisted contributions use the same review bar as other contributions.
The submitter owns the change and verification claims. Disclose material agent
assistance in the PR and distinguish automated checks from human review.

## Issues and conduct

Use the bug template for reproducible defects and the feature template for a
proposed boundary or acceptance property. Include a commit or version and remove
private data from logs and examples. Public issue and PR prose can be in English
or Chinese; keep repository guidance and templates in English.

Keep discussion respectful, focused on the work, and specific about evidence.
For vulnerabilities, follow [SECURITY.md](SECURITY.md).

## License

Contributions intentionally submitted for inclusion are licensed under
[Apache-2.0](LICENSE), subject to its contribution terms. Preserve applicable
third-party copyright and license notices.
