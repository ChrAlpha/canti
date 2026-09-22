# Development

## Prerequisites

- Go 1.25 or newer; `go.mod` declares the minimum version.
- Make and a POSIX shell for the check targets.
- A C compiler and a supported platform for Go's race detector.

The current module uses only the Go standard library, so there is no `go.sum`.
No model credentials, `.env` file, database, container, or Memoh service is
needed for the skeleton checks. Run commands in this repository, not its parent
workspace. If a parent `go.work` includes other modules, use `GOWORK=off`.

```sh
git clone https://github.com/ChrAlpha/canti.git
cd canti
make check
```

Cloning requires repository access while the repository is private.

## Commands

| Command | Purpose |
| --- | --- |
| `make check` | Read-only formatting validation, vet, and all unit tests |
| `make fmt` | Check `gofmt` output; does not rewrite files |
| `make vet` | Run `go vet ./...` |
| `make test` | Run `go test ./...` |
| `go test -count=1 -race ./...` | Fresh race-enabled unit test run |
| `go test ./core -run TestDecodeRejectsInvalidProtocol -v` | Focused protocol regression cases |
| `go test ./internal/harness -run TestApplyDecision -v` | Ordering, failure, and cancellation cases |
| `git diff --check` | Check pending patches for whitespace errors |

Use `gofmt -w path/to/changed.go` on changed files to resolve a formatting
failure. Do not run a repository-wide rewrite as part of an unrelated change.

## CLI smoke check

```sh
go run ./cmd/canti -version
go run ./cmd/canti -h
mkdir -p bin
go build -o bin/canti ./cmd/canti
./bin/canti -version
```

`-version` writes `dev` to standard output and exits with status 0. `-h` writes
help and exits with status 0. Invoking the compiled binary with no arguments
prints usage and exits with status 2. `go run` itself reports a non-zero wrapper
status for a failing program, so use the binary to inspect its exact exit code.

There is no command to start a model loop, execution backend, or recovery run
yet. The CLI smoke check proves the command builds and reports its development
version; it is not an end-to-end agent test.

## Where to work

Read [architecture](architecture.md) for interfaces and ownership. Read
[V1 acceptance](v1-acceptance.md) before adding a concrete adapter. Keep tests
beside the package they verify. Add integration fixtures only with the backend
they exercise, not empty placeholder directories or skipped acceptance tests.

Protocol tests should stay deterministic and require no credentials. Backend
tests must use temporary, isolated workspaces and report leftover processes or
artifacts. Live model checks must be explicit opt-ins with finite budgets;
do not make paid provider calls part of default unit tests.

When adding dependencies, use Go modules and commit the resulting `go.sum`.
Record the minimum toolchain or platform change and update CI in the same unit.
Keep large outputs, credentials, databases, and local task state outside Git.

## CI and delivery

The CI workflow runs local checks on pushes to `main` and pull requests. CI
configuration is versioned in `.github/workflows/ci.yml`; workflow success is
separate evidence from checks run locally. Repository hosting settings such as
branch protection, reviewer assignment, and private vulnerability reporting are
not configured by committing workflow files.

Before delivery, inspect the diff, run the relevant tests and documented
checks, and record the actual outcomes. Explain unrun checks. Use the
[contribution guide](../CONTRIBUTING.md) and PR template to provide a reviewer
with the problem, resulting behavior, limits, and evidence. A green unit suite
does not establish V1 durability.
