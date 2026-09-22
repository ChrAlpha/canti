## Summary

<!-- Describe the behavior or boundary this change adds or changes. -->

## Agent disclosure

<!-- Identify whether this is submitted by a human or an agent. Describe any
material agent assistance, or state that none was used. -->

## Human review

- [ ] A human reviewed the current diff and verification evidence.

<!-- Leave unchecked unless explicitly confirmed. Agent work and automated
checks do not count as human review. -->

## Scope

- [ ] `core` protocol
- [ ] harness boundary
- [ ] concrete storage or execution backend
- [ ] tests or tooling
- [ ] documentation or collaboration

## Verification

- [ ] `make check`
- [ ] `go test -race ./...`
- [ ] `git diff --check`
- [ ] Additional checks are listed below

<!-- Include commands, observed results, and relevant failure/recovery cases.
State checks not run. For runtime changes, describe the durable commit point,
restart behavior, cancellation, and unknown-result handling. -->

## Review constraints

- [ ] Core changes remain stateless and free of external I/O.
- [ ] Durable facts are committed before external effects are dispatched.
- [ ] Unknown execution state is preserved rather than treated as success.
- [ ] No credentials, generated artifacts, or unrelated refactors are included.
- [ ] Remaining skeleton or backend limits are stated explicitly.
