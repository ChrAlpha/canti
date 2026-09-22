---
name: Feature request
about: Propose one focused capability or acceptance property
title: ""
labels: "enhancement"
assignees: ""
---

## Problem and intended behavior

What user or integration problem should this solve? Describe the resulting
observable behavior and a concrete example where useful.

## Boundary owner

- [ ] `core` protocol
- [ ] harness boundary
- [ ] concrete storage or execution backend
- [ ] CLI or CI tooling

Name the package or interface that should own the behavior and explain why it
belongs there. Keep the core free of external I/O and lifecycle state.

## Acceptance evidence

List the smallest deterministic test or script that would demonstrate this
behavior. For durability, recovery, execution, or boundedness, include the
failure or restart case and its expected committed state.

## Constraints and alternatives

State resource limits, compatibility requirements, security boundaries, and
approaches considered. Do not assume that a mock or in-memory fixture proves a
SQLite, execution-backend, or live-provider property.

## Additional context

Link related decisions or documentation. Do not include credentials, private
data, or vulnerability details; use [SECURITY.md](https://github.com/ChrAlpha/canti/blob/main/SECURITY.md) for those.
