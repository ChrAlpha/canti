---
name: Bug report
about: Report a reproducible defect in the protocol, harness, or tooling
title: ""
labels: "bug"
assignees: ""
---

## What happened?

Describe the observed behavior and the behavior you expected.

## Reproduction

List the smallest command, test, input, or sequence that reproduces the issue.
Remove credentials, tokens, private data, and unrelated workspace content.

```text
command or input
```

## Scope

- [ ] `core` protocol
- [ ] harness boundary
- [ ] concrete storage or execution backend
- [ ] CLI or CI tooling

## Environment

- Commit or tag:
- Go version:
- OS and architecture:
- Backend or adapter versions, if applicable:

## Evidence

Include relevant logs, test output, run/step/effect IDs, and whether the result
comes from an offline fixture, a concrete backend, or a live provider. Mark any
unrun check instead of implying it passed.

## Additional context

Add failure, cancellation, recovery, or resource-limit context that may change
the diagnosis. For security-sensitive reports, do not use this template; follow
the reporting guidance in [SECURITY.md](https://github.com/ChrAlpha/canti/blob/main/SECURITY.md).
