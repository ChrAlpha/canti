## Summary

<!-- Describe the behavior or boundary this change adds or changes. -->

## Scope

- [ ] `core` protocol
- [ ] harness boundary
- [ ] concrete storage or execution backend
- [ ] tests or tooling

## Verification

- [ ] `make check`
- [ ] Additional checks are listed below

<!-- Include commands and relevant failure/recovery cases. -->

## Review constraints

- [ ] Core changes remain stateless and free of external I/O.
- [ ] Durable facts are committed before external effects are dispatched.
- [ ] Unknown execution state is preserved rather than treated as success.
- [ ] No credentials, generated artifacts, or unrelated refactors are included.
