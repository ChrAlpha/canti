# Native terminal dependency replacements

`tea` is Bubble Tea v2.1.1; `uv` is the ultraviolet revision pinned by that
release. `manifest.json` records exact upstream commits, module checksums,
licenses, changed files and the patch checksum. Original MIT licenses and the
upstream tests are retained. These replacements are dependencies, not workspace
product modules. Go 1.26 is required.

`native-input.patch` is the reproducible difference from the upstream module
archives (excluding upstream GitHub automation). `scripts/check-tui-deps.sh`
verifies module/revision/checksum metadata and negative provenance cases,
reconstructs both trees, compares every file and runs both upstream suites with
the race detector. Application integration regressions live in
`apps/tui/internal/terminal/native_linux_test.go`.

The patch adds generic native input limits, bounded paste overflow recovery,
cancelable event delivery, EOF/error messages, and cleanup after partial program
initialization. No session or editor policy belongs here. Paste storage and
pending sequences have separate byte limits; recovery after paste overflow
consumes at most another 64 KiB and lasts at most two seconds. Unterminated
control strings (configured by Tea through UV `IncompleteTimeout`) and inactive
unfinished pastes also terminate after two seconds. UV callers that leave
`IncompleteTimeout` zero retain the upstream escape ambiguity timing.
`InputClosedMsg.Err` is nil for clean EOF, otherwise the native input error;
the application decides stop versus detach. Input is read exclusively by Tea.

For an update, obtain the exact upstream module archives, apply and review these
changes, keep upstream tests unchanged, regenerate the patch and manifest, then
run the native regressions and serial PTY lane. Do not edit the module cache.
