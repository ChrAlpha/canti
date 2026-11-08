#!/bin/sh
# Root owns cross-executable orchestration; module-local tests build no siblings.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
(cd "$root/runtime" && GOWORK=off go build -race -o "$tmp/canti" ./cmd/canti)
(cd "$root/runtime" && GOWORK=off go test -c -race -tags integration -o "$tmp/canti-fixture" ./cmd/canti)
(cd "$root/apps/tui" && GOWORK=off go build -race -o "$tmp/canti-tui" ./cmd/canti-tui)
export CANTI_ACCEPTANCE_RUNTIME="$tmp/canti"
export CANTI_ACCEPTANCE_RUNTIME_FIXTURE="$tmp/canti-fixture"
export CANTI_ACCEPTANCE_TUI="$tmp/canti-tui"
cd "$root/apps/tui"
if [ "${ACCEPTANCE_REPORT:-0}" = 1 ]; then
 GOWORK=off "$root/scripts/test-report.sh" go test -json -count=1 -race -tags "${ACCEPTANCE_TAGS:-acceptance}" "$@" ./acceptance
else
 GOWORK=off go test -count=1 -race -tags "${ACCEPTANCE_TAGS:-acceptance}" "$@" ./acceptance
fi
