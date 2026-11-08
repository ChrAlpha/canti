#!/bin/sh
# Verify provenance, reconstruct source, and run unchanged upstream suites.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
python3 "$root/scripts/verify-tui-deps.py"
for module in tea uv; do
 (cd "$root/apps/tui/third_party/$module" && GOWORK=off go test -count=1 -race ./...)
done
