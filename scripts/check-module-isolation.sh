#!/bin/sh
# Runtime and SDK must compile and run default tests without a TUI checkout.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
(cd "$root" && tar --exclude='.env*' -cf - runtime sdk) | (cd "$tmp" && tar -xf -)
for module in sdk/go runtime; do
 (cd "$tmp/$module" && GOWORK=off go build ./... && GOWORK=off go test -count=1 ./...)
done
