#!/bin/sh
# Exercise the two installed command surfaces without creating state.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

(cd "$root/runtime" && GOWORK=off go build -buildvcs=false -o "$tmp/canti" ./cmd/canti)
(cd "$root/apps/tui" && GOWORK=off go build -buildvcs=false -o "$tmp/canti-tui" ./cmd/canti-tui)

for exe in canti canti-tui; do
	version=$("$tmp/$exe" -version)
	if [ "$version" != dev ]; then
		printf '%s -version: expected dev, got %s\n' "$exe" "$version" >&2
		exit 1
	fi
	help=$("$tmp/$exe" help)
	case $help in
		*"Usage:"*"$exe "*) ;;
		*) printf '%s help: missing usage\n' "$exe" >&2; exit 1 ;;
	esac
	printf '%s version/help: PASS\n' "$exe"
done
