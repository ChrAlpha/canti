#!/bin/sh
# Exercise the two installed command surfaces without creating state.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output=${1:-}
if [ -z "$output" ]; then
	output=$(mktemp -d)
	trap 'rm -rf "$output"' EXIT
	mkdir -p "$output/runtime" "$output/apps/tui"
	(cd "$root/runtime" && GOWORK=off go build -buildvcs=false -o "$output/runtime/canti" ./cmd/canti)
	(cd "$root/apps/tui" && GOWORK=off go build -buildvcs=false -o "$output/apps/tui/canti-tui" ./cmd/canti-tui)
fi

for path in runtime/canti apps/tui/canti-tui; do
	exe=${path##*/}
	version=$("$output/$path" -version)
	if [ "$version" != dev ]; then
		printf '%s -version: expected dev, got %s\n' "$exe" "$version" >&2
		exit 1
	fi
	help=$("$output/$path" help)
	case $help in
		*"Usage:"*"$exe "*) ;;
		*) printf '%s help: missing usage\n' "$exe" >&2; exit 1 ;;
	esac
	printf '%s version/help: PASS\n' "$exe"
done
