#!/bin/sh
# Run one Go command in every independently buildable module.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
for module in runtime sdk/go apps/tui; do
	if [ ! -f "$root/$module/go.mod" ]; then
		printf 'missing Go module: %s\n' "$module" >&2
		exit 1
	fi
	printf '==> %s: %s\n' "$module" "$*"
	(cd "$root/$module" && GOWORK=off "$@")
done
