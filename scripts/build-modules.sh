#!/bin/sh
# Build every module into disposable output, including its main packages.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output=$(mktemp -d)
trap 'rm -rf "$output"' EXIT

for module in runtime sdk/go apps/tui; do
	if [ ! -f "$root/$module/go.mod" ]; then
		printf 'missing Go module: %s\n' "$module" >&2
		exit 1
	fi
	mkdir -p "$output/$module"
	printf '==> %s: go build ./...\n' "$module"
	if [ -n "$(cd "$root/$module" && GOWORK=off go list -f '{{if eq .Name "main"}}{{.ImportPath}}{{end}}' ./... | sed '/^$/d')" ]; then
		(cd "$root/$module" && GOWORK=off go build -buildvcs=false -o "$output/$module" ./...)
	else
		(cd "$root/$module" && GOWORK=off go build -buildvcs=false ./...)
	fi
done
