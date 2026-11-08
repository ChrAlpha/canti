#!/bin/sh
# Install the same pinned linter as make lint, without compiling its dependencies.
set -eu

destination=${1:?usage: scripts/install-ci-lint.sh DESTINATION}
archive=golangci-lint-2.10.1-linux-amd64.tar.gz
checksum=dfa775874cf0561b404a02a8f4481fc69b28091da95aa697259820d429b09c99
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl --fail --silent --show-error --location --retry 3 \
	"https://github.com/golangci/golangci-lint/releases/download/v2.10.1/$archive" -o "$tmp/$archive"
printf '%s  %s\n' "$checksum" "$tmp/$archive" | sha256sum --check --status
tar -xzf "$tmp/$archive" -C "$tmp" golangci-lint-2.10.1-linux-amd64/golangci-lint
mkdir -p "$destination"
install -m 755 "$tmp/golangci-lint-2.10.1-linux-amd64/golangci-lint" "$destination/golangci-lint"
