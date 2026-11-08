#!/bin/sh
# Verify the pinned native input replacements against their upstream archives.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
source="$root/apps/tui/third_party"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
copy_module() {
 dir=$(GOWORK=off go mod download -json "$2@$3" | sed -n 's/.*"Dir": "\([^"]*\)".*/\1/p')
 test -n "$dir"
 mkdir "$tmp/$1"
 cp -R "$dir/." "$tmp/$1/"
 chmod -R u+w "$tmp/$1"
 rm -rf "$tmp/$1/.github"
}
copy_module tea charm.land/bubbletea/v2 v2.1.1
copy_module uv github.com/charmbracelet/ultraviolet v0.0.0-20261008173134-6b8d4baf91b4
(cd "$tmp" && patch -s -p1 < "$source/native-input.patch")
diff -qr "$tmp/tea" "$source/tea"
diff -qr "$tmp/uv" "$source/uv"
for module in tea uv; do
 (cd "$source/$module" && GOWORK=off go test -count=1 -race ./...)
done
