#!/bin/sh
# Check production and test dependency boundaries, independent of go.work.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ ! -f "$root/go.work" ]; then
	printf 'missing root go.work\n' >&2
	exit 1
fi
workspace_modules=$(cd "$root" && GOWORK="$root/go.work" go list -m | LC_ALL=C sort)
expected_modules=$(printf '%s\n' github.com/ChrAlpha/canti/apps/tui github.com/ChrAlpha/canti/runtime github.com/ChrAlpha/canti/sdk/go)
if [ "$workspace_modules" != "$expected_modules" ]; then
	printf 'go.work must use exactly runtime, sdk/go and apps/tui\n' >&2
	exit 1
fi
for module in runtime sdk/go apps/tui; do
	if [ ! -f "$root/$module/go.mod" ]; then
		printf 'missing Go module: %s\n' "$module" >&2
		exit 1
	fi
done

for module in runtime sdk/go apps/tui; do
	actual=$(cd "$root/$module" && GOWORK=off go list -m)
	if [ "$actual" != "github.com/ChrAlpha/canti/$module" ]; then
		printf 'unexpected module path in %s: %s\n' "$module" "$actual" >&2
		exit 1
	fi
done

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/sdk" "$tmp/apps"
for tags in default acceptance,integration,live; do
	for module in sdk/go apps/tui runtime; do
		if [ "$tags" = default ]; then
			(cd "$root/$module" && GOWORK=off go list -deps -test -f '{{.ImportPath}}' ./...) >"$tmp/$module-$tags"
		else
			(cd "$root/$module" && GOWORK=off go list -deps -test -tags "$tags" -f '{{.ImportPath}}' ./...) >"$tmp/$module-$tags"
		fi
	done
	if grep -E '^github\.com/ChrAlpha/canti/(runtime|apps)(/|$)' "$tmp/sdk/go-$tags"; then
		printf 'SDK %s graph imports runtime or apps\n' "$tags" >&2
		exit 1
	fi
	if grep -E '^github\.com/ChrAlpha/canti/apps(/|$)' "$tmp/runtime-$tags"; then
		printf 'runtime %s graph imports apps\n' "$tags" >&2
		exit 1
	fi
	if grep -E '^github\.com/ChrAlpha/canti/runtime(/|$)|^(modernc\.org/sqlite|github\.com/mattn/go-sqlite3|github\.com/glebarez/sqlite|gorm\.io/driver/sqlite)(/|$)' "$tmp/apps/tui-$tags"; then
		printf 'TUI %s graph imports runtime or SQLite\n' "$tags" >&2
		exit 1
	fi
	if grep -E '^(charm\.land/|github\.com/charmbracelet/|github\.com/creack/pty(/|$)|github\.com/mattn/go-isatty(/|$)|github\.com/muesli/cancelreader(/|$)|github\.com/atotto/clipboard(/|$)|github\.com/xo/terminfo(/|$)|golang\.org/x/term(/|$))' "$tmp/runtime-$tags"; then
		printf 'runtime %s graph imports terminal UI dependencies\n' "$tags" >&2
		exit 1
	fi
done
