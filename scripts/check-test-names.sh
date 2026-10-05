#!/bin/sh
# Usage: make test-names, or LONG='^(TestA|...)$' PTY='^(TestB|...)$' scripts/check-test-names.sh
set -eu

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
status=0
problem() {
	printf '%s\n' "$*" >&2
	status=1
}

listed() {
	go test -list . -tags "$1" ./... >"$tmp/list"
	awk '/^ok[ \t]/ { for (i = 0; i < n; i++) print $2, names[i]; n = 0; next }
		/^(Test|Fuzz|Benchmark|Example)/ { names[n++] = $0 }' "$tmp/list"
}

suite() {
	case $2 in
	'^('*')$') printf '%s\n' "$2" | sed 's/^^(//; s/)\$$//' | tr '|' '\n' ;;
	*) problem "$1 is not an anchored list of test names: $2" ;;
	esac
}

listed integration >"$tmp/integration"
listed integration,live | cut -d' ' -f2 | sort -u >"$tmp/names"
suite LONG "${LONG:-}" >"$tmp/long"
suite PTY "${PTY:-}" >"$tmp/pty"

while read -r name; do
	awk -v n="$name" '$2 == n { found = 1 } END { exit !found }' "$tmp/integration" ||
		problem "LONG: $name is not a test with -tags integration"
done <"$tmp/long"
while read -r name; do
	awk -v n="$name" '$2 == n { if ($1 ~ /\/cmd\/canti$/) found = 1; else other = 1 } END { exit !found || other }' "$tmp/integration" ||
		problem "PTY: $name is not a test of ./cmd/canti alone with -tags integration"
done <"$tmp/pty"
sort "$tmp/long" "$tmp/pty" | uniq -d >"$tmp/both"
while read -r name; do
	problem "$name is in both LONG and PTY"
done <"$tmp/both"

go list -tags integration,live -f '{{.Dir}}{{range .TestGoFiles}} {{.}}{{end}}{{range .XTestGoFiles}} {{.}}{{end}}' ./... |
	while read -r dir files; do
		[ -n "$files" ] || continue
		(cd "$dir" && awk '
			/^func / {
				cur = $0
				sub(/^func (\([^)]*\) )?/, "", cur)
				sub(/[^A-Za-z0-9_].*/, "", cur)
				body[cur] = body[cur] " " $0
				next
			}
			/^(}|type |var |const |import )/ { cur = ""; next }
			cur != "" { body[cur] = body[cur] " " $0 }
			END {
				for (f in body) if (body[f] ~ /pty\.(Open|Start)[A-Za-z]*\(|\/dev\/ptmx/) pty[f] = 1
				for (changed = 1; changed;) {
					changed = 0
					for (f in body) {
						if (f in pty) continue
						for (g in pty) if (body[f] ~ ("(^|[^A-Za-z0-9_])" g "([^A-Za-z0-9_]|$)")) {
							pty[f] = 1
							changed = 1
							break
						}
					}
				}
				for (f in pty) if (f ~ /^(Test|Fuzz)[A-Z0-9_]/) print f
			}' $files)
	done | sort >"$tmp/terminal"
sort "$tmp/pty" >"$tmp/pty.sorted"
comm -23 "$tmp/terminal" "$tmp/pty.sorted" >"$tmp/unlisted"
while read -r name; do
	problem "PTY: $name opens a pseudo-terminal but is not listed"
done <"$tmp/unlisted"
comm -13 "$tmp/terminal" "$tmp/pty.sorted" >"$tmp/unopened"
while read -r name; do
	problem "PTY: $name is listed but opens no pseudo-terminal"
done <"$tmp/unopened"

awk '
	/^```/ { fence = !fence; next }
	!fence && /^#+ / {
		level = match($0, /[^#]/) - 1
		if (skip && level <= skip) skip = 0
		if ($0 ~ /[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/) skip = level
	}
	skip { next }
	{
		line = $0
		run = line ~ /(^|[ \t])-run[ \t]/
		while (match(line, /Test[A-Z0-9_][A-Za-z0-9_]*/)) {
			name = substr(line, RSTART, RLENGTH)
			before = RSTART > 1 ? substr(line, RSTART - 1, 1) : ""
			line = substr(line, RSTART + RLENGTH)
			if (before ~ /[A-Za-z0-9_]/) continue
			print FILENAME ":" FNR, (run || substr(line, 1, 1) == "*") ? "prefix" : "exact", name
		}
	}' README.md AGENTS.md CONTRIBUTING.md docs/*.md >"$tmp/cited"
while read -r at kind name; do
	case $kind in
	prefix) grep -q "^$name" "$tmp/names" || problem "$at: no test name starts with $name" ;;
	*) grep -qx "$name" "$tmp/names" || problem "$at: $name is not a test with -tags integration,live" ;;
	esac
done <"$tmp/cited"

exit $status
