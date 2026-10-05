#!/usr/bin/env bash
# Usage: scripts/test-report.sh go test -json ... (TEST_JSON names the saved event file)
set -uo pipefail

json=${TEST_JSON:-$(mktemp)}
"$@" | tee "$json" | jq -Rrj --unbuffered '
	(fromjson? // {Action: "raw", Output: (. + "\n")})
	| if .Action == "raw" or .Action == "build-output" then .Output
	elif .Test == null and (.Action == "pass" or .Action == "fail" or .Action == "skip") then "\(.Action)\t\(.Package)\t\(.Elapsed)s\n"
	elif .Action == "fail" and (.Test | contains("/") | not) then "FAIL\t\(.Package)\t\(.Test)\n"
	else empty end'
status=$?

jq -Rrs '
	[split("\n")[] | fromjson? | select(type == "object")] as $events
	| [$events[] | select(.Test != null and (.Test | contains("/") | not) and (.Action == "pass" or .Action == "fail" or .Action == "skip"))] as $tests
	| ($tests | map(select(.Action == "fail"))) as $failed
	| [$events[] | select(.Action == "run" and (.Test | contains("/") | not)) | . as $r
		| select($tests | any(.Package == $r.Package and .Test == $r.Test) | not)] as $unfinished
	| ((($failed | map(.state = "failed")) + ($unfinished | map(.state = "unfinished")))[] as $f
		| "\n=== Output of \($f.state) \($f.Package) \($f.Test)\n"
		+ ([$events[] | select(.Action == "output" and .Package == $f.Package and .Test != null and (.Test == $f.Test or (.Test | startswith($f.Test + "/")))) | .Output] | add // "")),
	($events[] | select(.Action == "fail" and .Test == null) | . as $p
		| "\n=== Output of failed package \($p.Package)\n"
		+ ([$events[] | select(.Action == "output" and .Package == $p.Package and .Test == null) | .Output] | add // "")),
	"\n=== \($tests | length) top-level tests: \($tests | map(select(.Action == "pass")) | length) passed, \($tests | map(select(.Action == "skip")) | length) skipped, \($failed | length) failed, \($unfinished | length) unfinished; slowest 20:",
	($tests | sort_by(-.Elapsed) | .[:20][] | "\(.Elapsed)s\t\(.Action)\t\(.Package)\t\(.Test)")
' "$json"
exit "$status"
