#!/bin/sh
# Usage: scripts/check-commits.sh BASE, or BASE=origin/main make commits
set -eu

base=${1:?usage: scripts/check-commits.sh BASE}
commits=$(git rev-list --reverse "$base..HEAD")
tree=$(mktemp -d)
output=$(mktemp -d)
trap 'git worktree remove --force "$tree" >/dev/null 2>&1 || rm -rf "$tree"; rm -rf "$output"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
git worktree add -q --detach "$tree" HEAD
status=0
for commit in $commits; do
	git -C "$tree" checkout -q --detach "$commit"
	if [ -f "$tree/go.work" ]; then
		if ! (cd "$tree" && scripts/build-modules.sh && scripts/run-modules.sh go vet ./... && scripts/run-modules.sh go vet -tags integration,live ./... && (cd apps/tui && GOWORK=off go vet -tags acceptance,integration ./...)); then
			printf '%s does not build and vet\n' "$(git log -1 --format='%h %s' "$commit")" >&2
			status=1
		fi
	else
		if ! (cd "$tree" && go build -buildvcs=false -o "$output" ./... && go vet ./... && go vet -tags integration,live ./...); then
			printf '%s does not build and vet\n' "$(git log -1 --format='%h %s' "$commit")" >&2
			status=1
		fi
	fi
done
exit $status
