#!/usr/bin/env python3
"""Partition go test's discovered top-level tests, keeping each test intact.

A lane that owns only the tests a build tag adds names that tag in
CI_SHARD_ADDED_BY; discovery then also lists the build without it.
"""

import hashlib
import json
import os
import re
import subprocess
import sys


# Seconds observed in run 42454407939. These are scheduling estimates only:
# discovery and the suite's run/skip filters remain the source of membership.
LONG_SECONDS = {
    "TestContextCaptureBoundsRejectedRetrievalAcrossReopen": 35.50,
    "TestContextDiagnosticAcrossExecutionRetrievalHandoverAndRestart": 18.67,
    "TestDiscoveryCandidatesSurviveNoiseAndBinaryAcrossWindows": 84.47,
    "TestLoopBoundsRequestsAsHistoryGrows": 50.23,
    "TestNativeDiscoveryOwnRunAcrossRestartsAndWindows": 25.13,
    "TestRetrievalDiscoveryAcrossFreshPages": 100.0,
    "TestLineDialogueSurvivesHandoverRetryAndRestart": 22.74,
    "TestLineMultiRunWindowStress": 35.0,
    "TestLineUnknownResolutionPreservesTheRunningEffect": 70.0,
    "TestUnlimitedStepsAcrossCrashHandoverAndRestart": 49.75,
}


def test_names(events):
    """The (package, name) of every top-level test, fuzz target and example listed."""
    names = set()
    for line in events.splitlines():
        event = json.loads(line)
        output = event.get("Output", "").strip()
        if event.get("Action") == "output" and re.fullmatch(r"(?:Test|Fuzz|Example)\w*", output):
            names.add((event.get("Package", ""), output))
    return names


def added_names(tagged, base):
    # Compare within each package. -run cannot name a package, so an added
    # name that a base test elsewhere shares would run that test twice.
    added = {name for _, name in tagged - base}
    shared = sorted(added & {name for _, name in base})
    if shared:
        sys.exit("rename tests the tag adds that share a name with a default test: " + ", ".join(shared))
    return sorted(added)


def without_tag(args, tag):
    if "-tags" not in args:
        sys.exit(f"CI_SHARD_ADDED_BY={tag} needs a -tags argument")
    at = args.index("-tags")
    tags = args[at + 1].split(",")
    if tag not in tags:
        sys.exit(f"CI_SHARD_ADDED_BY={tag} is not among -tags {args[at + 1]}")
    # An explicit, possibly empty, -tags overrides any tags GOFLAGS adds.
    rest = [name for name in tags if name != tag]
    return args[:at] + ["-tags", ",".join(rest)] + args[at + 2:]


def discover(command):
    # Discovery compiles the same race/tagged packages and cannot silently lose
    # a package that fails to build.
    listing = subprocess.run(command + ["-list", "."], text=True, stdout=subprocess.PIPE)
    if listing.returncode:
        print(listing.stdout, end="")
        sys.exit(listing.returncode)
    return test_names(listing.stdout)


def selected(names, args, index, count, balance=False):
    run, skip = ".", "^$"
    for offset, arg in enumerate(args):
        if arg == "-run":
            run = args[offset + 1]
        elif arg == "-skip":
            skip = args[offset + 1]
    eligible = [name for name in names if re.search(run, name) and not re.search(skip, name)]
    if not balance:
        return [name for name in eligible
                if int.from_bytes(hashlib.sha256(name.encode()).digest()[:8], "big") % count == index]
    # Longest first, then place each complete test on the least loaded shard.
    # Unknown tests use the fallback estimate and are always included.
    def weight(name):
        return LONG_SECONDS.get(name, 40.0)

    loads = [0.0] * count
    result = []
    for name in sorted(eligible, key=lambda name: (-weight(name), name)):
        owner = min(range(count), key=lambda shard: (loads[shard], shard))
        loads[owner] += weight(name)
        if owner == index:
            result.append(name)
    return sorted(result)


def main():
    command = sys.argv[1:]
    count = int(os.environ.get("CI_SHARD_COUNT", "1"))
    index = int(os.environ.get("CI_SHARD_INDEX", "0"))
    balance = os.environ.get("CI_SHARD_BALANCE", "")
    added = os.environ.get("CI_SHARD_ADDED_BY", "")
    if count < 1 or not 0 <= index < count:
        sys.exit("invalid CI shard index/count")
    if balance not in ("", "long"):
        sys.exit("invalid CI shard balance mode")
    if count == 1 and not added:
        os.execvp(command[0], command)
    # command is test-report.sh go test -json ...
    tagged = discover(command[1:])
    if added:
        names = added_names(tagged, discover(without_tag(command[1:], added)))
    else:
        names = sorted({name for _, name in tagged})
    names = selected(names, command[3:], index, count, balance == "long")
    print(f"shard {index + 1}/{count}: {len(names)} top-level tests", flush=True)
    if not names:
        return
    pattern = "^(" + "|".join(names) + ")$"
    os.execvp(command[0], command + ["-run", pattern])


if __name__ == "__main__":
    main()
