#!/usr/bin/env python3
"""Partition go test's discovered top-level tests, keeping each test intact."""

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
    names = set()
    for line in events.splitlines():
        event = json.loads(line)
        output = event.get("Output", "").strip()
        if event.get("Action") == "output" and re.fullmatch(r"(?:Test|Fuzz|Example)\w*", output):
            names.add(output)
    return sorted(names)


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
    if count < 1 or not 0 <= index < count:
        sys.exit("invalid CI shard index/count")
    if balance not in ("", "long"):
        sys.exit("invalid CI shard balance mode")
    if count == 1:
        os.execvp(command[0], command)
    # command is test-report.sh go test -json ...; discovery compiles the same
    # race/tagged packages and cannot silently lose a package that fails to build.
    listing = subprocess.run(command[1:] + ["-list", "."], text=True, stdout=subprocess.PIPE)
    if listing.returncode:
        print(listing.stdout, end="")
        sys.exit(listing.returncode)
    names = selected(test_names(listing.stdout), command[3:], index, count, balance == "long")
    print(f"shard {index + 1}/{count}: {len(names)} top-level tests", flush=True)
    if not names:
        return
    pattern = "^(" + "|".join(names) + ")$"
    os.execvp(command[0], command + ["-run", pattern])


if __name__ == "__main__":
    main()
