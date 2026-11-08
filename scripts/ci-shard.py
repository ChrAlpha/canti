#!/usr/bin/env python3
"""Partition go test's discovered top-level tests, keeping each test intact."""

import hashlib
import json
import os
import re
import subprocess
import sys


def test_names(events):
    names = set()
    for line in events.splitlines():
        event = json.loads(line)
        output = event.get("Output", "").strip()
        if event.get("Action") == "output" and re.fullmatch(r"(?:Test|Fuzz|Example)\w*", output):
            names.add(output)
    return sorted(names)


def selected(names, args, index, count):
    run, skip = ".", "^$"
    for offset, arg in enumerate(args):
        if arg == "-run":
            run = args[offset + 1]
        elif arg == "-skip":
            skip = args[offset + 1]
    return [name for name in names if re.search(run, name) and not re.search(skip, name)
            and int.from_bytes(hashlib.sha256(name.encode()).digest()[:8], "big") % count == index]


def main():
    command = sys.argv[1:]
    count = int(os.environ.get("CI_SHARD_COUNT", "1"))
    index = int(os.environ.get("CI_SHARD_INDEX", "0"))
    if count < 1 or not 0 <= index < count:
        sys.exit("invalid CI shard index/count")
    if count == 1:
        os.execvp(command[0], command)
    # command is test-report.sh go test -json ...; discovery compiles the same
    # race/tagged packages and cannot silently lose a package that fails to build.
    listing = subprocess.run(command[1:] + ["-list", "."], text=True, stdout=subprocess.PIPE)
    if listing.returncode:
        print(listing.stdout, end="")
        sys.exit(listing.returncode)
    names = selected(test_names(listing.stdout), command[3:], index, count)
    print(f"shard {index + 1}/{count}: {len(names)} top-level tests", flush=True)
    if not names:
        return
    pattern = "^(" + "|".join(names) + ")$"
    os.execvp(command[0], command + ["-run", pattern])


if __name__ == "__main__":
    main()
