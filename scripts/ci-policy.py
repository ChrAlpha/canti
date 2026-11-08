#!/usr/bin/env python3
"""Conservative change selection and the final CI result contract."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import PurePosixPath


def documentation(path):
    return PurePosixPath(path).suffix == ".md" and (
        len(PurePosixPath(path).parts) == 1 or path.startswith("docs/")
    )


def docs_only(paths):
    return bool(paths) and all(documentation(path) for path in paths)


def code_tree(commit):
    tree = subprocess.check_output(["git", "ls-tree", "-r", "-z", "--full-tree", commit])
    entries = [entry for entry in tree.split(b"\0") if entry
               and not documentation(os.fsdecode(entry.split(b"\t", 1)[1]))]
    return hashlib.sha256(b"\0".join(entries)).hexdigest()


def changed_paths(base, head):
    # Disable rename detection so both the deleted and added paths are checked.
    result = subprocess.run(
        ["git", "diff", "--name-only", "--no-renames", "-z", base, head],
        check=True, stdout=subprocess.PIPE,
    )
    return [os.fsdecode(path) for path in result.stdout.split(b"\0") if path]


def gate(needs, event):
    if needs["changes"]["result"] != "success":
        return False
    code = needs["changes"]["outputs"].get("code")
    if code not in ("true", "false"):
        return False
    for name in ("quality", "lint", "unit", "integration", "long", "pty", "native-deps", "module-minimum", "commits"):
        expected = "success"
        if name == "commits" and event != "pull_request":
            expected = "skipped"
        elif name not in ("quality", "commits", "integration", "long") and code == "false":
            expected = "skipped"
        if needs[name]["result"] != expected:
            return False
    return True


def main():
    if sys.argv[1] == "changes":
        base, head = sys.argv[2:]
        # New branches, inaccessible history, or an empty diff run all checks.
        try:
            code = not docs_only(changed_paths(base, head))
        except subprocess.CalledProcessError:
            code = True
        print(f"code={str(code).lower()}")
    elif sys.argv[1] == "gate":
        if not gate(json.loads(os.environ["CI_NEEDS"]), os.environ["CI_EVENT"]):
            sys.exit("Required CI jobs did not reach their expected results")
    elif sys.argv[1] == "code-tree":
        print(code_tree(sys.argv[2]))
    else:
        sys.exit("usage: ci-policy.py changes BASE HEAD | code-tree COMMIT | gate")


if __name__ == "__main__":
    main()
