#!/usr/bin/env python3
"""Offline fixtures for the CI selection and required-check contracts."""

import copy
import importlib.util
import os
import pathlib
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("policy", pathlib.Path(__file__).with_name("ci-policy.py"))
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)
shard_spec = importlib.util.spec_from_file_location("shard", pathlib.Path(__file__).with_name("ci-shard.py"))
shard = importlib.util.module_from_spec(shard_spec)
shard_spec.loader.exec_module(shard)


class ChangeTests(unittest.TestCase):
    def test_only_documentation_can_skip_heavy_jobs(self):
        self.assertTrue(policy.docs_only(["README.md", "AGENTS.md", "docs/development.md"]))
        for path in ("runtime/prompt.md", "runtime/testdata/record.md", "sdk/go/go.sum",
                     "apps/tui/main.go", ".github/workflows/ci.yml", "scripts/ci-policy.py",
                     "docs/example.json", "unknown", "README.md\nmain.go"):
            with self.subTest(path=path):
                self.assertFalse(policy.docs_only(["README.md", path]))
        self.assertFalse(policy.docs_only([]))

    def test_deleted_and_renamed_paths_are_both_considered(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)

            def git(*args):
                return subprocess.check_output(["git", "-C", directory, *args]).decode().strip()

            git("init", "-q")
            (root / "runtime").mkdir()
            (root / "runtime" / "prompt.md").write_text("rules\n")
            (root / "README.md").write_text("docs\n")
            git("add", ".")
            # Avoid hooks/signing or dependence on the user's identity in this fixture.
            tree = git("write-tree")
            previous = os.getcwd()
            try:
                os.chdir(directory)
                original_fingerprint = policy.code_tree(tree)
                (root / "README.md").write_text("new docs\n")
                git("add", ".")
                self.assertEqual(policy.code_tree(git("write-tree")), original_fingerprint)
            finally:
                os.chdir(previous)
            (root / "runtime" / "prompt.md").rename(root / "prompt.md")
            (root / "README.md").unlink()
            git("add", "-A")
            new_tree = git("write-tree")
            previous = os.getcwd()
            try:
                os.chdir(directory)
                changed = policy.changed_paths(tree, new_tree)
                self.assertNotEqual(policy.code_tree(new_tree), original_fingerprint)
            finally:
                os.chdir(previous)
            self.assertEqual(set(changed), {"runtime/prompt.md", "prompt.md", "README.md"})
            self.assertFalse(policy.docs_only(changed))


class GateTests(unittest.TestCase):
    def results(self, code="true", event="pull_request"):
        needs = {"changes": {"result": "success", "outputs": {"code": code}}}
        for name in ("quality", "lint", "unit", "integration", "long", "pty", "commits"):
            result = "success"
            if (name == "commits" and event != "pull_request") or (
                name not in ("quality", "commits", "integration", "long") and code == "false"
            ):
                result = "skipped"
            needs[name] = {"result": result}
        return needs

    def test_expected_code_and_docs_results_pass_for_pr_and_push(self):
        for code in ("true", "false"):
            for event in ("pull_request", "push"):
                self.assertTrue(policy.gate(self.results(code, event), event))

    def test_failure_cancellation_and_unexpected_skip_fail_closed(self):
        for code in ("true", "false"):
            needs = self.results(code)
            for name in needs:
                for result in ("failure", "cancelled", "skipped", "success", ""):
                    if result == needs[name]["result"]:
                        continue
                    changed = copy.deepcopy(needs)
                    changed[name]["result"] = result
                    self.assertFalse(policy.gate(changed, "pull_request"), (code, name, result))

    def test_missing_plan_output_cannot_authorize_skips(self):
        for value in (None, "", "False", "unexpected"):
            needs = self.results("false")
            needs["changes"]["outputs"]["code"] = value
            self.assertFalse(policy.gate(needs, "pull_request"))

    def test_missing_job_cannot_pass(self):
        needs = self.results()
        del needs["unit"]
        with self.assertRaises(KeyError):
            policy.gate(needs, "pull_request")


class ShardTests(unittest.TestCase):
    def test_union_and_exclusion_match_each_original_suite_exactly(self):
        names = [f"TestContract{index}" for index in range(1000)] + ["FuzzDecode", "ExampleRun"]
        for args in ([], ["-skip", "^(TestContract0|TestContract1)$"],
                     ["-run", "^(TestContract0|TestContract1)$"]):
            expected = shard.selected(names, args, 0, 1)
            parts = [shard.selected(names, args, index, 4) for index in range(4)]
            flattened = [name for part in parts for name in part]
            self.assertCountEqual(flattened, expected)
            self.assertEqual(len(flattened), len(set(flattened)))

    def test_discovery_keeps_tests_fuzz_seeds_and_examples(self):
        events = '\n'.join([
            '{"Action":"output","Output":"TestOne\\n"}',
            '{"Action":"output","Output":"FuzzDecode\\n"}',
            '{"Action":"output","Output":"ExampleRun\\n"}',
            '{"Action":"output","Output":"Example\\n"}',
            '{"Action":"output","Output":"BenchmarkDecode\\n"}',
            '{"Action":"output","Output":"ok package\\n"}',
        ])
        self.assertEqual(shard.test_names(events), ["Example", "ExampleRun", "FuzzDecode", "TestOne"])


class CommitTests(unittest.TestCase):
    def test_docs_reuse_cannot_hide_a_broken_intermediate_commit(self):
        checker = pathlib.Path(__file__).resolve().with_name("check-commits.sh")
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            repo, binaries = root / "repo", root / "bin"
            repo.mkdir()
            binaries.mkdir()
            calls = root / "calls"
            fake_go = binaries / "go"
            fake_go.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$CI_FIXTURE_CALLS"\n'
                               '[ ! -f broken ]\n')
            fake_go.chmod(0o755)
            env = dict(os.environ, PATH=str(binaries) + os.pathsep + os.environ["PATH"],
                       CI_FIXTURE_CALLS=str(calls), GIT_AUTHOR_NAME="CI Fixture",
                       GIT_AUTHOR_EMAIL="ci@example.invalid", GIT_COMMITTER_NAME="CI Fixture",
                       GIT_COMMITTER_EMAIL="ci@example.invalid")

            def git(*args, text=None):
                return subprocess.check_output(["git", "-C", str(repo), *args],
                                               env=env, input=text, text=True).strip()

            parent = None

            def record():
                nonlocal parent
                git("add", "-A")
                args = ["commit-tree", git("write-tree")]
                if parent:
                    args += ["-p", parent]
                parent = git(*args, text="fixture\n")
                git("update-ref", "HEAD", parent)
                return parent

            git("init", "-q")
            (repo / "source").write_text("code\n")
            base = record()
            (repo / "README.md").write_text("first docs\n")
            record()
            (repo / "README.md").write_text("second docs\n")
            record()
            (repo / "broken").touch()
            record()
            (repo / "broken").unlink()
            record()
            result = subprocess.run(["sh", str(checker), base], cwd=repo, env=env,
                                    text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertEqual(result.stdout.count("reuses successful build/vet of identical sources"), 2)
            self.assertEqual(len(calls.read_text().splitlines()), 4)


if __name__ == "__main__":
    unittest.main()
