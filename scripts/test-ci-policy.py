#!/usr/bin/env python3
"""Offline fixtures for the CI selection and required-check contracts."""

import copy
import importlib.util
import json
import os
import pathlib
import re
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
        for name in ("quality", "lint", "unit", "integration", "long", "pty", "native-deps", "module-minimum", "commits"):
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
    def test_long_balance_preserves_discovered_membership_for_any_count(self):
        names = list(shard.LONG_SECONDS) + ["TestNewLongCase", "FuzzNewLongSeed", "ExampleNewLong"]
        for count in (1, 2, 3, 4, 6, 8, 16):
            for args in ([], ["-skip", "^TestSession"], ["-run", "^(TestNew|Fuzz|Example)"]):
                expected = shard.selected(names, args, 0, 1)
                parts = [shard.selected(names, args, index, count, True) for index in range(count)]
                flattened = [name for part in parts for name in part]
                self.assertCountEqual(flattened, expected, (count, args))
                self.assertEqual(len(flattened), len(set(flattened)))
                self.assertEqual(parts, [shard.selected(list(reversed(names)), args, index, count, True)
                                         for index in range(count)])

    def test_long_balance_reduces_the_measured_peak_without_adding_stale_hints(self):
        names = list(shard.LONG_SECONDS)
        parts = [shard.selected(names, [], index, 4, True) for index in range(4)]
        loads = [sum(shard.LONG_SECONDS[name] for name in part) for part in parts]
        original = [sum(shard.LONG_SECONDS[name] for name in shard.selected(names, [], index, 4))
                    for index in range(4)]
        self.assertLess(max(loads), max(original))
        self.assertLess(max(loads) - min(loads), 10)
        discovered = ["TestNewLongCase"]
        self.assertEqual([name for index in range(4)
                          for name in shard.selected(discovered, [], index, 4, True)], discovered)

    def test_union_and_exclusion_match_each_original_suite_exactly(self):
        names = [f"TestContract{index}" for index in range(1000)] + ["FuzzDecode", "ExampleRun"]
        for args in ([], ["-skip", "^(TestContract0|TestContract1)$"],
                     ["-run", "^(TestContract0|TestContract1)$"]):
            expected = shard.selected(names, args, 0, 1)
            for count in (4, 6):
                parts = [shard.selected(names, args, index, count) for index in range(count)]
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


class LaneTests(unittest.TestCase):
    def test_workflow_requires_native_provenance_and_independent_minimum(self):
        root = pathlib.Path(__file__).resolve().parent.parent
        workflow = (root / ".github/workflows/ci.yml").read_text()
        sections = re.split(r"^  ([a-z][a-z-]*):\n", workflow, flags=re.MULTILINE)
        jobs = dict(zip(sections[1::2], sections[2::2]))
        for job, target in (("native-deps", "native-deps"), ("module-minimum", "isolation")):
            self.assertIn("if: needs.changes.outputs.code == 'true'", jobs[job])
            self.assertIn("run: make " + target, jobs[job])
            self.assertEqual(workflow.count("run: make " + target), 1)
            required = re.search(r"needs: \[([^]]+)\]", jobs["check"]).group(1)
            self.assertIn(job, [name.strip() for name in required.split(',')])
        self.assertIn("go-version: '1.25.x'", jobs["module-minimum"])
        self.assertIn("GOTOOLCHAIN: local", jobs["module-minimum"])
        setup = (root / ".github/actions/setup-go/action.yml").read_text()
        self.assertIn("go-version: '1.26.x'", setup)
        self.assertIn("apps/tui/third_party/*/go.sum", setup)

    def test_make_lanes_partition_modules_and_executable_acceptance_once(self):
        # Exercise Make -> module/acceptance orchestration -> shard discovery ->
        # reporter. The fake compiler records selected tests, so this checks the
        # actual call path without Docker, sibling builds, or expensive suites.
        root = pathlib.Path(__file__).resolve().parent.parent
        with tempfile.TemporaryDirectory() as directory:
            temporary = pathlib.Path(directory)
            calls = temporary / "calls"
            fake_go = temporary / "go"
            fake_go.write_text(r"""#!/usr/bin/env python3
import json, os, pathlib, re, sys
args = sys.argv[1:]
if not args or args[0] != 'test' or '-c' in args:
    sys.exit(0)
acceptance = './acceptance' in args
module = pathlib.Path.cwd().relative_to(os.environ['CI_FIXTURE_ROOT']).as_posix()
tags = args[args.index('-tags') + 1] if '-tags' in args else ''
if acceptance:
    names = ['TestAcceptanceOne', 'TestAcceptanceTwo', 'TestChatKeepsInheritedDescriptorsBlocking']
    if 'integration' in tags:
        names += ['TestChatStopsDockerWithBlockedStderr', 'TestLineDialogueSurvivesHandoverRetryAndRestart']
elif module == 'runtime':
    names = ['TestRuntimeUnit']
    if 'integration' in tags:
        names += ['TestRuntimeIntegration', 'TestContextCaptureBoundsRejectedRetrievalAcrossReopen']
elif module == 'sdk/go':
    names = ['TestSDKUnit']
else:
    names = ['TestPTYNativeRawRestore']
    if './...' in args:
        names += ['TestAppUnit']
if '-list' in args:
    for name in names:
        print(json.dumps({'Action': 'output', 'Output': name + '\n'}))
else:
    run, skip = '.', '^$'
    for index, arg in enumerate(args):
        if arg == '-run': run = args[index + 1]
        if arg == '-skip': skip = args[index + 1]
    names = [name for name in names if re.search(run, name) and not re.search(skip, name)]
    if acceptance:
        for var in ['CANTI_ACCEPTANCE_RUNTIME', 'CANTI_ACCEPTANCE_RUNTIME_FIXTURE', 'CANTI_ACCEPTANCE_TUI']:
            assert pathlib.Path(os.environ[var]).is_absolute(), var
    assert os.environ['GOWORK'] == 'off'
    with open(os.environ['CI_FIXTURE_CALLS'], 'a') as output:
        output.write(json.dumps({'module': module, 'acceptance': acceptance, 'tags': tags,
                                 'names': names, 'args': args}) + '\n')
""")
            fake_go.chmod(0o755)
            # Formatting JSON reports is outside this selection test's boundary.
            fake_jq = temporary / "jq"
            fake_jq.write_text('#!/bin/sh\ncat >/dev/null\n')
            fake_jq.chmod(0o755)
            env = dict(os.environ, PATH=str(temporary) + os.pathsep + os.environ["PATH"],
                       CI_FIXTURE_ROOT=str(root), CI_FIXTURE_CALLS=str(calls))
            for name in ("MAKEFLAGS", "MFLAGS", "MAKELEVEL"):
                env.pop(name, None)
            cases = {
                "ci-unit": (4, ['TestRuntimeUnit', 'TestSDKUnit', 'TestAppUnit',
                                'TestAcceptanceOne', 'TestAcceptanceTwo']),
                "ci-integration": (6, ['TestRuntimeUnit', 'TestRuntimeIntegration', 'TestSDKUnit',
                                       'TestAppUnit', 'TestAcceptanceOne', 'TestAcceptanceTwo',
                                       'TestChatStopsDockerWithBlockedStderr']),
                "ci-long": (4, ['TestContextCaptureBoundsRejectedRetrievalAcrossReopen',
                                'TestLineDialogueSurvivesHandoverRetryAndRestart']),
                "ci-pty": (1, ['TestPTYNativeRawRestore', 'TestChatKeepsInheritedDescriptorsBlocking']),
            }
            for target, (count, expected) in cases.items():
                calls.write_text("")
                for index in range(count):
                    # PTY must override even an inherited nonzero shard index.
                    values = ("6", "5") if target == "ci-pty" else (str(count), str(index))
                    env.update(CI_SHARD_COUNT=values[0], CI_SHARD_INDEX=values[1])
                    result = subprocess.run(['make', target], cwd=root, env=env,
                                            stdin=subprocess.DEVNULL, text=True,
                                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    self.assertEqual(result.returncode, 0, result.stdout)
                recorded = [json.loads(line) for line in calls.read_text().splitlines()]
                flattened = [name for call in recorded for name in call['names']]
                self.assertCountEqual(flattened, expected, (target, recorded))
                self.assertEqual(len(flattened), len(set(flattened)), target)
                acceptance = [call for call in recorded if call['acceptance']]
                self.assertTrue(acceptance, target)
                expected_tags = 'acceptance' if target == 'ci-unit' else 'acceptance,integration'
                self.assertTrue(all(call['tags'] == expected_tags for call in acceptance), target)
                if target == 'ci-pty':
                    self.assertEqual(len(recorded), 2)
                    self.assertTrue(all(call['args'][call['args'].index('-p') + 1] == '1'
                                        for call in recorded))


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
