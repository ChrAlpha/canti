package harness_test

import (
	"fmt"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

// failureScenarios reach the failed, uncertain, and refused paths of a run.
func failureScenarios() []frozenScenario {
	stepLimits := core.Limits{MaxSteps: 20, MaxAttempts: 3}
	partial := func(detail string) harness.ArtifactRef { return harness.ArtifactRef{Available: true, Detail: detail} }
	failing, exit, zero := "--- FAIL: TestParse (0.00s)\n    parse_test.go:12: unexpected token \"}\"\nFAIL\n", 2, 0
	kept := fmt.Sprintf("kept %d of 9000 bytes", len(failing))
	handover := corpusDecision(core.Exec, harness.HandoverPrefix+"Read go.mod next, then report the module path.")
	return []frozenScenario{
		{
			name: "failed-context-actions", limits: stepLimits, windowSteps: 1,
			runs: []frozenRun{{id: corpusRun, goal: "Report the module path of /workspace.", replies: []string{
				corpusDecision(core.Exec, `canti:retrieve {"operation":"read","path":"/canti/runs/`+corpusRun+`/goal","reason":"Reread the goal.","bogus":true}`),
				corpusDecision(core.Exec, harness.HandoverPrefix),
				handover,
				corpusDecision(core.Return, "I could not read go.mod yet."),
			}, steps: []string{"1 failed", "2 failed", "3 done handover"}}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"Preview:\n```\nContext action failed: json: unknown field \"bogus\"\n```\n", "## Handover required\n"},
				"run1-step03-rev1.json": {"Preview:\n```\nContext action failed: harness: handover text must not be empty\n```\n", "## Handover required\n"},
				"run1-step04-rev1.json": {"This window begins at step 4.", "## Handover\n", "## Current window steps\nNone.\n"},
			},
		},
		{
			name: "action-type-errors", limits: stepLimits,
			runs: []frozenRun{{id: corpusRun, goal: "Report the module path of /workspace.", replies: []string{
				corpusDecision(core.Exec, `canti:retrieve {"operation":1,"path":"/canti/runs/`+corpusRun+`/goal","reason":"Reread the goal."}`),
				corpusDecision(core.Return, "The action was refused."),
			}, steps: []string{"1 failed"}}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"Context action failed: json: cannot unmarshal number into Go struct field RetrievalQuery.operation of type string"},
			},
		},
		{
			// A command the requested handover refuses never runs: it is
			// recorded as failed to start, with no job bound or started.
			name: "handover-gate", limits: stepLimits, windowSteps: 1, outputs: map[string]string{"ls /workspace": "go.mod\nmain.go\n"},
			runs: []frozenRun{{id: corpusRun, goal: "Report the module path of /workspace.", replies: []string{
				corpusDecision(core.Exec, "ls /workspace"),
				corpusDecision(core.Exec, "cat go.mod"),
				handover,
				corpusDecision(core.Return, "I have not read go.mod yet."),
			}, steps: []string{"1 exited 0 output complete", "2 refused", "3 done handover"}}},
			marks: map[string][]string{
				"run1-step03-rev1.json": {"### Step 2\nCommand:\n```\ncat go.mod\n```\nExecution: failed_to_start\n",
					"Preview:\n```\nContext action failed: handover is required before any more work; use canti:handover\n```\n", "## Handover required\n"},
				"run1-step04-rev1.json": {"This window begins at step 4.", "## Handover\n"},
			},
		},
		{
			name: "handover-gate-retrieval", limits: stepLimits, windowSteps: 1, outputs: map[string]string{"ls /workspace": "go.mod\nmain.go\n"},
			runs: []frozenRun{{id: corpusRun, goal: "Report the module path of /workspace.", replies: []string{
				corpusDecision(core.Exec, "ls /workspace"),
				corpusRetrieve("read", corpusSteps+"/00000001/preview", "", "Reread the listing."),
				handover,
				corpusDecision(core.Return, "I have not read go.mod yet."),
			}, steps: []string{"1 exited 0 output complete", "2 refused", "3 done handover"}}},
			marks: map[string][]string{
				"run1-step03-rev1.json": {"Execution: failed_to_start\n", "Context action failed: handover is required before any more work; use canti:handover"},
			},
		},
		{
			name: "stopped-session-run", limits: core.Limits{MaxSteps: 1, MaxAttempts: 3}, outputs: map[string]string{"ls /workspace": "go.mod\nmain.go\n"},
			runs: []frozenRun{
				{id: corpusRun, goal: "List /workspace.", replies: []string{corpusDecision(core.Exec, "ls /workspace")},
					steps: []string{"1 exited 0 output complete"}, stop: core.StopStepsExhausted},
				{id: corpusNextRun, goal: "What did the previous run find?", replies: []string{
					corpusDecision(core.Return, "It listed go.mod and main.go before it ran out of steps."),
				}},
			},
			marks: map[string][]string{
				"run2-step01-rev1.json": {"## Earlier session dialogue\n",
					"goal, steps, and result): stopped, reason steps_exhausted, recorded steps 1; no assistant return.\nEarlier user goal:\n```\nList /workspace.\n```\n"},
			},
		},
		{
			name: "operator-resolution", limits: stepLimits,
			finals: map[string]harness.Job{
				"make build":     {Status: harness.JobFailedToStart, Output: harness.ArtifactRef{Detail: `job never started: exec: "make": executable file not found in $PATH`}},
				"go build ./...": {Status: harness.JobUnknown, Output: harness.ArtifactRef{Detail: "container state is unavailable"}},
			},
			runs: []frozenRun{{id: corpusRun, goal: "Build the project and report the result.", replies: []string{
				corpusDecision(core.Exec, "make build"),
				corpusDecision(core.Exec, "go build ./..."),
				corpusDecision(core.Return, "go build ./... succeeds; make is not installed."),
			}, resolve: &harness.Observation{Execution: core.ExecutionExited, ExitCode: &zero, Preview: "Checked by hand: go build ./... finished without errors.",
				Output: harness.ArtifactRef{Detail: "resolved by operator"}},
				steps: []string{"1 failed_to_start output unavailable", "2 exited 0 resolved output unavailable"}}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"Execution: failed_to_start\nOutput: 0 bytes, incomplete, unavailable (job never started: exec: \"make\": ", "Preview: empty\n"},
				"run1-step03-rev1.json": {"Execution: exited, exit code 0, set by explicit resolution\nOutput: 0 bytes, incomplete, unavailable (resolved by operator)\n" +
					"Preview:\n```\nChecked by hand: "},
			},
		},
		{
			name: "execution-failures", limits: stepLimits,
			outputs: map[string]string{"go test ./...": failing, "go test -run TestSlow ./...": "=== RUN   TestSlow\n", "go test -run TestFlaky ./...": "=== RUN   TestFlaky\n"},
			finals: map[string]harness.Job{
				"go test ./...": {Status: harness.JobFailed, ExitCode: &exit, Output: partial(kept)},
				"go test -run TestSlow ./...": {Status: harness.JobCancelled, StartedAt: corpusCreated, FinishedAt: corpusCreated.Add(10 * time.Minute),
					Output: partial("job was cancelled; output is partial")},
				"go test -run TestFlaky ./...": {Status: harness.JobCancelled, Output: partial("job was cancelled; output is partial")},
			},
			runs: []frozenRun{{id: corpusRun, goal: "Run the tests and report which ones fail.", budget: harness.Budget{JobTimeout: 10 * time.Minute}, replies: []string{
				corpusDecision(core.Exec, "go test ./..."),
				corpusDecision(core.Exec, "go test -run TestSlow ./..."),
				corpusDecision(core.Exec, "go test -run TestFlaky ./..."),
				corpusDecision(core.Return, "TestParse fails; TestSlow timed out and TestFlaky was cancelled."),
			}, steps: []string{"1 exited 2 output incomplete", "2 timed_out output incomplete", "3 cancelled output incomplete"}}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"Execution: exited, exit code 2\nOutput: " + fmt.Sprint(len(failing)) + " bytes, incomplete, at /canti/artifacts/effect_", "(" + kept + ")\n"},
				"run1-step03-rev1.json": {"Execution: timed_out\nOutput: 19 bytes, incomplete, at ", "(job was cancelled; output is partial)\n"},
				"run1-step04-rev1.json": {"Execution: cancelled\nOutput: 20 bytes, incomplete, at "},
			},
		},
	}
}
