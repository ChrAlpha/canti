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
	failing, exit := "--- FAIL: TestParse (0.00s)\n    parse_test.go:12: unexpected token \"}\"\nFAIL\n", 2
	kept := fmt.Sprintf("kept %d of 9000 bytes", len(failing))
	return []frozenScenario{
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
