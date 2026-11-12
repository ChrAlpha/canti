package harness_test

import (
	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func failureScenarios() []frozenScenario {
	return []frozenScenario{
		{name: "bounded-local-failures", limits: core.Limits{MaxSteps: 10, MaxAttempts: 3}, runs: []frozenRun{{id: corpusRun, goal: "Report malformed local actions.", replies: []string{corpusDecision(core.Exec, `canti:retrieve {"bogus":true}`), corpusDecision(core.Exec, "canti:handover   "), corpusDecision(core.Return, "Local failures were reported.")}, steps: []string{"1 failed", "2 failed"}}}},
		{name: "failed-job", limits: core.Limits{MaxSteps: 10, MaxAttempts: 3}, outputs: map[string]string{"false": "job failed\n"}, finals: map[string]harness.Job{"false": {Status: harness.JobFailed, ExitCode: intPointer(1)}}, runs: []frozenRun{{id: corpusRun, goal: "Inspect failed job.", replies: []string{corpusDecision(core.Exec, "false"), corpusDecision(core.Return, "Command failed.")}, steps: []string{"1 exited 1 output unavailable"}}}},
	}
}
func intPointer(n int) *int { return &n }
