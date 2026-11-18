package harness_test

import (
	"github.com/ChrAlpha/canti/runtime/core"
)

func retrievalScenarios() []frozenScenario {
	return []frozenScenario{{name: "historical-frozen-evidence", limits: core.Limits{MaxSteps: 10, MaxAttempts: 3}, imports: map[string]string{"notes": "original frozen snapshot"}, runs: []frozenRun{{id: corpusRun, goal: "Read an earlier frozen observation through the archive.", replies: []string{corpusRetrieve("read", "/canti/imports/notes", "", "Observe original."), corpusRetrieve("read", corpusSteps+"/00000001/evidence/0000", "", "Read frozen observation."), corpusRetrieve("read", corpusSteps+"/00000001/evidence.json", "", "Inspect source identity."), corpusDecision(core.Return, "Frozen observation available.")}, steps: []string{"1 done retrieval 1 sources 0 entries", "2 done retrieval 1 sources 0 entries", "3 done retrieval 1 sources 0 entries"}}}}}
}
