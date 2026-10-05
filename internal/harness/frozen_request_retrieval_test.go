package harness_test

import (
	"github.com/ChrAlpha/canti/core"
)

// retrievalScenarios page through retrieval results with their cursors and
// read the history files a run publishes. The cursors are literals, so a
// continuation fails if their wire format changes.
func retrievalScenarios() []frozenScenario {
	stepLimits := core.Limits{MaxSteps: 20, MaxAttempts: 3}
	letters := `printf 'alpha\nbeta\ngamma\ndelta\n'`
	step1 := corpusSteps + "/00000001"
	// {"binding":"<sha256 of [path,query]>","file":"<meta in base64url>","offset":0}
	metaCursor := "eyJiaW5kaW5nIjoiMGVlOGI4M2E3YWYzMTgyMDdkODNhNDA0MTI2ZGRkMjNlODM4MDgyY2FkYzk4NmU1YThlOTJjN2I5YTlkMjhiOCIsImZpbGUiOiJiV1YwWVEiLCJvZmZzZXQiOjB9"
	return []frozenScenario{
		{
			name: "directory-search-pages", limits: stepLimits, outputs: map[string]string{letters: "alpha\nbeta\ngamma\ndelta\n"},
			runs: []frozenRun{{id: corpusRun, goal: "Find every record of step 1 that mentions beta.", replies: []string{
				corpusDecision(core.Exec, letters),
				corpusRetrieve("search", step1, `,"query":"beta","limit":24`, "Find beta in the step records."),
				corpusRetrieve("search", step1, `,"query":"beta","limit":24,"cursor":"`+metaCursor+`"`, "Continue the search."),
				corpusDecision(core.Return, "The command and the preview of step 1 mention beta."),
			}, steps: []string{"1 exited 0 output complete", "2 exited 0 retrieval 1 sources 0 entries more", "3 exited 0 retrieval 1 sources 0 entries"}}},
			marks: map[string][]string{
				"run1-step03-rev1.json": {`Retrieval continuation: {"next_offset":0,"next_cursor":"` + metaCursor + `","more":true}`, "Source " + step1 + "/command:\n"},
				"run1-step04-rev1.json": {`Retrieval continuation: {"next_offset":0,"more":false}`, "Source " + step1 + "/preview:\n"},
			},
		},
	}
}
