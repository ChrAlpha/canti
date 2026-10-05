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
	one, two := `printf 'beta one\n'`, `printf 'beta two\n'`
	// {"binding":"<sha256 of ["discover",path,query,false]>","through":2,"step":1,"field":0,"offset":0}
	stepCursor := "eyJiaW5kaW5nIjoiZjU1YzNhZjA2ZGRiNTA4Y2EyYmRhM2NmYzI1MThjNDhkMmQ3ZmVlN2JhNjRjNGExN2FjNzZjN2I3YjZkODhmMiIsInRocm91Z2giOjIsInN0ZXAiOjEsImZpZWxkIjowLCJvZmZzZXQiOjB9"
	earlier, current := "/canti/runs/"+corpusRun, "/canti/runs/"+corpusNextRun
	return []frozenScenario{
		{
			name: "session-history-files", limits: stepLimits,
			runs: []frozenRun{
				{id: corpusRun, goal: "Name the build target.", replies: []string{corpusDecision(core.Return, "The build target is ./cmd/canti.")}},
				{id: corpusNextRun, goal: "Which build target did the previous run name?", replies: []string{
					corpusRetrieve("read", earlier+"/result", "", "Read the previous result."),
					corpusRetrieve("read", earlier+"/goal", "", "Read the previous goal."),
					corpusRetrieve("read", current+"/session", "", "Check the session record."),
					corpusDecision(core.Return, "The previous run named ./cmd/canti."),
				}, steps: []string{"1 exited 0 retrieval 1 sources 0 entries", "2 exited 0 retrieval 1 sources 0 entries", "3 exited 0 retrieval 1 sources 0 entries"}},
			},
			marks: map[string][]string{
				"run2-step02-rev1.json": {"Source " + earlier + "/result:\n", "phase: returned\n\nThe build target is ./cmd/canti.\n"},
				"run2-step03-rev1.json": {"Source " + earlier + "/goal:\n", "Name the build target.\n"},
				"run2-step04-rev1.json": {"Source " + current + "/session:\n",
					`{"session_id":"session-1","sequence":2,"input_id":"input-2","previous_run_id":"` + corpusRun + `"}` + "\n"},
			},
		},
		{
			name: "discovery-pages", limits: stepLimits, outputs: map[string]string{one: "beta one\n", two: "beta two\n"},
			runs: []frozenRun{{id: corpusRun, goal: "Find the steps that printed beta.", replies: []string{
				corpusDecision(core.Exec, one),
				corpusDecision(core.Exec, two),
				corpusRetrieve("discover", corpusSteps, `,"query":"beta","limit":256`, "Find the steps that printed beta."),
				corpusRetrieve("discover", corpusSteps, `,"query":"beta","limit":256,"cursor":"`+stepCursor+`"`, "Continue the discovery."),
				corpusDecision(core.Return, "Steps 1 and 2 printed beta."),
			}, steps: []string{"1 exited 0 output complete", "2 exited 0 output complete", "3 exited 0 retrieval 3 sources 0 entries more", "4 exited 0 retrieval 3 sources 0 entries"}}},
			marks: map[string][]string{
				"run1-step04-rev1.json": {`Retrieval continuation: {"next_offset":0,"next_cursor":"` + stepCursor + `","more":true}`, "Source " + corpusSteps + "/00000002/meta:\n"},
				"run1-step05-rev1.json": {`Retrieval continuation: {"next_offset":0,"more":false}`, "Source " + corpusSteps + "/00000001/meta:\n"},
			},
		},
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
