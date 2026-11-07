package harness_test

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
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
	one, two, needle := `printf 'beta one\n'`, `printf 'beta two\n'`, `printf 'the needle is in the shell\n'`
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
				}, steps: []string{"1 done retrieval 1 sources 0 entries", "2 done retrieval 1 sources 0 entries", "3 done retrieval 1 sources 0 entries"}},
			},
			marks: map[string][]string{
				"run2-step02-rev1.json": {"Path " + earlier + "/result:\n", "phase: returned\n\nThe build target is ./cmd/canti.\n"},
				"run2-step03-rev1.json": {"Path " + earlier + "/goal:\n", "Name the build target.\n"},
				"run2-step04-rev1.json": {"Path " + current + "/session:\n",
					`{"session_id":"session-1","sequence":2,"input_id":"input-2","previous_run_id":"` + corpusRun + `"}` + "\n"},
			},
		},
		{
			name: "session-handover-paths", limits: core.Limits{MaxSteps: 3, MaxAttempts: 3}, windowSteps: 1,
			outputs: map[string]string{"ls /workspace": "go.mod\nmain.go\n", "cat go.mod": "module example\n"},
			runs: []frozenRun{
				{id: corpusRun, goal: "List /workspace.", replies: []string{
					corpusDecision(core.Exec, "ls /workspace"),
					corpusDecision(core.Exec, harness.HandoverPrefix+"Listed go.mod and main.go."),
					corpusDecision(core.Return, "go.mod and main.go."),
				}, steps: []string{"1 exited 0 output complete", "2 done handover"}},
				{id: corpusNextRun, goal: "Name the module.", replies: []string{
					corpusDecision(core.Exec, "cat go.mod"),
					corpusDecision(core.Exec, "cat go.mod"),
					corpusDecision(core.Exec, harness.HandoverPrefix+"go.mod names module example; report it."),
				}, steps: []string{"1 exited 0 output complete", "2 refused", "3 done handover"}, stop: core.StopStepsExhausted},
				{id: corpusThirdRun, goal: "Continue where the session stopped.", replies: []string{
					corpusRetrieve("read", current+"/steps/00000003/command", "", "Read where the stopped run handed over."),
					corpusDecision(core.Exec, harness.HandoverPrefix+"The module is example."),
					corpusDecision(core.Return, "The module is example."),
				}, steps: []string{"1 done retrieval 1 sources 0 entries", "2 done handover"}},
			},
			marks: map[string][]string{
				"run3-step01-rev1.json": {
					"; returned):\nLatest handover of this run, agent-written and possibly overtaken by later steps: " + earlier + "/steps/00000002/command\nEarlier user goal:\n",
					"no assistant return.\nLatest handover of this run, agent-written and possibly overtaken by later steps: " + current + "/steps/00000003/command\nEarlier user goal:\n",
				},
				"run3-step02-rev1.json": {"Path " + current + "/steps/00000003/command:\n", "go.mod names module example; report it."},
			},
			edge: func(t *testing.T, requests map[string]harness.Request) {
				if w := requests["run2-step03-rev1.json"].Window; w == nil || w.FirstStep != 1 || !w.Requested {
					t.Errorf("run 2 handed over in its final step under window %+v; want the first window still requesting", w)
				}
				var got []int
				for _, s := range requests["run3-step01-rev1.json"].Context.Session {
					got = append(got, s.HandoverStep)
				}
				if !slices.Equal(got, []int{2, 3}) {
					t.Errorf("run 3 points to handover steps %v; want [2 3]", got)
				}
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
			}, steps: []string{"1 exited 0 output complete", "2 exited 0 output complete", "3 done retrieval 3 sources 0 entries more", "4 done retrieval 3 sources 0 entries"}}},
			marks: map[string][]string{
				"run1-step04-rev1.json": {`Retrieval continuation: {"next_offset":0,"next_cursor":"` + stepCursor + `","more":true}`, "Path " + corpusSteps + "/00000002/meta:\n"},
				"run1-step05-rev1.json": {`Retrieval continuation: {"next_offset":0,"more":false}`, "Path " + corpusSteps + "/00000001/meta:\n"},
			},
		},
		{
			name: "directory-search-pages", limits: stepLimits, outputs: map[string]string{letters: "alpha\nbeta\ngamma\ndelta\n"},
			runs: []frozenRun{{id: corpusRun, goal: "Find every record of step 1 that mentions beta.", replies: []string{
				corpusDecision(core.Exec, letters),
				corpusRetrieve("search", step1, `,"query":"beta","limit":24`, "Find beta in the step records."),
				corpusRetrieve("search", step1, `,"query":"beta","limit":24,"cursor":"`+metaCursor+`"`, "Continue the search."),
				corpusDecision(core.Return, "The command and the preview of step 1 mention beta."),
			}, steps: []string{"1 exited 0 output complete", "2 done retrieval 1 sources 0 entries more", "3 done retrieval 1 sources 0 entries"}}},
			marks: map[string][]string{
				"run1-step03-rev1.json": {`Retrieval continuation: {"next_offset":0,"next_cursor":"` + metaCursor + `","more":true}`, "Path " + step1 + "/command:\n"},
				"run1-step04-rev1.json": {`Retrieval continuation: {"next_offset":0,"more":false}`, "Path " + step1 + "/preview:\n"},
			},
		},
		{
			name: "purpose-across-windows", limits: stepLimits, windowSteps: 1, outputs: map[string]string{"ls /workspace": "go.mod\nmain.go\n"},
			runs: []frozenRun{{id: corpusRun, goal: "Report why the workspace was listed.", replies: []string{
				corpusPurpose(core.Exec, "List the workspace,\nthen check go.mod for the module name.", "ls /workspace"),
				corpusDecision(core.Exec, harness.HandoverPrefix+"Listed /workspace. Next: recall why."),
				corpusRetrieve("read", step1+"/purpose", "", "Recall why step 1 ran."),
				corpusDecision(core.Exec, harness.HandoverPrefix+"Step 1 listed /workspace to find go.mod."),
				corpusDecision(core.Return, "Step 1 listed /workspace to find go.mod and its module name."),
			}, steps: []string{"1 exited 0 output complete", "2 done handover", "3 done retrieval 1 sources 0 entries", "4 done handover"}}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"### Step 1\nPurpose: List the workspace, then check go.mod for the module name.\n"},
				"run1-step04-rev1.json": {"This window begins at step 3.", "Path " + step1 + "/purpose:\n",
					"List the workspace,\nthen check go.mod for the module name.\n"},
			},
		},
		{
			// The errors that replace the details of the system prompt
			// reach the next request, and the recovery each names works.
			name: "retrieval-failure-guidance", limits: stepLimits, outputs: map[string]string{needle: "the needle is in the shell\n"},
			imports: map[string]string{"mixed/a.txt": "no match here\n", "mixed/b.bin": "needle\x00binary\n", "mixed/c.txt": "the needle is in c\n",
				"clean/d.txt": "the needle is in d\n"},
			runs: []frozenRun{{id: corpusRun, goal: "Find every record that mentions the needle.", replies: []string{
				corpusDecision(core.Exec, needle),
				corpusRetrieve("search", "/canti/imports/mixed", `,"query":"needle"`, "Find the needle."),
				corpusRetrieve("list", "/canti/imports/mixed", "", "List the files to search one at a time."),
				corpusRetrieve("search", "/canti/imports/mixed/c.txt", `,"query":"needle"`, "Search c.txt alone."),
				corpusRetrieve("search", "/canti/imports/clean", `,"query":"needle","cursor":"`+metaCursor+`"`, "Continue with a cursor of another search."),
				corpusRetrieve("search", "/canti/imports/clean", `,"query":"needle"`, "Start the search again."),
				corpusRetrieve("discover", corpusSteps, `,"query":"needle","cursor":"`+stepCursor+`"`, "Continue with a cursor of another discovery."),
				corpusRetrieve("discover", corpusSteps, `,"query":"needle"`, "Start the discovery again."),
				corpusDecision(core.Return, "c.txt, d.txt, and step 1 mention the needle."),
			}, steps: []string{"1 exited 0 output complete", "2 failed retrieval error", "3 done retrieval 0 sources 3 entries",
				"4 done retrieval 1 sources 0 entries", "5 failed retrieval error", "6 done retrieval 1 sources 0 entries",
				"7 failed retrieval error", "8 done retrieval 3 sources 0 entries"}}},
			marks: map[string][]string{
				"run1-step03-rev1.json": {`"error":"directory search failed at b.bin with no results, which is not proof of absence; list the directory and read or search its files one at a time: source contains NUL; only UTF-8 text is supported"`},
				"run1-step05-rev1.json": {"Path /canti/imports/mixed/c.txt:\n"},
				"run1-step06-rev1.json": {`"error":"invalid directory cursor or changed path/query; start a new search without cursor"`},
				"run1-step07-rev1.json": {"Path /canti/imports/clean/d.txt:\n"},
				"run1-step08-rev1.json": {`"error":"invalid discovery cursor or changed path/query/history; start a new discover without cursor"`},
				"run1-step09-rev1.json": {"Path " + step1 + "/command:\n"},
			},
		},
	}
}

// carryScenarios pin how a required handover carries committed evidence
// into the window it opens: a carry of a whole read and of one search range,
// a range carried again under its original identity, the old and the new
// snapshot of one source carried side by side, a refused selection followed
// by a plain handover, an unsolicited carry, and a carry the next window
// has no room for.
func carryScenarios() []frozenScenario {
	stepLimits := core.Limits{MaxSteps: 20, MaxAttempts: 3}
	notes := "notes"
	text := "```go\nfunc parse() {}\n```\n" + strings.Repeat("The parser was rewritten in v2.\n", 20) + "func parse is the entry point.\n"
	edited := strings.Replace(text, "rewritten in v2", "rewritten in v3", 1)
	read := corpusRetrieve("read", "/canti/imports/"+notes, "", "Read the notes.")
	carry := func(handover, entries string) string {
		return corpusDecision(core.Exec, harness.HandoverCarryPrefix+`{"text":"`+handover+`","carry":[`+entries+`]}`)
	}
	carried := func(name string, steps ...int) func(*testing.T, map[string]harness.Request) {
		return func(t *testing.T, requests map[string]harness.Request) {
			r := requests[name]
			var got []int
			for _, ref := range r.Window.Carry {
				got = append(got, ref.Step)
			}
			if !slices.Equal(got, steps) || r.Context == nil || !slices.Equal(r.Context.Carried, r.Window.Carry) || r.Context.Bytes.Carry == 0 {
				t.Errorf("%s carries %v with capture %+v; want steps %v captured with their bytes", name, r.Window.Carry, r.Context, steps)
			}
		}
	}
	return []frozenScenario{
		{
			name: "handover-carry", limits: stepLimits, windowSteps: 2, imports: map[string]string{notes: text},
			runs: []frozenRun{{id: corpusRun, goal: "What do the imported notes say about the parser?", replies: []string{
				read,
				corpusRetrieve("search", "/canti/imports/"+notes, `,"query":"func parse","limit":64`, "Find every mention of parse."),
				carry("Notes read; carry them to report the parser change.", `{"step":1},{"step":2,"source":1}`),
				corpusDecision(core.Return, "The parser was rewritten in v2."),
			}, steps: []string{"1 done retrieval 1 sources 0 entries", "2 done retrieval 2 sources 0 entries", "3 done handover"}}},
			marks: map[string][]string{
				"run1-step03-rev1.json": {"## Handover required\n", harness.HandoverCarryPrefix + "instead"},
				"run1-step04-rev1.json": {"## Carried evidence\n", "Selected by the agent-written handover at step 3.", "Carried step 1 source 0: /canti/imports/notes\n",
					"Carried step 2 source 1: /canti/imports/notes\n", "a candidate range around a search match, not the whole source", "## Current window steps\nNone.\n"},
			},
			edge: carried("run1-step04-rev1.json", 1, 2),
		},
		{
			name: "handover-carry-again", limits: stepLimits, windowSteps: 1, imports: map[string]string{notes: text, "plan": "Step one: rewrite the parser.\n"},
			runs: []frozenRun{{id: corpusRun, goal: "Compare the notes with the plan.", replies: []string{
				read,
				carry("Notes read; next read the plan.", `{"step":1}`),
				corpusRetrieve("read", "/canti/imports/plan", "", "Read the plan."),
				carry("Both read; compare them.", `{"step":1},{"step":3}`),
				corpusDecision(core.Return, "The plan matches the notes."),
			}, steps: []string{"1 done retrieval 1 sources 0 entries", "2 done handover", "3 done retrieval 1 sources 0 entries", "4 done handover"}}},
			marks: map[string][]string{
				"run1-step05-rev1.json": {"Carried step 1 source 0: /canti/imports/notes\n", "Carried step 3 source 0: /canti/imports/plan\n"},
			},
			edge: func(t *testing.T, requests map[string]harness.Request) {
				carried("run1-step05-rev1.json", 1, 3)(t, requests)
				first, again := requests["run1-step03-rev1.json"].Window.Carry, requests["run1-step05-rev1.json"].Window.Carry
				if len(first) != 1 || len(again) != 2 || again[0] != first[0] {
					t.Errorf("the range carried again is %+v, first carried as %+v; want its original identity", again, first)
				}
			},
		},
		{
			name: "handover-carry-snapshots", limits: stepLimits, windowSteps: 3, imports: map[string]string{notes: text},
			changes: map[string]func(*os.Root) error{"edit notes": func(r *os.Root) error { return r.WriteFile(notes, []byte(edited), 0o600) }},
			outputs: map[string]string{"edit notes": "edited\n"},
			runs: []frozenRun{{id: corpusRun, goal: "What changed in the notes?", replies: []string{
				read,
				corpusDecision(core.Exec, "edit notes"),
				read,
				carry("Compare the two snapshots of the notes.", `{"step":1},{"step":3}`),
				corpusDecision(core.Return, "v2 became v3."),
			}, steps: []string{"1 done retrieval 1 sources 0 entries", "2 exited 0 output complete", "3 done retrieval 1 sources 0 entries", "4 done handover"}}},
			marks: map[string][]string{
				"run1-step05-rev1.json": {"Carried step 1 source 0: /canti/imports/notes\n", "Carried step 3 source 0: /canti/imports/notes\n", "rewritten in v2", "rewritten in v3"},
			},
			edge: func(t *testing.T, requests map[string]harness.Request) {
				carried("run1-step05-rev1.json", 1, 3)(t, requests)
				c := requests["run1-step05-rev1.json"].Window.Carry
				if len(c) == 2 && (c[0].Length != c[1].Length || c[0].SHA256 == c[1].SHA256) {
					t.Errorf("the snapshots carry %+v; want one range of each content", c)
				}
			},
		},
		{
			name: "handover-carry-refused", limits: stepLimits, windowSteps: 1, outputs: map[string]string{"ls /workspace": "go.mod\n"},
			runs: []frozenRun{{id: corpusRun, goal: "Report the files in /workspace.", replies: []string{
				corpusDecision(core.Exec, "ls /workspace"),
				carry("Carry the listing.", `{"step":1}`),
				corpusDecision(core.Exec, harness.HandoverPrefix+"Listed /workspace: go.mod. Next: report it."),
				corpusDecision(core.Return, "The workspace holds go.mod."),
			}, steps: []string{"1 exited 0 output complete", "2 failed", "3 done handover"}}},
			marks: map[string][]string{
				"run1-step03-rev1.json": {"Context action failed: step 1 of this window holds no retrieved evidence", "This step is the last chance to hand over"},
				"run1-step04-rev1.json": {"This window begins at step 4.", "## Handover\n"},
			},
		},
		{
			name: "handover-carry-unsolicited", limits: stepLimits, imports: map[string]string{notes: text},
			runs: []frozenRun{{id: corpusRun, goal: "What do the imported notes say about the parser?", replies: []string{
				read,
				carry("Carry the notes.", `{"step":1}`),
				corpusDecision(core.Return, "The parser was rewritten in v2."),
			}, steps: []string{"1 done retrieval 1 sources 0 entries", "2 failed"}}},
			marks: map[string][]string{
				"run1-step03-rev1.json": {"Context action failed: handover was not requested for this step", "This window begins at step 1."},
			},
		},
		{
			name: "handover-carry-no-room", limits: stepLimits, windowSteps: 2, contextBytes: 75000,
			imports: map[string]string{notes: strings.Repeat("The parser was rewritten in v2.\n", 256)},
			runs: []frozenRun{{id: corpusRun, goal: "What do the imported notes say about the parser?", replies: []string{
				corpusRetrieve("read", "/canti/imports/"+notes, `,"limit":8192`, "Read the notes."),
				corpusRetrieve("read", "/canti/imports/"+notes, `,"offset":4096,"limit":4096`, "Read the rest."),
				carry("Carry both reads.", `{"step":1},{"step":2}`),
				corpusDecision(core.Exec, harness.HandoverPrefix+"Notes read: the parser was rewritten in v2. Next: report it."),
				corpusDecision(core.Return, "The parser was rewritten in v2."),
			}, steps: []string{"1 done retrieval 1 sources 0 entries", "2 done retrieval 1 sources 0 entries", "3 failed", "4 done handover"}}},
			marks: map[string][]string{
				"run1-step04-rev1.json": {"Context action failed: carried evidence of ", "; a carry needs more than 0. Carry fewer ranges, or hand over with canti:handover"},
			},
		},
	}
}
