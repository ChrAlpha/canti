package harness_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

const corpusThirdRun = "run_5d2a8e6f1c9b4a07e3f8b2d6c1a9e4f0"

// boundaryScenarios sit on the thresholds that decide a context: in twins one
// byte apart, the handover triggers, the earlier session dialogue a run
// selects, the second failed handover, and evidence that became unavailable.
// Each checks that it still sits on its threshold.
func boundaryScenarios() []frozenScenario {
	stepLimits := core.Limits{MaxSteps: 20, MaxAttempts: 3}
	handover := func(firstStep int, text string) string {
		return corpusDecision(core.Exec, fmt.Sprintf(`canti:handover {"run_id":"%s","first_step":%d,"text":"%s"}`, corpusRun, firstStep, text))
	}
	note := "Build failed to start; check the toolchain next."
	imported := strings.Repeat("a", 250) + "/" + strings.Repeat("b", 250) + "/" + strings.Repeat("c", 250) + "/" + strings.Repeat("d", 250) + ".md"
	return slices.Concat(handoverThresholds(), sessionBoundaries(), []frozenScenario{
		{
			name: "handover-failed-twice", limits: stepLimits, windowSteps: 1,
			finals: map[string]harness.Job{"make build": {Status: harness.JobFailedToStart,
				Output: harness.ArtifactRef{Detail: "job never started:\nexec: \"make\": executable file not found in $PATH"}}},
			runs: []frozenRun{{id: corpusRun, goal: "Build the project.", replies: []string{
				corpusDecision(core.Exec, "make build"),
				"I will hand over now.",
				handover(1, note+strings.Repeat(" ", 2049-len(note))),
				handover(3, note),
			}, steps: []string{"1 failed_to_start output unavailable", "2 exited 1 action failed", "3 exited 1 action failed"}, stop: core.StopUnrecoverable}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"(job never started: exec: \"make\": ", "## Handover required\n"},
				"run1-step02-rev2.json": {"## Rejected response\n", "## Handover required\n"},
				"run1-step03-rev1.json": {"Context action failed: harness: handover text is 2049 UTF-8 bytes; maximum 2048.", "## Handover required\n"},
			},
		},
		{
			name: "goal-and-constraints-at-caps", limits: stepLimits,
			runs: []frozenRun{{id: corpusRun, goal: "  " + strings.Repeat("Check every module. ", 1638) + "Then go.\n",
				constraints: "\t" + strings.Repeat("Keep the tree clean. ", 780) + "Stop  ",
				replies:     []string{corpusDecision(core.Return, "Every module is fine.")}}},
			edge: func(t *testing.T, requests map[string]harness.Request) {
				if b := requests["run1-step01-rev1.json"].Context.Bytes; b.Goal != len("## Goal\n")+32768+1 || b.Constraints != len("\n## Constraints\n")+16384+1 {
					t.Errorf("the goal and constraints took %d and %d bytes; want them at their 32768- and 16384-byte caps", b.Goal, b.Constraints)
				}
			},
		},
		{
			name: "unavailable-evidence", limits: stepLimits,
			imports: map[string]string{imported: "```go\nfunc parse() {}\n```\n" + strings.Repeat("The parser was rewritten in v2.\n", 40) + "func parse is the entry point.\n"},
			deletes: map[string]string{"sleep 1": imported}, outputs: map[string]string{"sleep 1": "done\n"},
			runs: []frozenRun{{id: corpusRun, goal: "What do the imported notes say about the parser?", replies: []string{
				corpusRetrieve("read", "/canti/imports/"+imported, "", strings.Repeat("Check the imported notes. ", 39)),
				corpusRetrieve("search", "/canti/imports/"+imported, `,"query":"func parse","limit":64`, "Find every mention of parse."),
				corpusDecision(core.Exec, "sleep 1"),
				corpusDecision(core.Return, "The notes said the parser was rewritten in v2, but they are no longer available."),
			}, steps: []string{"1 exited 0 retrieval 1 sources 0 entries", "2 exited 0 retrieval 2 sources 0 entries", "3 exited 0 output complete"}}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"…[truncated ", "Preview:\n```\n", "Retrieved original evidence:\n````\n", "```go\nfunc parse() {}\n```\n"},
				"run1-step04-rev1.json": {"Evidence unavailable: archive path unavailable: "},
			},
			edge: func(t *testing.T, requests map[string]harness.Request) {
				before, after := requests["run1-step03-rev1.json"].Context.Sources, requests["run1-step04-rev1.json"].Context.Sources
				if len(before) != 3 || len(after) != 3 || before[0].Source.Length <= 1024 || before[1].Source.Path != before[2].Source.Path {
					t.Fatalf("the evidence was %+v, then %+v; want a source over 1 KiB, then two of one path", before, after)
				}
				for i := range before {
					if !before[i].Available || after[i].Available || len(after[i].Error) <= 256 {
						t.Errorf("source %d was %+v, then %+v; want it available, then unavailable with an error over 256 bytes", i, before[i], after[i])
					}
				}
			},
		},
	})
}

// handoverThresholds are twins whose third request has used+growth one byte
// short of the capacity of its window and used one byte short of
// -window-bytes, or both exactly reached, with retrieval growth.
func handoverThresholds() []frozenScenario {
	var out []frozenScenario
	for _, reached := range []bool{false, true} {
		output := strings.Repeat("ok  \texample.com/build/pkg\t0.012s\n", 121)
		sc := frozenScenario{
			name: "handover-thresholds-short", limits: core.Limits{MaxSteps: 20, MaxAttempts: 3},
			contextBytes: 87344, windowBytes: 4937, outputs: map[string]string{"go test ./...": output},
			runs: []frozenRun{{id: corpusRun, goal: "Run the tests and report the result.", replies: []string{
				corpusDecision(core.Exec, "go test ./..."),
				corpusRetrieve("read", corpusSteps+"/00000001/preview", `,"limit":64`, "Check the first result."),
				corpusDecision(core.Return, "Every package passes."),
			}, steps: []string{"1 exited 0 output complete", "2 exited 0 retrieval 1 sources 0 entries more"}}},
		}
		off := -1
		if reached {
			off = 0
			sc.name, sc.contextBytes, sc.windowBytes = "handover-thresholds-reached", sc.contextBytes-1, sc.windowBytes-1
			r := &sc.runs[0]
			text := "The tests ran; every package passed. Next: report it."
			r.replies = slices.Insert(r.replies, 2, corpusDecision(core.Exec,
				`canti:handover {"run_id":"`+corpusRun+`","first_step":1,"text":"`+text+strings.Repeat(" ", 2048-len(text))+`"}`))
			r.steps = append(r.steps, "3 exited 0 handover")
		}
		sc.edge = func(t *testing.T, requests map[string]harness.Request) {
			h := requests["run1-step03-rev1.json"].Context.Handover
			if h.Growth == 0 || h.Used+h.Growth != h.Capacity+off || h.Used != h.WindowBytes+off {
				t.Errorf("step 3 evaluated %+v; want used+growth %d byte(s) from the capacity and used as far from window_bytes, with growth", h, off)
			}
			if w := requests["run1-step04-rev1.json"].Window; reached && (w == nil || w.FirstStep != 4 || len(w.Handover) != 2048) {
				t.Errorf("step 4 has window %+v; want one opened by a handover of exactly 2048 bytes", w)
			}
		}
		out = append(out, sc)
	}
	return out
}

// sessionBoundaries are twins one byte apart whose last run selects the
// earlier session dialogue at a threshold: a stopped run with its goal or
// only its status, its status or nothing with an older returned run that
// would fit, and the navigation line or no dialogue at all.
func sessionBoundaries() []frozenScenario {
	limits := core.Limits{MaxSteps: 1, MaxAttempts: 3}
	stopped := frozenRun{id: corpusNextRun, goal: strings.Repeat("Collect every build log under /workspace/logs. ", 40),
		replies: []string{corpusDecision(core.Exec, "ls /workspace")}, steps: []string{"1 exited 0 output complete"}, stop: core.StopStepsExhausted}
	small := frozenRun{id: corpusRun, goal: "A.", replies: []string{corpusDecision(core.Return, "B.")}}
	current := frozenRun{id: corpusThirdRun, goal: "What did the earlier runs find? " + strings.Repeat("List each log they collected with its size. ", 60),
		replies: []string{corpusDecision(core.Return, "See the session records.")}}
	type boundary struct {
		name         string
		contextBytes int
		runs         []frozenRun
		sources      string // the selection the last run records
	}
	var out []frozenScenario
	for _, b := range []boundary{
		{"session-stopped-goal", 92138, []frozenRun{small, stopped, current}, corpusNextRun + " stopped goal true"},
		{"session-stopped-status", 92137, []frozenRun{small, stopped, current}, corpusNextRun + " stopped goal false"},
		{"session-status-only", 90231, []frozenRun{small, stopped, current}, corpusNextRun + " stopped goal false"},
		{"session-navigation-only", 90230, []frozenRun{small, stopped, current}, "navigation " + corpusNextRun},
		{"session-navigation", 90015, []frozenRun{small, stopped, current}, "navigation " + corpusNextRun},
		{"session-without-dialogue", 90014, []frozenRun{small, stopped, current}, "none"},
	} {
		last := fmt.Sprintf("run%d-step01-rev1.json", len(b.runs))
		out = append(out, frozenScenario{name: b.name, limits: limits, contextBytes: b.contextBytes, runs: b.runs,
			outputs: map[string]string{"ls /workspace": "logs\n"},
			edge: func(t *testing.T, requests map[string]harness.Request) {
				if got := selection(requests[last].Context); got != b.sources {
					t.Errorf("the last run selected %q; want %q", got, b.sources)
				}
			}})
	}
	return out
}

func selection(c *harness.ContextCapture) string {
	if c == nil || c.SessionNavigation == "" {
		return "none"
	}
	if len(c.Session) == 0 {
		return "navigation " + c.SessionNavigation
	}
	var out []string
	for _, s := range c.Session {
		out = append(out, fmt.Sprintf("%s %s goal %v", s.RunID, s.Status, s.IncludedGoal))
	}
	return strings.Join(out, ", ")
}
