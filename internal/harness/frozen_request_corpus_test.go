package harness_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

var updateRequests = flag.Bool("update-requests", false, "rewrite testdata/frozen-requests from the records the scenarios commit")

const (
	corpusRun     = "run_7f3a9c2e5b8d4f1a6c0e9b2d7a4f8c31"
	corpusNextRun = "run_c84e1b9f0a2d4c7e8b3f6a1d5e9c2b70"
	corpusSteps   = "/canti/runs/" + corpusRun + "/steps"
	corpusListing = `printf 'alpha\nbeta\ngamma\n'`
)

type frozenScenario struct {
	name         string
	tool         bool
	limits       core.Limits
	contextBytes int // PromptLimits.Total instead of 256 KiB
	windowSteps  int
	windowBytes  int
	outputs      map[string]string
	finals       map[string]harness.Job                  // final job of a command instead of success
	imports      map[string]string                       // files of a read-only imported history archive
	changes      map[string]func(imports *os.Root) error // what a command does to the imported archive while it runs
	runs         []frozenRun
	marks        map[string][]string
	edge         func(t *testing.T, requests map[string]harness.Request) // checks the boundary the scenario sits on
}

type frozenRun struct {
	id, goal    string
	constraints string // instead of the corpus constraints
	replies     []string
	steps       []string // stepOutcome of every completed step, oldest first
	budget      harness.Budget
	resolve     *harness.Observation // operator resolution of an unknown outcome
	stop        core.StopReason      // the reason the run stops with instead of returning
}

func corpusDecision(kind core.Kind, payload string) string {
	b, err := json.Marshal(core.Decision{Kind: kind, Payload: payload})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func corpusRetrieve(operation, path, extra, reason string) string {
	return corpusDecision(core.Exec, `canti:retrieve {"operation":"`+operation+`","path":"`+path+`"`+extra+`,"reason":"`+reason+`"}`)
}

func frozenScenarios() []frozenScenario {
	stepLimits := core.Limits{MaxSteps: 20, MaxAttempts: 3}
	ls := map[string]string{"ls /workspace": "go.mod\nmain.go\n"}
	return slices.Concat([]frozenScenario{
		{
			name: "shell-and-retrieval", limits: stepLimits, outputs: map[string]string{corpusListing: "alpha\nbeta\ngamma\n"},
			runs: []frozenRun{{id: corpusRun, goal: "Find which line of the listing mentions beta.", replies: []string{
				corpusDecision(core.Exec, corpusListing),
				corpusRetrieve("read", corpusSteps+"/00000001/preview", "", "Read the listing preview."),
				corpusRetrieve("list", corpusSteps, "", "List the recorded steps."),
				corpusRetrieve("search", corpusSteps+"/00000001/preview", `,"query":"beta"`, "Find beta in the preview."),
				corpusRetrieve("search", corpusSteps+"/00000001", `,"query":"gamma"`, "Find gamma in the step record."),
				corpusRetrieve("discover", corpusSteps, `,"query":"beta"`, "Find the step that printed beta."),
				corpusDecision(core.Return, "Line 2 of the listing mentions beta."),
			}, steps: []string{
				"1 exited 0 output complete",
				"2 done retrieval 1 sources 0 entries",
				"3 done retrieval 0 sources 2 entries",
				"4 done retrieval 1 sources 0 entries",
				"5 done retrieval 2 sources 0 entries",
				"6 done retrieval 3 sources 0 entries",
			}}},
			marks: map[string][]string{
				"run1-step01-rev1.json": {"Step 1 of at most 20, attempt 1 of 3.", "## Current window steps\nNone.\n"},
				"run1-step02-rev1.json": {"### Step 1\n", "Execution: exited, exit code 0\nOutput: 17 bytes, complete", "Preview:\n```\nalpha\nbeta\ngamma\n```\n"},
				"run1-step03-rev1.json": {"### Step 2\n", "Retrieved original evidence:\n"},
				"run1-step04-rev1.json": {"### Step 3\n", "Entries: [\"" + corpusSteps + "/00000001\",\"" + corpusSteps + "/00000002\"]"},
				"run1-step05-rev1.json": {"### Step 4\n", `"operation":"search"`, "Retrieved original evidence:\n"},
				"run1-step06-rev1.json": {"### Step 5\n", "Path " + corpusSteps + "/00000001/command:\n"},
				"run1-step07-rev1.json": {"### Step 6\n", "Retrieval: 3 candidate ranges", "Path /canti/artifacts/effect_"},
			},
		},
		{
			name: "handover", limits: stepLimits, windowSteps: 1, outputs: ls,
			runs: []frozenRun{{id: corpusRun, goal: "Report the files in /workspace.", replies: []string{
				corpusDecision(core.Exec, "ls /workspace"),
				corpusDecision(core.Exec, harness.HandoverPrefix+"Listed /workspace: go.mod and main.go. Next: report them."),
				corpusDecision(core.Return, "The workspace holds go.mod and main.go."),
			}, steps: []string{"1 exited 0 output complete", "2 done handover"}}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"### Step 1\n", "## Handover required\n"},
				"run1-step03-rev1.json": {"This window begins at step 3.", "## Handover\n", "Next: report them.", "## Current window steps\nNone.\n"},
			},
		},
		{
			name: "protocol-repair", limits: stepLimits,
			runs: []frozenRun{{id: corpusRun, goal: "Report the files in /workspace.", replies: []string{
				"I will start by listing the workspace.",
				strings.Repeat("`", 2048),
				corpusDecision(core.Return, "I need the workspace path first."),
			}}},
			marks: map[string][]string{
				"run1-step01-rev2.json": {"attempt 2 of 3.", "## Rejected response\n", "I will start by listing the workspace."},
				"run1-step01-rev3.json": {"attempt 3 of 3.", "Rejected reply:\n```\n…[truncated 2048 bytes]\n```\n"},
			},
		},
		{
			name: "session-dialogue", limits: stepLimits, outputs: ls,
			runs: []frozenRun{
				{id: corpusRun, goal: "\tWhat is in /workspace?  \n", replies: []string{
					corpusDecision(core.Exec, "ls /workspace"),
					corpusDecision(core.Return, "  go.mod and main.go.\n"),
				}, steps: []string{"1 exited 0 output complete"}},
				{id: corpusNextRun, goal: "Which of those files is the entry point?", replies: []string{
					corpusDecision(core.Return, "main.go is the entry point."),
				}},
			},
			marks: map[string][]string{
				"run1-step01-rev1.json": {"## Goal\nWhat is in /workspace?\n\n"},
				"run2-step01-rev1.json": {"## Earlier session dialogue\n", "Earlier user goal:\n```\nWhat is in /workspace?\n```\n",
					"Earlier assistant return:\n```\n  go.mod and main.go.\n```\n"},
			},
		},
		{
			name: "unlimited-steps", limits: core.Limits{MaxAttempts: 3}, outputs: ls,
			runs: []frozenRun{{id: corpusRun, goal: "Report the files in /workspace.", replies: []string{
				corpusDecision(core.Exec, harness.HandoverPrefix+"Nothing done yet."),
				corpusDecision(core.Exec, "ls /workspace"),
				corpusDecision(core.Return, "go.mod and main.go."),
			}, steps: []string{"1 failed", "2 exited 0 output complete"}}},
			marks: map[string][]string{
				"run1-step01-rev1.json": {"Step 1 (unlimited steps), attempt 1 of 3."},
				"run1-step02-rev1.json": {"Step 2 (unlimited steps), attempt 1 of 3.", "Context action failed: handover was not requested for this step"},
			},
		},
		{
			name: "tool-transport", tool: true, limits: stepLimits, outputs: ls,
			runs: []frozenRun{{id: corpusRun, goal: "Report the files in /workspace.", replies: []string{
				`{"kind":"exec","payload":"ls /workspace"}`,
				`{"kind":"exec"}`,
				`null`,
				`{"kind":"return","payload":"go.mod and main.go."}`,
			}, steps: []string{"1 exited 0 output complete"}}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"### Step 1\n"},
				"run1-step02-rev2.json": {"attempt 2 of 3.", "## Rejected response\n", `{"kind":"exec"}`},
				"run1-step02-rev3.json": {"attempt 3 of 3.", "Rejected reply:\n```\n{\"id\":\"msg_03\"", `"input":null`},
			},
		},
	}, edgeScenarios(), failureScenarios(), retrievalScenarios(), carryScenarios(), boundaryScenarios())
}

// TestLoopScenariosKeepTheirCommittedRecords compares the exact body of each
// model request every scenario freezes and the event IDs and facts of its
// transitions with testdata/frozen-requests/<scenario>.
// Rewrite them with -update-requests only for an intended prompt, adapter, or
// record change.
func TestLoopScenariosKeepTheirCommittedRecords(t *testing.T) {
	scenarios := frozenScenarios()
	orphans := checkScenarioDirectories(t, scenarios)
	defer func() {
		if *updateRequests && !t.Failed() {
			for _, dir := range orphans {
				if err := os.RemoveAll(dir); err != nil {
					t.Error(err)
				}
			}
		}
	}()
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			rig := newCorpusRig(t, sc)
			rig.drive(t, sc)
			adapter := "canti-anthropic/2"
			if sc.tool {
				adapter = "canti-anthropic/3-tool"
			}
			got, requests := map[string][]byte{}, map[string]harness.Request{}
			var committed [][]byte
			for i, run := range sc.runs {
				rig.checkOutcomes(t, run)
				for _, tr := range rig.frozen(t, run.id) {
					if tr.Facts.Request.Adapter != adapter {
						t.Errorf("run %s step %d adapter = %q, want %q", run.id, tr.To.Step, tr.Facts.Request.Adapter, adapter)
					}
					name := fmt.Sprintf("run%d-step%02d-rev%d.json", i+1, tr.To.Step, tr.To.RequestRev)
					got[name], requests[name] = tr.Facts.Request.Body, *tr.Facts.Request
					committed = append(committed, tr.Facts.Request.Body)
				}
			}
			if sc.edge != nil {
				sc.edge(t, requests)
			}
			got["journal.txt"] = rig.journal(t, sc.runs)
			sent := rig.model.sent()
			if len(sent) != len(committed) {
				t.Fatalf("sent %d requests for %d frozen requests", len(sent), len(committed))
			}
			for i := range sent {
				if !bytes.Equal(sent[i], committed[i]) {
					t.Errorf("request %d sent other bytes than it committed", i+1)
				}
			}
			for name, marks := range sc.marks {
				body, ok := got[name]
				if !ok {
					t.Errorf("scenario froze no request %s", name)
					continue
				}
				text := promptText(t, body)
				for _, mark := range marks {
					if !strings.Contains(text, mark) {
						t.Errorf("%s does not hold %q", name, mark)
					}
				}
			}
			dir := filepath.Join("testdata", "frozen-requests", sc.name)
			if *updateRequests {
				if t.Failed() {
					t.Fatal("-update-requests keeps the goldens of a failing scenario")
				}
				writeFrozenRequests(t, dir, got)
				return
			}
			compareFrozenRequests(t, dir, got)
		})
	}
}

func (r *corpusRig) checkOutcomes(t *testing.T, run frozenRun) {
	t.Helper()
	steps, err := r.store.Recent(context.Background(), run.id, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, s := range steps {
		got = append(got, stepOutcome(s))
	}
	if !slices.Equal(got, run.steps) {
		t.Errorf("run %s ended its steps as\n%s\nwant\n%s", run.id, strings.Join(got, "\n"), strings.Join(run.steps, "\n"))
	}
}

// stepOutcome reports how a completed step ended: its execution, exit code,
// explicit resolution, and the result it recorded.
func stepOutcome(s harness.StepRecord) string {
	if o := s.Outcome; o != nil {
		out := fmt.Sprintf("%d %s", s.Step, o.Status)
		switch r := o.Retrieval; {
		case r != nil && r.Error != "":
			out += " retrieval error"
		case r != nil:
			out += fmt.Sprintf(" retrieval %d sources %d entries", len(r.Sources), len(r.Entries))
			if r.More {
				out += " more"
			}
		case o.Handover != nil:
			out += " handover"
		}
		return out
	}
	o := s.Observation
	out := fmt.Sprintf("%d %s", s.Step, o.Execution)
	if o.ExitCode != nil {
		out += fmt.Sprintf(" %d", *o.ExitCode)
	}
	if s.Resolved {
		out += " resolved"
	}
	switch {
	case o.Output.Complete:
		out += " output complete"
	case o.Output.Available:
		out += " output incomplete"
	default:
		out += " output unavailable"
	}
	return out
}

// journal lists the sequence, kind, event ID, and facts of every transition
// the runs committed, as the transitions table holds them, with the body of
// each frozen request replaced by the name of its golden.
func (r *corpusRig) journal(t *testing.T, runs []frozenRun) []byte {
	t.Helper()
	db, err := sql.Open("sqlite", r.path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var b bytes.Buffer
	for i, run := range runs {
		rows, err := db.Query(`SELECT seq, kind, event_id, step, rev, facts FROM transitions WHERE run_id = ? ORDER BY seq`, run.id)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var seq int64
			var step, rev int
			var kind, id, facts string
			if err := rows.Scan(&seq, &kind, &id, &step, &rev, &facts); err != nil {
				t.Fatal(err)
			}
			if kind == "request_frozen" {
				facts = elideBody(t, facts, fmt.Sprintf("run%d-step%02d-rev%d.json", i+1, step, rev))
			}
			fmt.Fprintf(&b, "run%d %d %s %s %s\n", i+1, seq, kind, id, facts)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
	}
	return b.Bytes()
}

func elideBody(t *testing.T, facts, name string) string {
	t.Helper()
	var f struct{ Request struct{ Body []byte } }
	if err := json.Unmarshal([]byte(facts), &f); err != nil {
		t.Fatal(err)
	}
	body := `"Body":"` + base64.StdEncoding.EncodeToString(f.Request.Body) + `"`
	if strings.Count(facts, body) != 1 {
		t.Fatalf("facts of %s hold its body %d times", name, strings.Count(facts, body))
	}
	return strings.Replace(facts, body, `"Body":"`+name+`"`, 1)
}

// checkScenarioDirectories fails on a golden directory that no scenario
// records, or with -update-requests returns it for removal once every
// scenario passed.
func checkScenarioDirectories(t *testing.T, scenarios []frozenScenario) (orphans []string) {
	t.Helper()
	root := filepath.Join("testdata", "frozen-requests")
	names := map[string]bool{}
	for _, sc := range scenarios {
		if names[sc.name] {
			t.Fatalf("two scenarios are named %s", sc.name)
		}
		names[sc.name] = true
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		switch {
		case names[e.Name()]:
		case *updateRequests:
			orphans = append(orphans, filepath.Join(root, e.Name()))
		default:
			t.Errorf("%s holds %s, which no scenario records; -update-requests removes it", root, e.Name())
		}
	}
	return orphans
}

func writeFrozenRequests(t *testing.T, dir string, got map[string][]byte) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range got {
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func compareFrozenRequests(t *testing.T, dir string, got map[string][]byte) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("%v; record the goldens with -update-requests", err)
	}
	var stored []string
	for _, e := range entries {
		stored = append(stored, e.Name())
	}
	var names []string
	for name := range got {
		names = append(names, name)
	}
	slices.Sort(names)
	if !slices.Equal(stored, names) {
		t.Errorf("%s holds %v, the scenario froze %v", dir, stored, names)
	}
	for _, name := range names {
		want, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Error(err)
			continue
		}
		if body := got[name]; !bytes.Equal(body, want) {
			at := 0
			for at < min(len(body), len(want)) && body[at] == want[at] {
				at++
			}
			from := max(0, at-120)
			t.Errorf("%s differs at byte %d of %d (golden %d bytes):\ngot  …%s…\nwant …%s…", name, at, len(body), len(want),
				body[from:min(len(body), at+120)], want[from:min(len(want), at+120)])
		}
	}
}

// TestEarlierVersionRequestsAreSentAsFrozen requires that a loop of the
// current context version sends a request that a run froze under an earlier
// version byte for byte, although the current rules render that request
// otherwise: a frozen request is never rebuilt. Each retained version is
// checked by one of its own requests.
func TestEarlierVersionRequestsAreSentAsFrozen(t *testing.T) {
	for _, tc := range []struct{ version, name string }{
		{"13", filepath.Join("handover", "run1-step02-rev1.json")},
		{"14", filepath.Join("handover-carry", "run1-step03-rev1.json")},
	} {
		t.Run("version "+tc.version, func(t *testing.T) {
			frozen, err := os.ReadFile(filepath.Join("testdata", "frozen-requests-v"+tc.version, tc.name))
			if err != nil {
				t.Fatal(err)
			}
			current, err := os.ReadFile(filepath.Join("testdata", "frozen-requests", tc.name))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(frozen, current) {
				t.Fatal("the retained request equals the current rendering, so sending it proves nothing")
			}
			f := newFixture(t, limits, harness.Budget{}, returnReply("done"))
			ctx := f.bounded()
			runner, err := harness.NewRunner(f.store, f.exec)
			if err != nil {
				t.Fatal(err)
			}
			snap, err := f.store.Latest(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
			req := harness.Request{Body: frozen, Model: "fake-1", Protocol: "fake/v1", Adapter: "fake", Window: &harness.Window{FirstStep: 1, Requested: true}}
			if _, err := runner.Commit(ctx, snap, core.RequestFrozen{}, harness.Facts{Request: &req}); err != nil {
				t.Fatal(err)
			}
			wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
			if prompts, sent := f.model.calls(); prompts != 0 || sent != 1 || !bytes.Equal(f.model.sent[0], frozen) {
				t.Fatalf("froze %d prompts and sent %d requests; want the version-%s bytes sent unchanged", prompts, sent, tc.version)
			}
		})
	}
}
