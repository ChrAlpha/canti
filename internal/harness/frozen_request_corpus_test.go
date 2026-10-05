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
	name        string
	tool        bool
	limits      core.Limits
	windowSteps int
	outputs     map[string]string
	runs        []frozenRun
	marks       map[string][]string
}

type frozenRun struct {
	id, goal string
	replies  []string
	steps    []string // stepOutcome of every completed step, oldest first
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
				"2 exited 0 retrieval 1 sources 0 entries",
				"3 exited 0 retrieval 0 sources 2 entries",
				"4 exited 0 retrieval 1 sources 0 entries",
				"5 exited 0 retrieval 2 sources 0 entries",
				"6 exited 0 retrieval 3 sources 0 entries",
			}}},
			marks: map[string][]string{
				"run1-step01-rev1.json": {"Step 1 of at most 20, attempt 1 of 3.", "## Current window steps\nNone.\n"},
				"run1-step02-rev1.json": {"### Step 1\n", "Execution: exited, exit code 0\nOutput: 17 bytes, complete", "Preview:\n```\nalpha\nbeta\ngamma\n```\n"},
				"run1-step03-rev1.json": {"### Step 2\n", "Retrieved original evidence:\n"},
				"run1-step04-rev1.json": {"### Step 3\n", "Entries: [\"" + corpusSteps + "/00000001\",\"" + corpusSteps + "/00000002\"]"},
				"run1-step05-rev1.json": {"### Step 4\n", `"operation":"search"`, "Retrieved original evidence:\n"},
				"run1-step06-rev1.json": {"### Step 5\n", "Source " + corpusSteps + "/00000001/command:\n"},
				"run1-step07-rev1.json": {"### Step 6\n", "Retrieval: 3 candidate ranges", "Source /canti/artifacts/effect_"},
			},
		},
		{
			name: "handover", limits: stepLimits, windowSteps: 1, outputs: ls,
			runs: []frozenRun{{id: corpusRun, goal: "Report the files in /workspace.", replies: []string{
				corpusDecision(core.Exec, "ls /workspace"),
				corpusDecision(core.Exec, `canti:handover {"run_id":"`+corpusRun+`","first_step":1,"text":"Listed /workspace: go.mod and main.go. Next: report them."}`),
				corpusDecision(core.Return, "The workspace holds go.mod and main.go."),
			}, steps: []string{"1 exited 0 output complete", "2 exited 0 handover"}}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"### Step 1\n", "## Handover required\n"},
				"run1-step03-rev1.json": {"This window begins at step 3.", "## Handover\n", "Next: report them.", "## Current window steps\nNone.\n"},
			},
		},
		{
			name: "protocol-repair", limits: stepLimits,
			runs: []frozenRun{{id: corpusRun, goal: "Report the files in /workspace.", replies: []string{
				"I will start by listing the workspace.",
				corpusDecision(core.Return, "I need the workspace path first."),
			}}},
			marks: map[string][]string{
				"run1-step01-rev2.json": {"attempt 2 of 3.", "## Rejected response\n", "I will start by listing the workspace."},
			},
		},
		{
			name: "session-dialogue", limits: stepLimits, outputs: ls,
			runs: []frozenRun{
				{id: corpusRun, goal: "What is in /workspace?", replies: []string{
					corpusDecision(core.Exec, "ls /workspace"),
					corpusDecision(core.Return, "go.mod and main.go."),
				}, steps: []string{"1 exited 0 output complete"}},
				{id: corpusNextRun, goal: "Which of those files is the entry point?", replies: []string{
					corpusDecision(core.Return, "main.go is the entry point."),
				}},
			},
			marks: map[string][]string{
				"run2-step01-rev1.json": {"## Earlier session dialogue\n", "What is in /workspace?", "go.mod and main.go."},
			},
		},
		{
			name: "unlimited-steps", limits: core.Limits{MaxAttempts: 3}, outputs: ls,
			runs: []frozenRun{{id: corpusRun, goal: "Report the files in /workspace.", replies: []string{
				corpusDecision(core.Exec, "ls /workspace"),
				corpusDecision(core.Return, "go.mod and main.go."),
			}, steps: []string{"1 exited 0 output complete"}}},
			marks: map[string][]string{
				"run1-step01-rev1.json": {"Step 1 (unlimited steps), attempt 1 of 3."},
				"run1-step02-rev1.json": {"Step 2 (unlimited steps), attempt 1 of 3."},
			},
		},
		{
			name: "tool-transport", tool: true, limits: stepLimits, outputs: ls,
			runs: []frozenRun{{id: corpusRun, goal: "Report the files in /workspace.", replies: []string{
				`{"kind":"exec","payload":"ls /workspace"}`,
				`{"kind":"exec"}`,
				`{"kind":"return","payload":"go.mod and main.go."}`,
			}, steps: []string{"1 exited 0 output complete"}}},
			marks: map[string][]string{
				"run1-step02-rev1.json": {"### Step 1\n"},
				"run1-step02-rev2.json": {"attempt 2 of 3.", "## Rejected response\n", `{"kind":"exec"}`},
			},
		},
	}, edgeScenarios())
}

// TestFrozenRequestBodies records the exact request body committed for each
// model request of every scenario under testdata/frozen-requests. Rewrite
// them with -update-requests only for an intended prompt or adapter change.
func TestFrozenRequestBodies(t *testing.T) {
	for _, sc := range frozenScenarios() {
		t.Run(sc.name, func(t *testing.T) {
			rig := newCorpusRig(t, sc)
			rig.drive(t, sc)
			adapter := "canti-anthropic/2"
			if sc.tool {
				adapter = "canti-anthropic/3-tool"
			}
			got := map[string][]byte{}
			var committed [][]byte
			for i, run := range sc.runs {
				rig.checkOutcomes(t, run)
				for _, tr := range rig.frozen(t, run.id) {
					if tr.Facts.Request.Adapter != adapter {
						t.Errorf("run %s step %d adapter = %q, want %q", run.id, tr.To.Step, tr.Facts.Request.Adapter, adapter)
					}
					got[fmt.Sprintf("run%d-step%02d-rev%d.json", i+1, tr.To.Step, tr.To.RequestRev)] = tr.Facts.Request.Body
					committed = append(committed, tr.Facts.Request.Body)
				}
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
	o := s.Observation
	out := fmt.Sprintf("%d %s", s.Step, o.Execution)
	if o.ExitCode != nil {
		out += fmt.Sprintf(" %d", *o.ExitCode)
	}
	if s.Resolved {
		out += " resolved"
	}
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
	case strings.HasPrefix(o.Preview, "Context action failed: "):
		out += " action failed"
	case o.Output.Complete:
		out += " output complete"
	case o.Output.Available:
		out += " output incomplete"
	default:
		out += " output unavailable"
	}
	return out
}

// journal lists the sequence, kind, and event ID of every transition the
// runs committed, as the transitions table holds them, and the facts column
// of each frozen request with its body replaced by the name of its golden.
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
			fmt.Fprintf(&b, "run%d %d %s %s", i+1, seq, kind, id)
			if kind == "request_frozen" {
				b.WriteString(" " + elideBody(t, facts, fmt.Sprintf("run%d-step%02d-rev%d.json", i+1, step, rev)))
			}
			b.WriteByte('\n')
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
