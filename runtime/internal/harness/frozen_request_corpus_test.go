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

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

var updateRequests = flag.Bool("update-requests", false, "rewrite testdata/frozen-requests from the records the scenarios commit")

const (
	corpusRun      = "run_7f3a9c2e5b8d4f1a6c0e9b2d7a4f8c31"
	corpusNextRun  = "run_c84e1b9f0a2d4c7e8b3f6a1d5e9c2b70"
	corpusThirdRun = "run_5d1e8c3a7b2f4e9d6a0c8b5f3e1d9a72"
	corpusSteps    = "/canti/runs/" + corpusRun + "/steps"
	corpusListing  = `printf 'alpha\nbeta\ngamma\n'`
	corpusVersion  = 20
)

type frozenScenario struct {
	name          string
	nativeArchive bool
	tool          bool
	limits        core.Limits
	contextBytes  int // PromptLimits.Total instead of 256 KiB
	outputs       map[string]string
	finals        map[string]harness.Job                  // final job of a command instead of success
	imports       map[string]string                       // files of a read-only imported history archive
	changes       map[string]func(imports *os.Root) error // what a command does to the imported archive while it runs
	observations  []harness.InputSubmission               // observations admitted before the first run
	runs          []frozenRun
	marks         map[string][]string
	edge          func(t *testing.T, requests map[string]harness.Request) // checks the boundary the scenario sits on
}

type frozenRun struct {
	id, goal    string
	constraints string // instead of the corpus constraints
	fresh       bool   // starts a new line
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
	limits := core.Limits{MaxSteps: 20, MaxAttempts: 4}
	overflow := `raw:{"model":"claude-opus-5","stop_reason":"model_context_window_exceeded","content":[{"type":"text","text":"{\"kind\":\"exec\",\"payload\":\"never execute partial\"}"}],"usage":{"input_tokens":13,"output_tokens":2}}`
	handover := func(text string) string { return corpusDecision(core.Exec, "canti:handover "+text) }
	return slices.Concat(persistentScenarios(limits), []frozenScenario{
		{name: "fresh-run", limits: limits, runs: []frozenRun{{id: corpusRun, goal: "Answer immediately.", replies: []string{corpusDecision(core.Return, "done")}}},
			marks: map[string][]string{"run1-step01-rev1.json": {"## Steps since the anchor", "anchor sequence 0"}}},
		{name: "shell-and-retrieval", limits: limits, outputs: map[string]string{corpusListing: "alpha\nbeta\ngamma\n"}, runs: []frozenRun{{id: corpusRun, goal: "Find beta in original history.", replies: []string{corpusDecision(core.Exec, corpusListing), corpusRetrieve("read", corpusSteps+"/00000001/preview", "", "Read the preview."), corpusRetrieve("search", corpusSteps+"/00000001/preview", `,"query":"beta"`, "Find beta."), corpusRetrieve("discover", corpusSteps, `,"query":"beta"`, "Find original step."), corpusDecision(core.Return, "beta is line 2.")}, steps: []string{"1 exited 0 output complete", "2 done retrieval 1 sources 0 entries", "3 done retrieval 1 sources 0 entries", "4 done retrieval 3 sources 0 entries"}}},
			marks: map[string][]string{"run1-step05-rev1.json": {"### Step 1\n", "### Step 4\n", "Command:\n```\n" + corpusListing + "\n```", "Path " + corpusSteps + "/00000001/preview:"}}},
		{name: "mid-epoch", limits: limits, outputs: map[string]string{"ls": "alpha\n", "cat alpha": "alpha\n"}, runs: []frozenRun{{id: corpusRun, goal: "Keep the epoch resident.", replies: []string{corpusDecision(core.Exec, "ls"), corpusDecision(core.Exec, "cat alpha"), corpusRetrieve("read", corpusSteps+"/00000001/preview", "", "Read alpha."), corpusDecision(core.Return, "done")}, steps: []string{"1 exited 0 output complete", "2 exited 0 output complete", "3 done retrieval 1 sources 0 entries"}}},
			marks: map[string][]string{"run1-step04-rev1.json": {"### Step 1\n", "### Step 2\n", "### Step 3\n", "## Steps since the anchor"}}},
		{name: "after-agent-handover", limits: limits, outputs: map[string]string{"ls": "go.mod\n"}, runs: []frozenRun{{id: corpusRun, goal: "Close a phase.", replies: []string{handover("gathered: go.mod exists; next list the tests"), corpusDecision(core.Exec, "ls"), corpusDecision(core.Return, "done")}, steps: []string{"1 done handover 44 bytes", "2 exited 0 output complete"}}},
			marks: map[string][]string{"run1-step02-rev1.json": {"## Handover", "gathered: go.mod exists", "## Steps since the anchor"}, "run1-step03-rev1.json": {"## Handover", "### Step 2\n"}}},
		{name: "reset-handover-repair", limits: limits, imports: map[string]string{"notes": "FROZEN_ORIGINAL_NOTES"}, runs: []frozenRun{{id: corpusRun, goal: "Recover original notes across provider rejection.", replies: []string{corpusRetrieve("read", "/canti/imports/notes", "", "Read notes."), overflow, handover("recovered notes: FROZEN_ORIGINAL_NOTES"), "invalid decision", corpusDecision(core.Return, "Recovered notes.")}, steps: []string{"1 done retrieval 1 sources 0 entries", "2 done handover 38 bytes"}}},
			marks: map[string][]string{"run1-step02-rev2.json": {"## Earlier steps", "## Context anchor"}, "run1-step03-rev1.json": {"## Handover", "FROZEN_ORIGINAL_NOTES"}, "run1-step03-rev2.json": {"## Protocol repair"}}},
		{name: "rejection-handover-index", limits: limits, outputs: map[string]string{"ls": "a\n", "cat a": "a\n"}, runs: []frozenRun{{id: corpusRun, goal: "Index the uncovered steps.", replies: []string{handover("phase one done"), corpusDecision(core.Exec, "ls"), corpusDecision(core.Exec, "cat a"), overflow, corpusDecision(core.Return, "done")}, steps: []string{"1 done handover 14 bytes", "2 exited 0 output complete", "3 exited 0 output complete"}}},
			marks: map[string][]string{"run1-step04-rev2.json": {"## Handover", "phase one done", "## Earlier steps", "- Step 2:", "- Step 3:"}}},
		{name: "linked-run-inherits-handover", limits: limits, runs: []frozenRun{{id: corpusRun, goal: "Carry the phase.", replies: []string{handover("inherited phase note"), corpusDecision(core.Return, "done")}, steps: []string{"1 done handover 20 bytes"}}, {id: corpusNextRun, goal: "Continue the line.", replies: []string{corpusDecision(core.Return, "done")}}},
			marks: map[string][]string{"run2-step01-rev1.json": {"## Handover", "inherited phase note", "From an earlier run of this line.", "run " + corpusRun}}},
		{name: "fresh-line-no-inheritance", limits: limits, runs: []frozenRun{{id: corpusRun, goal: "Carry the phase.", replies: []string{handover("must not cross a fresh line"), corpusDecision(core.Return, "done")}, steps: []string{"1 done handover 27 bytes"}}, {id: corpusThirdRun, fresh: true, goal: "Start a new line.", replies: []string{corpusDecision(core.Return, "done")}}},
			edge: func(t *testing.T, requests map[string]harness.Request) {
				body := string(requests["run2-step01-rev1.json"].Body)
				if strings.Contains(body, "must not cross a fresh line") || strings.Contains(body, "## Handover") || strings.Contains(body, corpusRun) {
					t.Fatalf("a fresh line inherited a handover: %s", body)
				}
			}},
		{name: "minimal-after-rejected-optional", limits: limits, runs: []frozenRun{{id: corpusRun, goal: "Carry the phase.", replies: []string{handover("the inherited note"), corpusDecision(core.Return, "done")}, steps: []string{"1 done handover 18 bytes"}}, {id: corpusNextRun, goal: "Reject the optional context floor.", replies: []string{overflow, corpusDecision(core.Return, "done")}}},
			marks: map[string][]string{"run2-step01-rev1.json": {"## Handover", "the inherited note"}, "run2-step01-rev2.json": {"## Optional context omitted", "Handover record: /canti/runs/" + corpusRun + "/steps/00000001/command", "/canti/runs/. Inputs: /canti/inputs/."}},
			edge: func(t *testing.T, requests map[string]harness.Request) {
				for name, request := range requests {
					if !strings.HasPrefix(name, "run2-") {
						continue
					}
					capture := request.Context
					if capture != nil && capture.Anchor.Minimal {
						if capture.Bytes.Handover != 0 || capture.Bytes.Index != 0 || capture.Bytes.Previous != 0 || capture.Bytes.Observations != 0 {
							t.Fatalf("%s rendered an optional body in minimal mode: %+v", name, capture.Bytes)
						}
					}
				}
			}},
		{name: "observation-tail", limits: limits, observations: []harness.InputSubmission{
			{Authority: harness.AuthorityObservation, Source: "ci", Subject: "build", Text: "RED", Now: corpusCreated},
			{Authority: harness.AuthorityObservation, Source: "ci", Subject: "build", Text: "GREEN", Now: corpusCreated},
		}, runs: []frozenRun{{id: corpusRun, goal: "Use the latest observation.", replies: []string{corpusDecision(core.Return, "done")}}},
			marks: map[string][]string{"run1-step01-rev1.json": {"GREEN", "Position 2; archive /canti/inputs/000000000002"}}},
		{name: "line-navigation", limits: limits, runs: []frozenRun{{id: corpusRun, goal: "  Earlier exact goal.\n", replies: []string{corpusDecision(core.Return, "Earlier answer.")}}, {id: corpusNextRun, goal: "Current goal.", replies: []string{corpusDecision(core.Return, "Current answer.")}}}, marks: map[string][]string{"run2-step01-rev1.json": {"## Previous runs", "- " + corpusRun + ":", "/canti/runs/" + corpusRun + "/"}}},
		{name: "legacy-actions-are-local-failures", limits: limits, runs: []frozenRun{{id: corpusRun, goal: "Reject retired actions locally.", replies: []string{corpusDecision(core.Exec, "canti:handover-carry next"), corpusDecision(core.Exec, "canti:handover2 x"), corpusDecision(core.Return, "Retired actions failed locally.")}, steps: []string{"1 failed", "2 failed"}}}},
		{name: "tool-transport", tool: true, limits: limits, outputs: map[string]string{"ls": "go.mod\n"}, runs: []frozenRun{{id: corpusRun, goal: "Inspect files.", replies: []string{corpusDecision(core.Exec, "ls"), `{"kind":"exec"}`, corpusDecision(core.Return, "go.mod")}, steps: []string{"1 exited 0 output complete"}}}},
		{name: "protocol-repair", limits: limits, runs: []frozenRun{{id: corpusRun, goal: "Repair a decision.", replies: []string{"not a decision", strings.Repeat("`", 2048), corpusDecision(core.Return, "repaired")}}}},
	}, edgeScenarios(), failureScenarios(), retrievalScenarios(), boundaryScenarios())
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
			adapter := "canti-anthropic/4"
			if sc.tool {
				adapter = "canti-anthropic/4-tool"
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
			dir := filepath.Join("testdata", fmt.Sprintf("frozen-requests-v%d", corpusVersion), sc.name)
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
			out += fmt.Sprintf(" handover %d bytes", len(o.Handover.Text))
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
	root := filepath.Join("testdata", fmt.Sprintf("frozen-requests-v%d", corpusVersion))
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

// TestCurrentVersionRequestsAreSentAsFrozen proves that reopening a current
// journal sends its committed request bytes even if a renderer would differ.
func TestCurrentVersionRequestsAreSentAsFrozen(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, returnReply("done"))
	frozen := []byte("a previously committed current-version request")
	runner, err := harness.NewRunner(f.store)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := f.store.Latest(f.bounded(), runID)
	if err != nil {
		t.Fatal(err)
	}
	req := harness.Request{Body: frozen, Model: "fake-1", Protocol: "fake/v1", Adapter: "fake", ContextState: &harness.ContextState{FirstStep: 1}}
	if _, err = runner.Commit(f.bounded(), snap, core.RequestFrozen{}, harness.Facts{Request: &req}); err != nil {
		t.Fatal(err)
	}
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
	if prompts, sent := f.model.calls(); prompts != 0 || sent != 1 || !bytes.Equal(f.model.sent[0], frozen) {
		t.Fatalf("rendered %d prompts, sent %q", prompts, f.model.sent)
	}
}

func persistentScenarios(limits core.Limits) []frozenScenario {
	overflow := `raw:{"model":"claude-opus-5","stop_reason":"model_context_window_exceeded","content":[{"type":"text","text":"{\"kind\":\"return\",\"payload\":\"partial\"}"}]}`
	return []frozenScenario{{name: "persistent-native-retrieval-reset-handover", nativeArchive: true, limits: limits, imports: map[string]string{"notes": "PERSISTENT_ORIGINAL_EVIDENCE"},
		runs:  []frozenRun{{id: corpusRun, goal: "Read original environment evidence across context reset.", replies: []string{corpusRetrieve("read", "/canti/imports/notes", "", "Read original evidence."), overflow, corpusDecision(core.Exec, "canti:handover PERSISTENT_ORIGINAL_EVIDENCE retained; next report it"), corpusDecision(core.Return, "Evidence retained.")}, steps: []string{"1 done retrieval 1 sources 0 entries", "2 done handover 53 bytes"}}},
		marks: map[string][]string{"run1-step02-rev2.json": {"## Earlier steps", "logical addresses"}, "run1-step03-rev1.json": {"PERSISTENT_ORIGINAL_EVIDENCE", "## Handover"}}}}
}
