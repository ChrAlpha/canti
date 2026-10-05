package harness_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

const legacyHandover = "Read evidence ranges [0,24576) of /canti/imports/evidence.txt. Next: summarize them."

// legacyExpectation is what the current harness does when it resumes a
// legacy journal: the decisions its scripted model returns, the event kinds
// it appends, and the run's answer.
type legacyExpectation struct {
	replies   func(runID string) []string
	kinds     []string
	answer    string
	published []int
	check     func(t *testing.T, r legacyResult)
}

func legacyExpectations() map[string]legacyExpectation {
	notes := "The notes are numbered plain-text lines."
	return map[string]legacyExpectation{
		legacySchema1 + "/retrieval-awaiting-observation": {
			replies:   func(string) []string { return []string{corpusDecision(core.Return, notes)} },
			kinds:     append([]string{"observed"}, closing...),
			answer:    notes,
			published: []int{2},
			check: func(t *testing.T, r legacyResult) {
				preview := "/canti/runs/" + r.run.ID + "/steps/00000001/preview"
				data, ok := r.copy.files["history"+preview[len("/canti/runs"):]]
				o := r.suffix[0].Facts.Observation
				if !ok || o == nil || o.Execution != core.ExecutionExited || o.ExitCode == nil || *o.ExitCode != 0 ||
					o.Retrieval == nil || o.Retrieval.Error != "" || len(o.Retrieval.Sources) != 1 {
					t.Fatalf("the paused retrieval observed %+v", o)
				}
				want := harness.SourceRange{Path: preview, Length: len(data), SHA256: legacyHash(data)}
				if got := o.Retrieval.Sources[0]; got != want {
					t.Errorf("the retrieval read %+v, not the producer's published preview %+v", got, want)
				}
				next := r.suffix[1].Facts.Request
				legacySamePrefix(t, r.frozen(t, 2, 1), next.Body, 2)
				_, blocks := legacyBlocks(t, next.Body)
				expanded := slices.Contains(next.Context.Sources, harness.ContextSource{Step: 2, Source: want, Available: true})
				if !expanded || len(blocks) != 4 || !strings.Contains(blocks[2], "Retrieved original evidence:") || !strings.Contains(blocks[2], string(data)) {
					t.Errorf("the next request does not carry the resumed retrieval's evidence: %+v", next.Context.Sources)
				}
			},
		},
		legacySemantics2 + "/request-sent-killed": {
			replies: func(string) []string {
				return []string{
					corpusRetrieve("read", "/canti/imports/notes.txt", "", "Read the imported notes again."),
					corpusDecision(core.Return, notes),
				}
			},
			kinds:     append([]string{"attempt_started", "response_saved", "response_loaded", "job_bound", "job_started", "observed"}, closing...),
			answer:    notes,
			published: []int{2},
			check: func(t *testing.T, r legacyResult) {
				p := r.paused
				if p.Phase != core.PhaseRequestSent {
					t.Fatalf("the fixture is paused in %s", p.Phase)
				}
				if got, want := r.suffix[0].Event, core.Event(core.AttemptStarted{Step: p.Step, Attempt: p.Attempt + 1}); got != want {
					t.Errorf("resume first committed %#v, want %#v counting the lost attempt", got, want)
				}
				if len(r.sent) != 2 || !bytes.Equal(r.sent[0], r.frozen(t, p.Step, p.RequestRev)) {
					t.Error("resume did not resend exactly the request the producer froze")
				}
				next := r.suffix[6].Facts.Request
				legacySamePrefix(t, r.frozen(t, p.Step, p.RequestRev), next.Body, 2)
				if _, blocks := legacyBlocks(t, next.Body); !strings.Contains(blocks[0], "## Earlier session dialogue\n") || len(next.Context.Session) != 1 {
					t.Errorf("the next request does not carry the earlier session run: %+v", next.Context.Session)
				}
			},
		},
		legacySemantics2 + "/capacity-unrequested": {
			replies: func(run string) []string {
				return []string{
					corpusRetrieve("read", "/canti/imports/evidence.txt", `,"offset":16384,"limit":8192`, "Read the third evidence range."),
					corpusDecision(core.Exec, `canti:handover {"run_id":"`+run+`","first_step":1,"text":"`+legacyHandover+`"}`),
					corpusDecision(core.Return, "The evidence is numbered plain-text lines."),
				}
			},
			kinds:     slices.Concat(cycle, cycle, closing),
			answer:    "The evidence is numbered plain-text lines.",
			published: []int{2, 3, 4},
			check: func(t *testing.T, r legacyResult) {
				var requests []*harness.Request
				var steps []int
				for _, tr := range r.suffix {
					if tr.Facts.Request != nil {
						requests, steps = append(requests, tr.Facts.Request), append(steps, tr.To.Step)
					}
				}
				if !slices.Equal(steps, []int{3, 4, 5}) {
					t.Fatalf("resume froze requests for steps %v", steps)
				}
				probe, next := r.copy.manifest.Capacity.Probe, requests[0]
				got := legacyHandoverCheck{Step: steps[0], Status: next.Context.Handover.Status, Used: next.Context.Handover.Used,
					Capacity: next.Context.Handover.Capacity, Growth: next.Context.Handover.Growth, Requested: next.Window.Requested,
					Reasons: next.Context.Handover.Reasons, Body: legacyHash(next.Body)}
				got.Headroom = got.Capacity - got.Used - got.Growth
				if !reflect.DeepEqual(got, probe) || probe.Requested || probe.Headroom <= 0 {
					t.Errorf("the next request evaluated %+v; the producer evaluated %+v", got, probe)
				}
				legacySamePrefix(t, r.frozen(t, 2, 1), next.Body, 2)
				if w := requests[1].Window; !w.Requested || !slices.Equal(requests[1].Context.Handover.Reasons, []string{"capacity"}) {
					t.Errorf("the request after one more read did not request a handover for capacity: %+v %+v", w, requests[1].Context.Handover)
				}
				if w := *requests[2].Window; w != (harness.Window{FirstStep: 5, SourceStep: 4, Handover: legacyHandover}) {
					t.Errorf("the handover opened window %+v", w)
				}
			},
		},
	}
}

// TestLegacyJournals opens every journal of the corpus with the current
// store, reads it through every journal, session, and context read, resumes
// its paused run with the current loop, and requires that no recorded row
// changes except the run counters a commit raises.
func TestLegacyJournals(t *testing.T) {
	names, expectations := legacyCaseNames(t), legacyExpectations()
	for name := range expectations {
		if !slices.Contains(names, name) {
			t.Errorf("legacy journal %s is missing", name)
		}
	}
	var stored int64
	for _, name := range names {
		x, ok := expectations[name]
		if !ok {
			t.Errorf("legacy journal %s has no expectation", name)
			continue
		}
		t.Run(name[:7]+"/"+filepath.Base(name), func(t *testing.T) { stored += testLegacyJournal(t, name, x) })
	}
	if stored > legacyMaxCorpus {
		t.Errorf("the legacy corpus stores %d bytes, more than %d", stored, legacyMaxCorpus)
	}
}

func testLegacyJournal(t *testing.T, name string, x legacyExpectation) int64 {
	c := openLegacyCase(t, name)
	before := snapshotLegacy(t, c.db)
	summary := legacySummary(t, before)
	summary.DB = c.manifest.Journal.DB
	if !reflect.DeepEqual(summary, c.manifest.Journal) {
		t.Fatalf("the journal does not match its manifest:\n got %+v\nwant %+v", summary, c.manifest.Journal)
	}
	checkLegacyManifest(t, c, before)
	runs := before.runs(t)
	paused := runs[len(runs)-1].text(t, "id")
	ctx := context.Background()
	store, err := sqlite.OpenReadOnly(ctx, c.db)
	if err != nil {
		t.Fatal(err)
	}
	exerciseLegacyReads(t, store, before, paused, before.version != 1)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(c.db); err != nil || legacyHash(data) != c.manifest.Journal.DB.SHA256 {
		t.Fatalf("the read-only store changed the journal file: %v", err)
	}
	compareLegacy(t, before, snapshotLegacy(t, c.db), before.version, nil, nil)
	if store, err = sqlite.Open(ctx, c.db); err != nil {
		t.Fatal(err)
	}
	exerciseLegacyReads(t, store, before, paused, true)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	compareLegacy(t, before, snapshotLegacy(t, c.db), 2, nil, nil)
	roots := legacyRootFiles(t, c.roots)
	r := resumeLegacy(t, c, before, x)
	output := legacyBuiltinOutput(t, before, paused)
	var grown legacyCounters
	for _, tr := range r.suffix {
		switch tr.Event.(type) {
		case core.ResponseSaved:
			grown.input, grown.output = grown.input+legacyUsage.InputTokens, grown.output+legacyUsage.OutputTokens
		case core.Observed, core.Resolved:
			if tr.Facts.Observation.Output != output {
				t.Errorf("seq %d observed built-in output %+v, the producer %+v", tr.Seq, tr.Facts.Observation.Output, output)
			}
			grown.steps, grown.bytes = grown.steps+1, grown.bytes+output.Bytes
		}
	}
	after := snapshotLegacy(t, c.db)
	compareLegacy(t, before, after, 2, map[string]legacyCounters{paused: grown}, map[string]int{paused: len(r.suffix)})
	checkLegacyRoots(t, c, roots, paused, x.answer, x.published)
	if store, err = sqlite.Open(ctx, c.db); err != nil {
		t.Fatal(err)
	}
	exerciseLegacyReads(t, store, after, "", true)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	x.check(t, r)
	return c.stored
}

// checkLegacyManifest checks the manifest's claims about the journal: one
// scripted reply per send, each to the bytes frozen for that attempt, the
// recorded producer paths, and the producer's capacity measurement.
func checkLegacyManifest(t *testing.T, c legacyCopy, db legacyDB) {
	t.Helper()
	m, runs := c.manifest, db.runs(t)
	sends := 0
	for _, row := range runs {
		var run harness.Run
		var cfg legacyConfig
		if err := json.Unmarshal([]byte(row.text(t, "run")), &run); err != nil {
			t.Fatal(err)
		}
		if err := legacyStrict(run.Config, &cfg); err != nil || cfg.Workspace != m.Paths.Workspace || cfg.HistoryImports != m.Paths.Imports {
			t.Fatalf("run %s config %+v does not match the recorded paths %+v: %v", run.ID, cfg, m.Paths, err)
		}
		if m.Capacity != nil && row.text(t, "id") == runs[len(runs)-1].text(t, "id") &&
			(cfg.ContextBytes != m.Capacity.ContextBytes || !reflect.DeepEqual(legacyLastCheck(t, db, run.ID), m.Capacity.Last)) {
			t.Fatalf("run %s does not record the manifest's last capacity check %+v", run.ID, m.Capacity.Last)
		}
		frozen := map[[2]int64][]byte{}
		reply := func(v legacyView) legacyReply {
			i := slices.IndexFunc(m.Replies, func(p legacyReply) bool {
				return int64(p.Run) == row.num(t, "n") && p.Step == v.Step && p.Attempt == v.Attempt
			})
			if i < 0 {
				t.Fatalf("run %s step %d attempt %d has no reply", run.ID, v.Step, v.Attempt)
			}
			return m.Replies[i]
		}
		for _, r := range db.transitions(t, run.ID) {
			v := legacyViewOf(t, r.text(t, "to_state"))
			switch r.text(t, "kind") {
			case "request_frozen":
				frozen[[2]int64{r.num(t, "step"), r.num(t, "rev")}] = legacyFactsOf(t, r).Request.Body
			case "attempt_started":
				if reply(v).Request != legacyHash(frozen[[2]int64{int64(v.Step), int64(v.RequestRev)}]) {
					t.Fatalf("run %s step %d attempt %d has no reply to its frozen request", run.ID, v.Step, v.Attempt)
				}
				sends++
			case "response_saved":
				var body struct{ Content []struct{ Text string } }
				p := reply(v)
				if err := json.Unmarshal(legacyFactsOf(t, r).Response.Body, &body); err != nil || p.Hold || len(body.Content) != 1 || body.Content[0].Text != p.Decision {
					t.Fatalf("run %s step %d attempt %d saved another response than the scripted %q: %v", run.ID, v.Step, v.Attempt, p.Decision, err)
				}
			}
		}
	}
	if sends != len(m.Replies) {
		t.Fatalf("the journal records %d sends, the manifest %d replies", sends, len(m.Replies))
	}
}

// legacyBuiltinOutput returns the output reference the producer recorded for
// the built-in actions of a run, which every pilot observes only; a resumed
// built-in action must record the same.
func legacyBuiltinOutput(t *testing.T, db legacyDB, runID string) harness.ArtifactRef {
	t.Helper()
	var outputs []harness.ArtifactRef
	for _, row := range db.transitions(t, runID) {
		if row.text(t, "kind") != "observed" {
			continue
		}
		var o harness.Observation
		if err := json.Unmarshal([]byte(legacyField(t, row, "Observation")), &o); err != nil {
			t.Fatal(err)
		}
		if o.Retrieval == nil && o.Handover == nil {
			t.Fatalf("seq %d of %s observed a job, not a built-in action", row.num(t, "seq"), runID)
		}
		if outputs = append(outputs, o.Output); o.Output != outputs[0] {
			t.Fatalf("the producer recorded built-in outputs %+v and %+v", outputs[0], o.Output)
		}
	}
	if len(outputs) == 0 {
		t.Fatalf("run %s observed no built-in action", runID)
	}
	return outputs[0]
}

// legacyRootFiles returns the file information of every entry but the
// directories under the roots.
func legacyRootFiles(t *testing.T, roots string) map[string]fs.FileInfo {
	t.Helper()
	files := map[string]fs.FileInfo{}
	err := filepath.WalkDir(roots, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(roots, p)
		if err == nil {
			files[filepath.ToSlash(rel)], err = os.Lstat(p)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// checkLegacyRoots requires that resuming kept every file of the producer's
// roots, which before held, without rewriting or replacing it, and added only
// the step records of the given steps, whose meta has the form of the
// producer's meta of step 1, and the result of the returned run.
func checkLegacyRoots(t *testing.T, c legacyCopy, before map[string]fs.FileInfo, runID, answer string, published []int) {
	t.Helper()
	added := map[string]bool{"history/" + runID + "/result": true}
	for _, step := range published {
		for _, file := range []string{"command", "meta", "preview"} {
			added[fmt.Sprintf("history/%s/steps/%08d/%s", runID, step, file)] = true
		}
	}
	if len(before) != len(c.files) {
		t.Fatalf("the roots held %d files before the resume, the manifest lists %d", len(before), len(c.files))
	}
	after := legacyRootFiles(t, c.roots)
	for name := range c.files {
		if _, ok := after[name]; !ok {
			t.Errorf("the resume removed %s", name)
		}
	}
	first, ok := c.files["history/"+runID+"/steps/00000001/meta"]
	if !ok {
		t.Fatalf("the producer published no meta of step 1 of %s", runID)
	}
	for name, info := range after {
		data, err := os.ReadFile(filepath.Join(c.roots, filepath.FromSlash(name)))
		original, ok := c.files[name]
		switch {
		case err != nil:
			t.Fatal(err)
		case !info.Mode().IsRegular():
			t.Errorf("the resume left %s, which is not a regular file", name)
		case ok && (!bytes.Equal(data, original) || !os.SameFile(info, before[name]) || !info.ModTime().Equal(before[name].ModTime())):
			t.Errorf("the resume rewrote %s", name)
		case !ok && !added[name]:
			t.Errorf("the resume wrote %s", name)
		case !ok && path.Base(name) == "meta":
			step := path.Base(path.Dir(name))
			if want := strings.Replace(string(first), "step: 1\n", "step: "+strings.TrimLeft(step, "0")+"\n", 1); string(data) != want {
				t.Errorf("the resume published %s as %q, want %q", name, data, want)
			}
		case !ok && path.Base(name) == "result" && string(data) != "phase: returned\n\n"+answer:
			t.Errorf("the resume published %s as %q", name, data)
		}
		delete(added, name)
	}
	for name := range added {
		t.Errorf("the resume did not publish %s", name)
	}
}
