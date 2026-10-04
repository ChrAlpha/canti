//go:build integration

package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

// legacySystem is the system prompt that 7624f85, the last revision of
// semantics version 2, froze for both decision formats ahead of the
// retrieval section, which is unchanged since.
const legacySystem = `You are an autonomous agent working in a shell workspace. Each request stands alone: the user message holds everything known about the task, and your reply chooses the next step.

Reply with exactly one JSON object and nothing else: no Markdown, no code fences, no text before or after it. The object has exactly two string fields, "kind" and "payload", in one of these forms:

{"kind":"exec","payload":"<one POSIX shell command>"}
Runs the command with /bin/sh -c in /workspace. Each step runs in a fresh process, and only files under /workspace persist between steps: the working directory, environment variables, and anything outside /workspace do not. Standard output and standard error are captured and reported in the next request.

{"kind":"return","payload":"<final answer, question, or blocker for the user>"}
Ends the run and hands the payload to the user. It does not claim success: use it to deliver a result, ask a question, or report a blocker.

The payload is a JSON string, so escape quotes, backslashes, and newlines.

The user message reports the goal, your progress, and the most recent steps with a bounded preview of each output. Full outputs are files under the artifact directory named in the Environment section, at the path given for each step, and the step records named there keep the full command, outcome, and output path of every completed step, including steps no longer shown.

Work continues across bounded context windows. The handover is the only automatically carried working state. Current goal and constraints remain authoritative. When asked to hand over, use the exact canti:handover action specified in the request; this records guidance and continues the same run.`

// legacySchemas are the two-field decision schemas 7624f85 froze.
var legacySchemas = map[string]string{
	"tool": `[{"name":"canti_decision","description":"Submit the next canti decision as tool input instead of replying with JSON text.","input_schema":{"type":"object","properties":{"kind":{"type":"string","enum":["exec","return"]},"payload":{"type":"string"}},"required":["kind","payload"],"additionalProperties":false}}]`,
	"text": `{"format":{"type":"json_schema","schema":{"type":"object","properties":{"kind":{"type":"string","enum":["exec","return"]},"payload":{"type":"string"}},"required":["kind","payload"],"additionalProperties":false}}}`,
}

// purposeReserves is what the purpose added to the step and the failed
// handover reserves of the work capacity at the default limits.
const purposeReserves = 2 * 562

func openJournal(t *testing.T, f *fixture) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(f.state, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// freezeLegacy rewrites the request frozen at rev of step into the record
// 7624f85 freezes for the same frame: its system prompt and decision schema
// in the body, and the system, total, and work-capacity bytes of its context
// capture. It returns the rewritten body.
func freezeLegacy(t *testing.T, db *sql.DB, format string, step, rev int) []byte {
	t.Helper()
	const where = `kind = 'request_frozen' AND step = ? AND rev = ?`
	var data string
	if err := db.QueryRow(`SELECT facts FROM transitions WHERE `+where, step, rev).Scan(&data); err != nil {
		t.Fatal(err)
	}
	var facts harness.Facts
	if err := strict(data, &facts); err != nil || facts.Request == nil || facts.Request.Context == nil {
		t.Fatalf("request %d/%d: %v", step, rev, err)
	}
	req := facts.Request
	var body struct {
		System       string          `json:"system"`
		Tools        json.RawMessage `json:"tools"`
		OutputConfig json.RawMessage `json:"output_config"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		t.Fatal(err)
	}
	retrieval := strings.Index(body.System, "\n\nTo read historical evidence")
	schema := map[string][]byte{"tool": body.Tools, "text": body.OutputConfig}[format]
	if retrieval < 0 || len(schema) == 0 {
		t.Fatalf("request %d/%d has no retrieval section or %s schema", step, rev, format)
	}
	system := legacySystem + body.System[retrieval:]
	current, _ := json.Marshal(body.System)
	earlier, _ := json.Marshal(system)
	for _, r := range [][2][]byte{{current, earlier}, {schema, []byte(legacySchemas[format])}} {
		if n := bytes.Count(req.Body, r[0]); n != 1 {
			t.Fatalf("request %d/%d holds %d copies of %s", step, rev, n, r[0])
		}
		req.Body = bytes.Replace(req.Body, r[0], r[1], 1)
	}
	shrink := len(body.System) - len(system)
	req.Context.Bytes.System -= shrink
	req.Context.Bytes.Total -= shrink
	if req.Context.Handover.Status == "evaluated" {
		req.Context.Handover.Capacity += shrink + purposeReserves
	}
	out, err := json.Marshal(facts)
	if err == nil {
		_, err = db.Exec(`UPDATE transitions SET facts = ? WHERE `+where, string(out), step, rev)
	}
	if err != nil {
		t.Fatal(err)
	}
	return req.Body
}

// rewriteResponse replaces old by new once in the decision of the response
// saved for attempt of step: in its raw body, where a text reply holds it
// JSON-escaped, and in the body its response_loaded event decoded, whose
// event ID it derives again. It returns the new decision body.
func rewriteResponse(t *testing.T, db *sql.DB, step, attempt int, old, new string) []byte {
	t.Helper()
	escaped := func(s string) string { b, _ := json.Marshal(s); return string(b[1 : len(b)-1]) }
	replace := func(b []byte) []byte {
		for _, r := range [][2]string{{old, new}, {escaped(old), escaped(new)}} {
			if bytes.Count(b, []byte(r[0])) == 1 {
				return bytes.Replace(b, []byte(r[0]), []byte(r[1]), 1)
			}
		}
		t.Fatalf("%s is not once in %s", old, b)
		return nil
	}
	var data string
	saved := `kind = 'response_saved' AND step = ? AND attempt = ?`
	if err := db.QueryRow(`SELECT facts FROM transitions WHERE `+saved, step, attempt).Scan(&data); err != nil {
		t.Fatal(err)
	}
	var facts harness.Facts
	if err := strict(data, &facts); err != nil || facts.Response == nil {
		t.Fatalf("response %d/%d: %v", step, attempt, err)
	}
	facts.Response.Body = replace(facts.Response.Body)
	var run, id string
	var seq int64
	loaded := `kind = 'response_loaded' AND step = ? AND json_extract(event, '$.Attempt') = ?`
	if err := db.QueryRow(`SELECT run_id, seq, event_id, event FROM transitions WHERE `+loaded, step, attempt).Scan(&run, &seq, &id, &data); err != nil {
		t.Fatal(err)
	}
	eventID := func(payload string) string {
		sum := sha256.Sum256(fmt.Appendf(nil, "%d:%s:%d:response_loaded:%s", len(run), run, seq, payload))
		return "event_" + hex.EncodeToString(sum[:])
	}
	var event core.ResponseLoaded
	if err := strict(data, &event); err != nil || eventID(data) != id {
		t.Fatalf("response_loaded %d/%d: %v, or its event ID is not derived from it", step, attempt, err)
	}
	event.Body = replace(event.Body)
	out, err := json.Marshal(facts)
	if err == nil {
		_, err = db.Exec(`UPDATE transitions SET facts = ? WHERE `+saved, string(out), step, attempt)
	}
	if err == nil {
		out, err = json.Marshal(event)
	}
	if err == nil {
		_, err = db.Exec(`UPDATE transitions SET event = ?, event_id = ? WHERE `+loaded, string(out), eventID(string(out)), step, attempt)
	}
	if err != nil {
		t.Fatal(err)
	}
	return event.Body
}

// relabel marks the run and every transition as written under semantics
// version and returns the last sequence.
func relabel(t *testing.T, db *sql.DB, version int) int {
	t.Helper()
	var last int
	_, err := db.Exec(`UPDATE runs SET semantics = ?; UPDATE transitions SET semantics = ?`, version, version)
	if err == nil {
		err = db.QueryRow(`SELECT max(seq) FROM transitions`).Scan(&last)
	}
	if err != nil {
		t.Fatal(err)
	}
	return last
}

// journalRows dumps the transitions up to seq.
func journalRows(t *testing.T, db *sql.DB, seq int) string {
	t.Helper()
	var data string
	if err := db.QueryRow(`SELECT json_group_array(json_array(seq, event_id, kind, semantics, event, from_state, to_state, facts))
		FROM (SELECT * FROM transitions WHERE seq <= ? ORDER BY seq)`, seq).Scan(&data); err != nil {
		t.Fatal(err)
	}
	return data
}

// checkCurrent fails unless every transition after seq, and at least one, is
// written under semantics version 3.
func checkCurrent(t *testing.T, db *sql.DB, seq int) int {
	t.Helper()
	var current, total int
	if err := db.QueryRow(`SELECT count(*) FILTER (WHERE semantics = 3), count(*) FROM transitions WHERE seq > ?`, seq).Scan(&current, &total); err != nil || current != total || total == 0 {
		t.Fatalf("new transitions: %d of %d at version 3, %v", current, total, err)
	}
	return total
}

// checkLegacy fails unless body holds the system prompt and decision schema
// of 7624f85 and no purpose.
func checkLegacy(t *testing.T, format string, body []byte) {
	t.Helper()
	if !strings.HasPrefix(parseFrozen(t, body).System, legacySystem+"\n\nTo read historical evidence") ||
		!bytes.Contains(body, []byte(legacySchemas[format])) || bytes.Contains(bytes.ToLower(body), []byte("purpose")) {
		t.Fatalf("%s request is not the one 7624f85 freezes:\n%s", format, body)
	}
}

func eventCount(h []entry, kind string, step int) int {
	n := 0
	for _, e := range h {
		if e.Kind == kind && e.State.Step == step {
			n++
		}
	}
	return n
}

func TestDecisionPurposeResumesLegacyJournals(t *testing.T) {
	for _, format := range []string{"tool", "text"} {
		for _, version := range []int{1, 2} {
			for _, crash := range []string{"attempt_started", "response_saved"} {
				t.Run(fmt.Sprintf("%s/v%d/%s", format, version, crash), func(t *testing.T) {
					f, image := recoveryFixture(t, nil)
					answerDecision(f, format, 1, 1, core.Decision{Kind: core.Exec, Payload: "printf 'one\\n' > one.txt"})
					answerDecision(f, format, 2, 1, core.Decision{Kind: core.Exec, Payload: "cat one.txt"})
					answerDecision(f, format, 3, 1, core.Decision{Kind: core.Exec, Purpose: "check the file, then return", Payload: "test -s one.txt"})
					answerDecision(f, format, 4, 1, core.Decision{Kind: core.Return, Purpose: "verified", Payload: "done"})
					if r := f.cli(nil, "run", "-once", "-goal", "resume an earlier journal", "-image", image, "-decision-format", format,
						"-fallbacks=false", "-wall-time", "2m", "-job-timeout", "10s"); r.code != exitActive {
						t.Fatalf("run -once = %+v", r)
					}
					db := openJournal(t, f)
					legacy := map[[2]int][]byte{{1, 1}: freezeLegacy(t, db, format, 1, 1)}
					frozenTwo := func(s status) bool { return s.Step == 2 && s.Phase == core.PhaseRequestFrozen }
					if r := resumeOnce(t, f, frozenTwo); r.code != exitActive || !frozenTwo(f.status()) {
						t.Fatalf("did not stop with the step 2 request frozen: %+v %+v", r, f.status())
					}
					legacy[[2]int{2, 1}] = freezeLegacy(t, db, format, 2, 1)
					crashRun(t, f, "after:"+crash, "resume", "-once")
					want := map[string]core.Phase{"attempt_started": core.PhaseRequestSent, "response_saved": core.PhaseResponseSaved}[crash]
					sent := map[string]int{"attempt_started": 1, "response_saved": 2}[crash]
					if s := f.status(); s.Step != 2 || s.Phase != want || s.Attempt != 1 || len(f.model.requests()) != sent {
						t.Fatalf("crash after %s left %+v after %d sends", crash, s, len(f.model.requests()))
					}
					seq := relabel(t, db, version)
					before := journalRows(t, db, seq)
					if r := f.cli(nil, "resume"); r.code != exitOK || strings.TrimSpace(r.stdout) != "done" {
						t.Fatalf("resume of a v%d journal = %+v", version, r)
					}
					if journalRows(t, db, seq) != before {
						t.Fatal("resume rewrote legacy records")
					}
					total := checkCurrent(t, db, seq)
					h := f.history()
					frozen, wire := frozenRequests(t, h), f.model.requests()
					if len(wire) != 4 || len(frozen) != 4 {
						t.Fatalf("%d sends, %d frozen requests; want 4 each", len(wire), len(frozen))
					}
					for i, key := range [][2]int{{1, 1}, {2, 1}} {
						checkLegacy(t, format, legacy[key])
						if !bytes.Equal(frozen[key], legacy[key]) || !bytes.Equal(wire[i], legacy[key]) {
							t.Fatalf("legacy request %v was not sent or kept byte for byte", key)
						}
					}
					if n := map[string]int{"attempt_started": 2, "response_saved": 1}[crash]; eventCount(h, "attempt_started", 2) != n {
						t.Fatalf("step 2 has %d attempts, want %d", eventCount(h, "attempt_started", 2), n)
					}
					for _, e := range h {
						if e.Kind == "response_loaded" && e.State.Step == 2 && e.State.Decision != (core.Decision{Kind: core.Exec, Payload: "cat one.txt"}) {
							t.Fatalf("the new binary decoded the legacy step 2 response as %+v", e.State.Decision)
						}
					}
					for step := range 3 {
						if eventCount(h, "job_started", step+1) != 1 {
							t.Fatalf("step %d started %d jobs", step+1, eventCount(h, "job_started", step+1))
						}
					}
					last := parseFrozen(t, frozen[[2]int{4, 1}])
					if len(last.Blocks) != 5 || !strings.HasPrefix(last.Blocks[1], "### Step 1\nCommand:\n") || !strings.HasPrefix(last.Blocks[2], "### Step 2\nCommand:\n") ||
						!strings.HasPrefix(last.Blocks[3], "### Step 3\nPurpose: check the file, then return\nCommand:\n") {
						t.Fatalf("step 4 request blocks = %q", last.Blocks)
					}
					checkTransport(t, format, frozen[[2]int{3, 1}])
					checkTransport(t, format, frozen[[2]int{4, 1}])
					checkFile(t, filepath.Join(f.workspace, "one.txt"), "one\n")
					checkReplay(t, h[0].State.RunID, h)
					checkReleased(t, f, h)
					t.Logf("LEGACY_RESUME format=%s semantics=%d crash=after:%s legacy_seq=%d new_transitions=%d; real CLI/SQLite/Docker, scripted Messages provider",
						format, version, crash, seq, total)
				})
			}
		}
	}
}

func TestDecisionPurposeOfALegacyRejectionNeedsNoRepair(t *testing.T) {
	for _, format := range []string{"tool", "text"} {
		t.Run(format, func(t *testing.T) {
			f, image := recoveryFixture(t, nil)
			rejected := core.Decision{Kind: core.Exec, Purpose: "append a line version 2 rejected", Payload: "printf 'rejected\\n' >> log.txt"}
			unsupported := rejected
			unsupported.Kind = "wait"
			two := core.Decision{Kind: core.Exec, Purpose: "append two, then return", Payload: "printf 'two\\n' >> log.txt"}
			answerDecision(f, format, 1, 1, core.Decision{Kind: core.Exec, Payload: "printf 'one\\n' >> log.txt"})
			answerDecision(f, format, 2, 1, unsupported)
			answerDecision(f, format, 2, 2, two)
			answerDecision(f, format, 3, 1, core.Decision{Kind: core.Return, Purpose: "both lines are written", Payload: "done"})
			if r := f.cli(nil, "run", "-once", "-goal", "repair an earlier rejection", "-image", image, "-decision-format", format,
				"-fallbacks=false", "-wall-time", "2m", "-job-timeout", "10s"); r.code != exitActive {
				t.Fatalf("run -once = %+v", r)
			}
			db := openJournal(t, f)
			legacy := map[[2]int][]byte{{1, 1}: freezeLegacy(t, db, format, 1, 1)}
			frozenTwo := func(s status) bool { return s.Step == 2 && s.Phase == core.PhaseRequestFrozen }
			if r := resumeOnce(t, f, frozenTwo); r.code != exitActive || !frozenTwo(f.status()) {
				t.Fatalf("did not stop with the step 2 request frozen: %+v %+v", r, f.status())
			}
			legacy[[2]int{2, 1}] = freezeLegacy(t, db, format, 2, 1)
			repair := func(s status) bool { return s.Step == 2 && s.Phase == core.PhaseIdle }
			if r := resumeOnce(t, f, repair); r.code != exitActive {
				t.Fatalf("resume -once to the repair point = %+v", r)
			}
			if s := f.status(); !repair(s) || s.Attempt != 2 || s.RequestRev != 1 || len(f.model.requests()) != 2 {
				t.Fatalf("not idle at the repair of step 2: %+v after %d sends", s, len(f.model.requests()))
			}
			body := rewriteResponse(t, db, 2, 1, `"kind":"wait"`, `"kind":"exec"`)
			var fields map[string]json.RawMessage
			if d, err := core.Decode(body); err != nil || d != rejected || json.Unmarshal(body, &fields) != nil || len(fields) != 3 {
				t.Fatalf("saved reply %s is not a three-field decision the current decoder accepts: %+v, %v", body, d, err)
			}
			seq := relabel(t, db, 2)
			before := journalRows(t, db, seq)
			if r := f.cli(nil, "resume"); r.code != exitOK || strings.TrimSpace(r.stdout) != "done" {
				t.Fatalf("resume at the repair of a v2 rejection = %+v", r)
			}
			if journalRows(t, db, seq) != before {
				t.Fatal("resume rewrote legacy records")
			}
			total := checkCurrent(t, db, seq)
			h := f.history()
			frozen, wire := frozenRequests(t, h), f.model.requests()
			keys := [][2]int{{1, 1}, {2, 1}, {2, 2}, {3, 1}}
			if len(wire) != len(keys) || len(frozen) != len(keys) {
				t.Fatalf("%d sends, %d frozen requests; want %d each", len(wire), len(frozen), len(keys))
			}
			for i, key := range keys {
				if !bytes.Equal(frozen[key], wire[i]) || i < 2 && !bytes.Equal(wire[i], legacy[key]) {
					t.Fatalf("request %v sent other bytes than the journal froze", key)
				}
			}
			rev1, rev2 := parseFrozen(t, frozen[[2]int{2, 1}]), parseFrozen(t, frozen[[2]int{2, 2}])
			if len(rev2.Blocks) != 3 || rev2.Blocks[0] != rev1.Blocks[0] || rev2.Blocks[1] != rev1.Blocks[1] ||
				rev2.Blocks[2] != strings.Replace(rev1.Blocks[2], ", attempt 1 of ", ", attempt 2 of ", 1) {
				t.Fatalf("repair revision blocks =\n%q\nwant the revision 1 blocks at attempt 2\n%q", rev2.Blocks, rev1.Blocks)
			}
			checkTransport(t, format, frozen[[2]int{2, 2}])
			for _, key := range [][2]int{{2, 2}, {3, 1}} {
				if body := frozen[key]; bytes.Contains(body, []byte("Rejected response")) || bytes.Contains(body, []byte(rejected.Purpose)) {
					t.Fatalf("request %v shows the rejected reply:\n%s", key, body)
				}
			}
			stepTwo := "### Step 2\nPurpose: " + two.Purpose + "\nCommand:\n```\n" + two.Payload + "\n```\n"
			if last := parseFrozen(t, frozen[[2]int{3, 1}]); len(last.Blocks) != 4 || !strings.HasPrefix(last.Blocks[2], stepTwo) {
				t.Fatalf("step 3 request blocks = %q, want step 2 starting\n%q", last.Blocks, stepTwo)
			}
			for step := range 2 {
				if eventCount(h, "job_started", step+1) != 1 {
					t.Fatalf("step %d started %d jobs", step+1, eventCount(h, "job_started", step+1))
				}
			}
			checkFile(t, filepath.Join(f.workspace, "log.txt"), "one\ntwo\n")
			checkReleased(t, f, h)
			t.Logf("LEGACY_REPAIR format=%s legacy_seq=%d new_transitions=%d requests=%d; real CLI/SQLite/Docker, scripted Messages provider",
				format, seq, total, len(wire))
		})
	}
}
