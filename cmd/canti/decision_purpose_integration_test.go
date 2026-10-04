//go:build integration

package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

const (
	toolInstructions = "Call the canti_decision tool exactly once per reply; never reply with text."
	textInstructions = "Reply with exactly one JSON object and nothing else"
	textEscaping     = "The purpose and payload are JSON strings, so escape quotes, backslashes, and newlines."
	purposeRequired  = `"required":["kind","payload","purpose"]`
)

type frozenBody struct {
	System string
	Blocks []string
}

// frozenRequests returns the frozen request bodies of h by step and revision.
func frozenRequests(t *testing.T, h []entry) map[[2]int][]byte {
	t.Helper()
	out := map[[2]int][]byte{}
	for _, e := range h {
		if req := e.Facts.Request; req != nil {
			out[[2]int{e.State.Step, e.State.RequestRev}] = req.Body
		}
	}
	return out
}

func parseFrozen(t *testing.T, body []byte) frozenBody {
	t.Helper()
	var req struct {
		System   string
		Messages []struct{ Content []struct{ Text string } }
	}
	if err := json.Unmarshal(body, &req); err != nil || len(req.Messages) != 1 {
		t.Fatalf("frozen body is not one user message: %v", err)
	}
	f := frozenBody{System: req.System}
	for _, b := range req.Messages[0].Content {
		f.Blocks = append(f.Blocks, b.Text)
	}
	return f
}

func checkTransport(t *testing.T, format string, body []byte) {
	t.Helper()
	system := parseFrozen(t, body).System
	tool := format == "tool"
	if strings.Contains(system, toolInstructions) != tool || strings.Contains(system, textInstructions) == tool ||
		strings.Contains(system, textEscaping) == tool || !bytes.Contains(body, []byte(purposeRequired)) {
		t.Fatalf("%s request froze a mismatched system or schema:\n%s", format, body)
	}
}

// answerDecision makes the n-th request of step answer d in format.
func answerDecision(f *fixture, format string, step, n int, d core.Decision) {
	if format == "tool" {
		f.model.answer(step, n, reply{status: http.StatusOK, body: toolReply(d)})
		return
	}
	text, _ := json.Marshal(d)
	f.model.answer(step, n, reply{text: string(text)})
}

// resumeOnce runs resume -once, each in a new process, until the run leaves
// the active state, and returns that last result.
func resumeOnce(t *testing.T, f *fixture, until func(status) bool) result {
	t.Helper()
	for range 40 {
		r := f.cli(nil, "resume", "-once")
		if r.code != exitActive || until != nil && until(f.status()) {
			return r
		}
	}
	t.Fatal("run did not settle within 40 single steps")
	return result{}
}

func TestDecisionPurposeReachesLaterRequestsAcrossRestarts(t *testing.T) {
	for _, format := range []string{"tool", "text"} {
		t.Run(format, func(t *testing.T) {
			f, image := recoveryFixture(t, nil)
			first := "write one.txt, then read it back"
			long := "read the file back\n" + strings.Repeat("é", 300)
			answerDecision(f, format, 1, 1, core.Decision{Kind: core.Exec, Purpose: first, Payload: "printf 'one\\n' > one.txt"})
			if format == "tool" {
				f.model.answer(2, 1, reply{status: http.StatusOK, body: strings.Replace(toolReply(core.Decision{Kind: core.Exec, Payload: "touch forbidden"}), "canti_decision", "other_tool", 1)})
			} else {
				f.model.answer(2, 1, reply{text: "not a decision"})
			}
			answerDecision(f, format, 2, 2, core.Decision{Kind: core.Exec, Purpose: long, Payload: "cat one.txt"})
			answerDecision(f, format, 3, 1, core.Decision{Kind: core.Return, Purpose: "the file was written and read", Payload: "done"})
			if r := f.cli(nil, "run", "-once", "-goal", "write and read one file", "-image", image, "-decision-format", format,
				"-fallbacks=false", "-max-attempts", "2", "-wall-time", "2m", "-job-timeout", "10s"); r.code != exitActive {
				t.Fatalf("run -once = %+v", r)
			}
			if r := resumeOnce(t, f, nil); r.code != exitOK || strings.TrimSpace(r.stdout) != "done" {
				t.Fatalf("final resume -once = %+v", r)
			}
			h := f.history()
			frozen := frozenRequests(t, h)
			wire := f.model.requests()
			if len(frozen) != 4 || len(wire) != 4 {
				t.Fatalf("frozen %d, sent %d requests; want 4 each", len(frozen), len(wire))
			}
			for i, key := range [][2]int{{1, 1}, {2, 1}, {2, 2}, {3, 1}} {
				if !bytes.Equal(frozen[key], wire[i]) {
					t.Fatalf("request %v sent other bytes than the journal froze", key)
				}
				checkTransport(t, format, frozen[key])
			}
			rev1, rev2, last := parseFrozen(t, frozen[[2]int{2, 1}]), parseFrozen(t, frozen[[2]int{2, 2}]), parseFrozen(t, frozen[[2]int{3, 1}])
			stepOne := "### Step 1\nPurpose: " + first + "\nCommand:\n```\nprintf 'one\\n' > one.txt\n```\n"
			if len(rev1.Blocks) != 3 || !strings.HasPrefix(rev1.Blocks[1], stepOne) {
				t.Fatalf("step 2 request blocks =\n%q\nwant step 1 starting\n%q", rev1.Blocks, stepOne)
			}
			if rev2.System != rev1.System || len(rev2.Blocks) != 3 || rev2.Blocks[0] != rev1.Blocks[0] || rev2.Blocks[1] != rev1.Blocks[1] ||
				!strings.Contains(rev2.Blocks[2], "## Rejected response") {
				t.Fatalf("revision 2 of step 2, rendered by a new process, changed the stable prefix:\n%q\n%q", rev2.Blocks, rev1.Blocks)
			}
			stepTwo := "### Step 2\nPurpose: read the file back " + strings.Repeat("é", 246) + "…[truncated 108 bytes]\nCommand:\n```\ncat one.txt\n```\n"
			if last.System != rev1.System || len(last.Blocks) != 4 || last.Blocks[1] != rev1.Blocks[1] || !strings.HasPrefix(last.Blocks[2], stepTwo) {
				t.Fatalf("step 3 request blocks =\n%q\nwant step 1 unchanged and step 2 starting\n%q", last.Blocks, stepTwo)
			}
			purposes := map[int]string{}
			for _, e := range h {
				if e.Kind == "response_loaded" && e.State.Decision.Kind != "" {
					purposes[e.State.Step] = e.State.Decision.Purpose
				}
			}
			if purposes[1] != first || purposes[2] != long || purposes[3] != "the file was written and read" {
				t.Fatalf("committed purposes = %q", purposes)
			}
			checkFile(t, filepath.Join(f.workspace, "one.txt"), "one\n")
			checkReplay(t, h[0].State.RunID, h)
			checkReleased(t, f, h)
			t.Logf("PURPOSE_CHAIN format=%s requests=4 transitions=%d; real CLI/SQLite/Docker, one process per step, scripted Messages provider", format, len(h))
		})
	}
}

func TestDecisionPurposeResumesLegacyJournals(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			f, image := recoveryFixture(t, nil)
			answerDecision(f, "tool", 1, 1, core.Decision{Kind: core.Exec, Payload: "printf 'one\\n' > one.txt"})
			answerDecision(f, "tool", 2, 1, core.Decision{Kind: core.Exec, Payload: "cat one.txt"})
			answerDecision(f, "tool", 3, 1, core.Decision{Kind: core.Exec, Purpose: "check the file, then return", Payload: "test -s one.txt"})
			answerDecision(f, "tool", 4, 1, core.Decision{Kind: core.Return, Purpose: "verified", Payload: "done"})
			if r := f.cli(nil, "run", "-once", "-goal", "resume an earlier journal", "-image", image, "-decision-format", "tool",
				"-fallbacks=false", "-wall-time", "2m", "-job-timeout", "10s"); r.code != exitActive {
				t.Fatalf("run -once = %+v", r)
			}
			frozenTwo := func(s status) bool { return s.Step == 2 && s.Phase == core.PhaseRequestFrozen }
			if r := resumeOnce(t, f, frozenTwo); r.code != exitActive || !frozenTwo(f.status()) {
				t.Fatalf("did not stop with the step 2 request frozen: %+v %+v", r, f.status())
			}
			f.cli([]string{"CANTI_TEST_CRASH=after:attempt_started"}, "resume", "-once")
			if s := f.status(); s.Step != 2 || s.Phase != core.PhaseRequestSent || s.Attempt != 1 || len(f.model.requests()) != 1 {
				t.Fatalf("crash after the attempt commit left %+v after %d sends", s, len(f.model.requests()))
			}
			db, err := sql.Open("sqlite", filepath.Join(f.state, "canti.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			rows := func(where string) string {
				var data string
				if err := db.QueryRow(`SELECT json_group_array(json_array(seq, event_id, kind, semantics, event, from_state, to_state, facts))
					FROM (SELECT * FROM transitions WHERE ` + where + ` ORDER BY seq)`).Scan(&data); err != nil {
					t.Fatal(err)
				}
				return data
			}
			var legacy int
			if _, err := db.Exec(`UPDATE runs SET semantics = ?; UPDATE transitions SET semantics = ?`, version, version); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT max(seq) FROM transitions`).Scan(&legacy); err != nil {
				t.Fatal(err)
			}
			before := rows(fmt.Sprintf("seq <= %d", legacy))
			if r := f.cli(nil, "resume"); r.code != exitOK || strings.TrimSpace(r.stdout) != "done" {
				t.Fatalf("resume of a v%d journal = %+v", version, r)
			}
			if after := rows(fmt.Sprintf("seq <= %d", legacy)); after != before {
				t.Fatal("resume rewrote legacy records")
			}
			var current, total int
			if err := db.QueryRow(`SELECT count(*) FILTER (WHERE semantics = 3), count(*) FROM transitions WHERE seq > ?`, legacy).Scan(&current, &total); err != nil || current != total || total == 0 {
				t.Fatalf("new transitions: %d of %d at version 3, %v", current, total, err)
			}
			h := f.history()
			frozen, wire := frozenRequests(t, h), f.model.requests()
			attempts := 0
			for _, e := range h {
				if e.Kind == "attempt_started" && e.State.Step == 2 {
					attempts++
				}
			}
			if len(wire) != 4 || len(frozen) != 4 || !bytes.Equal(wire[1], frozen[[2]int{2, 1}]) || attempts != 2 {
				t.Fatalf("the in-flight legacy request was not resent verbatim after its lost attempt: %d sends, %d frozen, %d step 2 attempts", len(wire), len(frozen), attempts)
			}
			last := parseFrozen(t, frozen[[2]int{4, 1}])
			if len(last.Blocks) != 5 || !strings.HasPrefix(last.Blocks[1], "### Step 1\nCommand:\n") || !strings.HasPrefix(last.Blocks[2], "### Step 2\nCommand:\n") ||
				!strings.HasPrefix(last.Blocks[3], "### Step 3\nPurpose: check the file, then return\nCommand:\n") {
				t.Fatalf("step 4 request blocks = %q", last.Blocks)
			}
			checkTransport(t, "tool", frozen[[2]int{4, 1}])
			checkReplay(t, h[0].State.RunID, h)
			checkReleased(t, f, h)
			t.Logf("LEGACY_RESUME semantics=%d legacy_seq=%d new_transitions=%d; real CLI/SQLite/Docker, scripted Messages provider", version, legacy, total)
		})
	}
}
