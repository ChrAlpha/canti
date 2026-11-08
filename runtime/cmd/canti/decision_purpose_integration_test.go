//go:build integration

package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
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

// frozenBodies returns the frozen request bodies of h by step and revision.
func frozenBodies(t *testing.T, h []entry) map[[2]int][]byte {
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
			frozen := frozenBodies(t, h)
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
