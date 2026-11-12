package harness_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func capturedRequest(t *testing.T, f *fixture, step, rev int) harness.Request {
	t.Helper()
	r, err := f.open().Request(f.bounded(), runID, step, rev)
	if err != nil || r.Context == nil {
		t.Fatalf("request %d/%d context = %+v, %v", step, rev, r.Context, err)
	}
	return r
}

func checkCapturedBytes(t *testing.T, req harness.Request, p harness.Prompt) {
	t.Helper()
	c := req.Context.Bytes
	total := c.System + c.Goal + c.Constraints + c.HeadInstructions + c.Environment + c.Anchor +
		c.Previous + c.Handover + c.Index + c.Notice + c.EpochHeader + c.EpochInstructions +
		c.Observations + c.Progress + c.Latest + c.Repair
	for i, step := range c.Steps {
		total += step.Bytes
		if step.Bytes != len(p.Blocks[i+1]) {
			t.Fatalf("step %d bytes = %d, block = %d", step.Step, step.Bytes, len(p.Blocks[i+1]))
		}
	}
	if c.Total != len(p.System)+len(p.User()) || total != c.Total || c.Total > c.Limit || c.System != len(p.System) {
		t.Fatalf("capture = %+v, total parts=%d, actual=%d", c, total, len(p.System)+len(p.User()))
	}
	head := c.Goal + c.Constraints + c.HeadInstructions + c.Environment + c.Anchor + c.Previous + c.Handover + c.Index + c.Notice + c.EpochHeader
	if head != len(p.Blocks[0]) || c.Observations+c.Progress+c.Latest+c.Repair != len(p.Blocks[len(p.Blocks)-1]) {
		t.Fatalf("head or tail does not match blocks: %+v", c)
	}
	if c.Instructions != c.HeadInstructions+c.EpochInstructions {
		t.Fatalf("instruction accounting = %+v", c)
	}
	if !bytes.Equal(req.Body, []byte(p.System+"\n\n"+p.User())) {
		t.Fatal("metadata changed the model's frozen body")
	}
}

func TestContextCapturePersistsFrozenEvidenceAndRetry(t *testing.T) {
	imported := t.TempDir()
	file := filepath.Join(imported, "evidence")
	original := []byte("ORIGINAL_CAPTURE_EVIDENCE")
	if err := os.WriteFile(file, original, 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, limits, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"verify original","limit":8}`),
		reply{err: errors.New("transport interrupted"), hook: func() {
			if err := os.Remove(file); err != nil {
				t.Fatal(err)
			}
		}},
		reply{body: `{"wrong":"protocol"}`}, returnReply("done"))
	f.cfg.Reader = &archive.Reader{Imports: imported}
	ctx := f.bounded()
	for i := 0; i < 40; i++ {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Intent() == core.IntentHalt {
			wantState(t, s, core.PhaseReturned, "")
			break
		}
		if i == 39 {
			t.Fatal("run did not complete")
		}
	}
	first := capturedRequest(t, f, 1, 1)
	originalReq := capturedRequest(t, f, 2, 1)
	repaired := capturedRequest(t, f, 2, 2)
	for i, req := range []harness.Request{first, originalReq, repaired} {
		checkCapturedBytes(t, req, f.model.prompts[i])
	}
	c := originalReq.Context
	if len(c.Sources) != 1 {
		t.Fatalf("frozen evidence needs no source check, captured %+v", c.Sources)
	}
	steps, err := f.store.Recent(ctx, runID, 0, 1)
	if err != nil || len(steps) != 1 || steps[0].Outcome == nil || len(steps[0].Outcome.Retrieval.Evidence) != 1 || steps[0].Outcome.Retrieval.Evidence[0] != "ORIGINAL" {
		t.Fatalf("committed evidence = %+v, %v", steps, err)
	}
	if len(c.Retrievals) != 1 || !c.Retrievals[0].More || c.Retrievals[0].Operation != "read" || c.Retrievals[0].Step != 1 || steps[0].Outcome.Retrieval.Query.Reason != "verify original" {
		t.Fatalf("retrieval = %+v", c.Retrievals)
	}
	if len(f.model.sent) != 4 || !bytes.Equal(f.model.sent[1], f.model.sent[2]) || !strings.Contains(string(originalReq.Body), "ORIGINAL") {
		t.Fatal("transport retry did not preserve original bytes")
	}
	r := repaired.Context
	if len(r.Sources) != 1 || !strings.Contains(string(repaired.Body), "ORIGINAL") || strings.Contains(string(repaired.Body), "Evidence unavailable") {
		t.Fatalf("the repair after the source was removed lost the frozen evidence: %+v", r.Sources)
	}
	if c.Preparation != "first_request" || r.Preparation != "protocol_repair" || r.Bytes.Repair == 0 {
		t.Fatalf("wrong preparation capture: %+v %+v", c, r)
	}

	if !reflect.DeepEqual(originalReq.Context, capturedRequest(t, f, 2, 1).Context) {
		t.Fatal("later repair rewrote the original capture")
	}
}
