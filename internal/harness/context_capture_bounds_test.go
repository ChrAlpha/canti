package harness_test

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestContextCaptureBoundsRejectedRetrievalAcrossReopen(t *testing.T) {
	for _, field := range []string{"reason", "operation", "path", "query", "cursor"} {
		for _, size := range []int{16 << 10, 1 << 20} {
			t.Run(fmt.Sprintf("%s/%d", field, size), func(t *testing.T) {
				q := archive.RetrievalQuery{Operation: "search", Path: "/canti/imports/evidence", Reason: "verify evidence", Query: "ORIGINAL"}
				large := strings.Repeat("x", size)
				switch field {
				case "reason":
					q.Reason = large
				case "operation":
					q.Operation = large
				case "path":
					q.Path = "/canti/imports/" + large
				case "query":
					q.Query = large
				case "cursor":
					q.Cursor = large
				}
				encoded, err := json.Marshal(q)
				if err != nil {
					t.Fatal(err)
				}
				f := newFixture(t, limits, harness.Budget{}, execReply("canti:retrieve "+string(encoded)),
					reply{body: "invalid decision"},
					execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"recover with valid input","limit":8}`),
					execReply(harness.HandoverPrefix+"Original evidence verified; retrieval recovered."),
					returnReply("Original evidence verified; retrieval recovered."))
				imported := t.TempDir()
				if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte("ORIGINAL_EVIDENCE"), 0600); err != nil {
					t.Fatal(err)
				}
				f.cfg.Reader = &archive.Reader{Imports: imported}
				f.cfg.WindowSteps = 2
				ctx := f.bounded()
				for i := 0; ; i++ {
					if i == 50 {
						t.Fatal("retrieval did not recover within 50 transitions")
					}
					s, err := f.fresh().Step(ctx, runID)
					if err != nil {
						t.Fatal(err)
					}
					if s.State.Intent() == core.IntentHalt {
						wantState(t, s, core.PhaseReturned, "")
						if s.State.Step != 4 || s.State.Decision.Payload != "Original evidence verified; retrieval recovered." {
							t.Fatalf("next window did not complete: %+v", s.State)
						}
						break
					}
				}
				steps, err := f.open().Recent(ctx, runID, 0, 3)
				if err != nil || len(steps) != 3 {
					t.Fatalf("observations = %d, %v", len(steps), err)
				}
				failed := steps[0].Observation
				if failed.Retrieval == nil || failed.Retrieval.Query != q || failed.Retrieval.Error == "" || failed.ExitCode == nil || *failed.ExitCode != 1 || steps[0].Command != "canti:retrieve "+string(encoded) {
					t.Fatal("failed original observation lost its full query or error")
				}
				if len(f.exec.log()) != 0 {
					t.Fatal("builtin retrieval called the executor")
				}
				if len(f.model.prompts) != 5 {
					t.Fatalf("prepares = %d, want 5", len(f.model.prompts))
				}
				for i, id := range [][2]int{{1, 1}, {2, 1}, {2, 2}, {3, 1}, {4, 1}} {
					req := capturedRequest(t, f, id[0], id[1])
					checkCapturedBytes(t, req, f.model.prompts[i])
					t.Logf("request %d/%d body_sha256=%x", id[0], id[1], sha256.Sum256(req.Body))
					capture, err := json.Marshal(req.Context)
					if err != nil {
						t.Fatal(err)
					}
					if len(capture) > 4096 {
						t.Errorf("capture %d/%d = %d bytes for rejected %s of %d bytes; want <= 4096", id[0], id[1], len(capture), field, size)
					}
					stored, err := json.Marshal(harness.Facts{Request: &req})
					t.Logf("request %d/%d capture_bytes=%d facts_bytes=%d body_bytes=%d", id[0], id[1], len(capture), len(stored), len(req.Body))
					if err != nil || len(stored) > 2*len(req.Body)+4096 {
						t.Errorf("frozen request %d/%d = %d bytes, body %d, error %v", id[0], id[1], len(stored), len(req.Body), err)
					}
					for _, retrieval := range req.Context.Retrievals {
						body, err := json.Marshal(retrieval)
						if err != nil || len(body) > 1024 || strings.Contains(string(body), `"Query":`) {
							t.Errorf("capture at source step %d copied the original query: %d bytes, %v", retrieval.Step, len(body), err)
						}
						if retrieval.Step == 1 && (retrieval.Error != failed.Retrieval.Error || retrieval.More || retrieval.Incomplete) {
							t.Fatal("failed retrieval metadata changed")
						}
						operation := "read"
						if retrieval.Step == 1 {
							operation = "search"
							if field == "operation" {
								operation = "invalid"
							}
						}
						if retrieval.Operation != operation {
							t.Fatalf("operation = %q, want %q", retrieval.Operation, operation)
						}
					}
				}
				first, repair := capturedRequest(t, f, 2, 1), capturedRequest(t, f, 2, 2)
				if !reflect.DeepEqual(first.Context.Retrievals, repair.Context.Retrievals) || repair.Context.Bytes.Repair == 0 || repair.Context.Handover.Status != "request_revision" {
					t.Fatal("protocol repair lost the original capture identity")
				}
				trigger := capturedRequest(t, f, 3, 1)
				if !trigger.Window.Requested || len(trigger.Context.Retrievals) != 2 || !trigger.Context.Retrievals[1].More || len(trigger.Context.Sources) != 0 || !strings.Contains(string(trigger.Body), "ORIGINAL") {
					t.Fatal("valid retrieval did not progress into the handover request")
				}
				next := capturedRequest(t, f, 4, 1)
				if next.Window.FirstStep != 4 || next.Window.SourceStep != 3 || len(next.Context.Retrievals) != 0 || !strings.Contains(string(next.Body), "Original evidence verified; retrieval recovered.") {
					t.Fatal("handover did not release the rejected query and preserve next-step progress")
				}
			})
		}
	}
}

func TestContextCaptureLegacyQuerySurvivesStrictSQLiteDecode(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, returnReply("done"))
	f.steps(f.loop(), 1)
	db, err := sql.Open("sqlite", f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	legacy := `[{"Step":1,"Query":{"operation":"search","path":"/canti/imports/evidence","reason":"legacy full query","query":"ORIGINAL"},"More":true,"Incomplete":true,"Error":"legacy error"}]`
	if _, err := db.ExecContext(f.bounded(), `UPDATE transitions SET facts = json_set(facts, '$.Request.Context.Retrievals', json(?)) WHERE kind = 'request_frozen'`, legacy); err != nil {
		t.Fatal(err)
	}
	r := capturedRequest(t, f, 1, 1)
	if len(r.Context.Retrievals) != 1 || r.Context.Retrievals[0].Query == nil || r.Context.Retrievals[0].Query.Reason != "legacy full query" || !r.Context.Retrievals[0].More || !r.Context.Retrievals[0].Incomplete || r.Context.Retrievals[0].Error != "legacy error" {
		t.Fatalf("legacy context query changed: %+v", r.Context.Retrievals)
	}
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
}
