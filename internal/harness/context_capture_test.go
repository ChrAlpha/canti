package harness_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
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
	total := c.System + c.Goal + c.Constraints + c.Environment + c.Session + c.Window + c.Handover + c.StepHeader + c.EmptySteps + c.Progress + c.HandoverRequired + c.Repair
	for i, step := range c.Steps {
		total += step.Bytes
		if step.Bytes != len(p.Blocks[i+1]) {
			t.Fatalf("step %d bytes = %d, block = %d", step.Step, step.Bytes, len(p.Blocks[i+1]))
		}
	}
	if c.Total != len(p.System)+len(p.User()) || total != c.Total || c.Total > c.Limit || c.System != len(p.System) {
		t.Fatalf("capture = %+v, total parts=%d, actual=%d", c, total, len(p.System)+len(p.User()))
	}
	if c.Goal+c.Constraints+c.Environment+c.Session+c.Window+c.Handover+c.StepHeader != len(p.Blocks[0]) || c.EmptySteps+c.Progress+c.HandoverRequired+c.Repair != len(p.Blocks[len(p.Blocks)-1]) {
		t.Fatalf("head or tail does not match blocks: %+v", c)
	}
	if !bytes.Equal(req.Body, []byte(p.System+"\n\n"+p.User())) {
		t.Fatal("metadata changed the model's frozen body")
	}
}

func TestContextCapturePersistsActualExpansionAndRetry(t *testing.T) {
	archive := t.TempDir()
	file := filepath.Join(archive, "evidence")
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
	f.cfg.Reader = &harness.Reader{Imports: archive}
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
	if len(c.Sources) != 1 || !c.Sources[0].Available || c.Sources[0].Step != 1 || c.Sources[0].Source.Path != "/canti/imports/evidence" || c.Sources[0].Source.Length != 8 {
		t.Fatalf("original source = %+v", c.Sources)
	}
	steps, err := f.store.Recent(ctx, runID, 0, 1)
	if err != nil || c.Sources[0].Source != steps[0].Observation.Retrieval.Sources[0] {
		t.Fatalf("source lost provenance: %v", err)
	}
	if len(c.Retrievals) != 1 || !c.Retrievals[0].More || c.Retrievals[0].Query.Reason != "verify original" || c.Retrievals[0].Step != 1 {
		t.Fatalf("retrieval = %+v", c.Retrievals)
	}
	if len(f.model.sent) != 4 || !bytes.Equal(f.model.sent[1], f.model.sent[2]) || !strings.Contains(string(originalReq.Body), "ORIGINAL") {
		t.Fatal("transport retry did not preserve original bytes")
	}
	r := repaired.Context
	if len(r.Sources) != 1 || r.Sources[0].Available || r.Sources[0].Error == "" || r.Sources[0].Source != c.Sources[0].Source || !strings.Contains(string(repaired.Body), "Evidence unavailable:") {
		t.Fatalf("repair source = %+v", r.Sources)
	}
	if c.Handover.Status != "evaluated" || c.Handover.Capacity <= 0 || c.Handover.Growth <= 0 || r.Handover.Status != "request_revision" || r.Handover.Capacity != 0 || len(r.Handover.Reasons) != 0 || r.Bytes.Repair == 0 {
		t.Fatalf("checks original=%+v repair=%+v", c.Handover, r.Handover)
	}
	if !reflect.DeepEqual(originalReq.Context, capturedRequest(t, f, 2, 1).Context) {
		t.Fatal("later repair rewrote the original capture")
	}
}

func TestContextCaptureRecordsHandoverReasonsAndSkippedChecks(t *testing.T) {
	for _, mode := range []string{"steps", "bytes", "both"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{}, execReply("true"),
				execReply("must be rejected while handover pending"),
				execReply(`canti:handover {"run_id":"run-1","first_step":1,"text":"Continue from original history."}`), returnReply("done"))
			var reasons []string
			if mode != "bytes" {
				f.cfg.WindowSteps = 1
				reasons = append(reasons, "window_steps")
			}
			if mode != "steps" {
				f.cfg.WindowBytes = 1
				reasons = append(reasons, "window_bytes")
			}
			wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
			trigger := capturedRequest(t, f, 2, 1)
			pending := capturedRequest(t, f, 3, 1)
			next := capturedRequest(t, f, 4, 1)
			if !slices.Equal(trigger.Context.Handover.Reasons, reasons) || !trigger.Window.Requested || trigger.Context.Handover.Steps != 1 || trigger.Context.Handover.Used != trigger.Context.Bytes.Steps[0].Bytes {
				t.Fatalf("trigger = %+v", trigger.Context)
			}
			if pending.Context.Handover.Status != "already_requested" || len(pending.Context.Handover.Reasons) != 0 || pending.Context.Handover.Capacity != 0 {
				t.Fatalf("pending = %+v", pending.Context.Handover)
			}
			if next.Window.FirstStep != 4 || next.Window.SourceStep != 3 || next.Context.Bytes.Handover == 0 || next.Context.Handover.Status != "evaluated" || next.Context.Handover.Used != 0 || len(next.Context.Sources) != 0 {
				t.Fatalf("next window = %+v", next)
			}
			for i, req := range []harness.Request{capturedRequest(t, f, 1, 1), trigger, pending, next} {
				checkCapturedBytes(t, req, f.model.prompts[i])
			}
		})
	}
}

func TestContextCaptureRecordsCapacityWithUnavailableSource(t *testing.T) {
	archive := t.TempDir()
	file := filepath.Join(archive, "evidence")
	if err := os.WriteFile(file, []byte(strings.Repeat("x", harness.RetrievalMaxBytes)), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, limits, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"first","limit":8192}`),
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"second","limit":8192}`),
		execReply(`canti:handover {"run_id":"run-1","first_step":1,"text":"Verify original references."}`), returnReply("done"))
	f.cfg.Reader = &harness.Reader{Imports: archive}
	f.cfg.Limits = harness.DefaultPromptLimits()
	f.cfg.Limits.Total = 96 << 10
	ctx := f.bounded()
	for i := 0; i < 30; i++ {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Step == 3 {
			break
		}
		if i == 29 {
			t.Fatal("retrieval did not complete")
		}
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
	r := capturedRequest(t, f, 3, 1)
	c := r.Context.Handover
	if c.Status != "evaluated" || !slices.Equal(c.Reasons, []string{"capacity"}) || c.Used+c.Growth < c.Capacity || !r.Window.Requested {
		t.Fatalf("capacity decision = %+v", c)
	}
	if len(r.Context.Sources) != 2 || r.Context.Sources[0].Available || r.Context.Sources[1].Available {
		t.Fatalf("sources = %+v", r.Context.Sources)
	}
	checkCapturedBytes(t, r, f.model.prompts[2])
}

func TestContextCapturePreservesProviderWireBaseline(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("printf '你好\\n```\\n'"),
		reply{body: `{"wrong":"protocol"}`},
		execReply(`canti:handover {"run_id":"run-1","first_step":1,"text":"Continue at original evidence."}`), returnReply("done"))
	f.cfg.WindowSteps = 1
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
	m, err := anthropic.New(anthropic.Config{APIKey: "fixture", Model: "fixture-model"})
	if err != nil {
		t.Fatal(err)
	}
	// Captured from the provider adapter at 956232c before context diagnostics.
	want := []string{
		"bbf78772f118b62458939adddedb96f617877d53438270c5ef07e43adc33a603",
		"acc3112e7671654abc1710fc681707f6fc7893003c32af8a429b714e19d899b2",
		"0e155a5e7735d0115b42c68d4e3edd723b21ab77dbecd6050a58f687b32a101c",
		"3eb3c8734c11c1c19058b1434dadc77231c7b7bd1a5a0d6fca54ede5c50a3aa9",
	}
	if len(f.model.prompts) != len(want) {
		t.Fatalf("prompts = %d", len(f.model.prompts))
	}
	for i, p := range f.model.prompts {
		r, err := m.Freeze(p)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(r.Body)); got != want[i] {
			t.Fatalf("provider body %d differs from baseline: %s", i, got)
		}
	}
}

func TestContextCaptureKeepsSelectedSessionIdentities(t *testing.T) {
	for _, mode := range []string{"dialogue", "status-only", "navigation-only"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{}, returnReply("standalone"), returnReply("earlier answer"), returnReply("current answer"))
			wantState(t, f.run(f.loop()), core.PhaseReturned, "")
			ctx := f.bounded()
			submit := func(id, goal string) harness.Run {
				t.Helper()
				state, err := core.NewState(id, limits)
				if err != nil {
					t.Fatal(err)
				}
				r, err := f.store.Submit(ctx, harness.Run{ID: id, SessionID: "session-capture", InputID: id, Goal: goal, Created: created}, state)
				if err != nil {
					t.Fatal(err)
				}
				return r
			}
			prior := submit("session-prior", "earlier goal")
			if mode == "dialogue" {
				s, err := f.fresh().Run(ctx, prior.ID)
				if err != nil {
					t.Fatal(err)
				}
				wantState(t, s, core.PhaseReturned, "")
			} else {
				f.model.replies = []reply{returnReply("current answer")}
				s, err := f.store.Latest(ctx, prior.ID)
				if err != nil {
					t.Fatal(err)
				}
				r, err := harness.NewRunner(f.store, f.exec)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := r.Commit(ctx, s, core.StopRequested{Reason: core.StopCancelled}, harness.Facts{}); err != nil {
					t.Fatal(err)
				}
				large := submit("session-large", strings.Repeat("G", 200000))
				s, err = f.store.Latest(ctx, large.ID)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "status-only" {
					if _, err := r.Commit(ctx, s, core.StopRequested{Reason: core.StopCancelled}, harness.Facts{}); err != nil {
						t.Fatal(err)
					}
				} else {
					model := &fakeModel{replies: []reply{returnReply("large return")}}
					cfg := f.cfg
					cfg.Limits = harness.DefaultPromptLimits()
					cfg.Limits.Goal, cfg.Limits.Total = 256<<10, 512<<10
					loop, err := harness.NewLoop(f.store, f.exec, model, cfg)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := loop.Run(ctx, large.ID); err != nil {
						t.Fatal(err)
					}
				}
				prior = large
			}
			current := submit("session-current", "current goal")
			s, err := f.fresh().Run(ctx, current.ID)
			if err != nil {
				t.Fatal(err)
			}
			wantState(t, s, core.PhaseReturned, "")
			req, err := f.open().LatestRequest(ctx, current.ID)
			if err != nil || req.Context == nil {
				t.Fatalf("context = %+v, %v", req.Context, err)
			}
			c := req.Context
			if c.SessionNavigation != prior.ID || c.Bytes.Session <= 0 {
				t.Fatalf("navigation = %+v", c)
			}
			if mode == "navigation-only" {
				if len(c.Session) != 0 {
					t.Fatalf("omitted sources entered metadata: %+v", c.Session)
				}
			} else {
				wantStatus := "returned"
				if mode == "status-only" {
					wantStatus = "stopped"
				}
				want := []harness.ContextSessionSource{{RunID: prior.ID, SessionSeq: prior.SessionSeq, Status: wantStatus, IncludedGoal: mode == "dialogue"}}
				if !reflect.DeepEqual(c.Session, want) {
					t.Fatalf("session sources = %+v, want %+v", c.Session, want)
				}
			}
			checkCapturedBytes(t, req, f.model.prompts[len(f.model.prompts)-1])
		})
	}
}
