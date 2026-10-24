package harness_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

type contextCostModel struct {
	*anthropic.Model
	commands []string
}

func (m *contextCostModel) Complete(_ context.Context, req harness.Request) (harness.Response, error) {
	if req.Adapter == "canti-anthropic/5-summary" {
		decision, _ := json.Marshal(core.Decision{Kind: core.Return, Payload: "Continue inspecting original evidence."})
		body, _ := json.Marshal(map[string]any{"stop_reason": "end_turn", "content": []map[string]string{{"type": "text", "text": string(decision)}}})
		return harness.Response{Adapter: req.Adapter, Complete: true, UsageKnown: true, Body: body}, nil
	}
	if len(m.commands) == 0 {
		return harness.Response{}, fmt.Errorf("no scripted decision")
	}
	body, err := json.Marshal(core.Decision{Kind: core.Exec, Payload: m.commands[0]})
	m.commands = m.commands[1:]
	return harness.Response{Body: body, Complete: true}, err
}

func (m *contextCostModel) Decision(r harness.Response) []byte { return r.Body }

type contextCostFixture struct {
	store      *sqlite.Store
	db         *sql.DB
	loop       *harness.Loop
	probe      *probe
	runner     harness.Runner
	snapshot   harness.Snapshot
	request    *harness.Request
	factsBytes int
}

func newContextCostFixture(b *testing.B, scenario string) contextCostFixture {
	b.Helper()
	ctx := context.Background()
	root := b.TempDir()
	store, err := sqlite.Open(ctx, filepath.Join(root, "canti.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })
	state, err := core.NewState(runID, core.Limits{MaxSteps: 128, MaxAttempts: 3})
	if err != nil {
		b.Fatal(err)
	}
	if err := store.CreateRun(ctx, harness.Run{ID: runID, Goal: "Inspect the original evidence.", Created: created}, state); err != nil {
		b.Fatal(err)
	}
	model, err := anthropic.New(anthropic.Config{APIKey: "benchmark-fixture", Model: "fixture-model"})
	if err != nil {
		b.Fatal(err)
	}
	imported := filepath.Join(root, "archive")
	if err := os.Mkdir(imported, 0700); err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte("evidence\n"), 0600); err != nil {
		b.Fatal(err)
	}
	commands := []string{"true"}
	switch scenario {
	case "near_capacity":
		commands = make([]string, 100)
		for i := range commands {
			commands[i] = "printf '" + strings.Repeat("payload ", 700) + "'"
		}
	case "retrieval_dense":
		commands = make([]string, 32)
		for i := range commands {
			commands[i] = `canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"verify evidence","limit":8}`
		}
	case "rejected_1MiB":
		query, err := json.Marshal(archive.RetrievalQuery{Operation: "read", Path: "/canti/imports/evidence", Reason: strings.Repeat("x", 1<<20)})
		if err != nil {
			b.Fatal(err)
		}
		commands = []string{"canti:retrieve " + string(query), "true", "true", "true"}
	}
	m := &contextCostModel{Model: model, commands: commands}
	c := &clock{now: created}
	x := &fakeExec{clock: c, jobs: map[string]*fakeJob{}, fenced: map[string]bool{}}
	cfg := harness.LoopConfig{Environment: "fixture shell",
		History: filepath.Join(root, "history"), Reader: &archive.Reader{Imports: imported}, Now: c.Now,
		Poll: time.Microsecond, Backoff: -1, Limits: harness.DefaultPromptLimits()}
	cfg.Limits.Total = 256 << 10
	loop, err := harness.NewLoop(store, x, m, cfg)
	if err != nil {
		b.Fatal(err)
	}
	p := &probe{Store: store}
	prepare, err := harness.NewLoop(p, x, m, cfg)
	if err != nil {
		b.Fatal(err)
	}
	var snap harness.Snapshot
	ready := false
	for range 1000 {
		snap, err = store.Latest(ctx, runID)
		if err != nil {
			b.Fatal(err)
		}
		if snap.State.Phase == core.PhaseIdle {
			p.frozen = nil
			if _, err := prepare.Step(ctx, runID); err != nil {
				b.Fatal(err)
			}
			if p.frozen == nil {
				continue
			}
			if scenario == "near_capacity" && (p.frozen.CheckpointID != "") || len(m.commands) == 0 {
				ready = true
				break
			}
		}
		if snap.State.Intent() == core.IntentHalt {
			b.Fatal("fixture halted before benchmark boundary")
		}
		if _, err := loop.Step(ctx, runID); err != nil {
			b.Fatal(err)
		}
	}
	if !ready || snap.State.Phase != core.PhaseIdle || p.frozen == nil {
		b.Fatal("fixture did not reach prepare boundary")
	}
	if scenario == "near_capacity" && !(p.frozen.CheckpointID != "") {
		b.Fatal("fixture did not reach the compaction capacity threshold")
	}
	runner, err := harness.NewRunner(store, x)
	if err != nil {
		b.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "canti.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	f := contextCostFixture{store: store, db: db, loop: prepare, probe: p, runner: runner, snapshot: snap, request: p.frozen}
	if _, err := runner.Commit(ctx, snap, core.RequestFrozen{}, harness.Facts{Request: f.request}); err != nil {
		b.Fatal(err)
	}
	if err := db.QueryRow("SELECT length(CAST(facts AS BLOB)) FROM transitions WHERE run_id = ? AND seq = ?", runID, snap.Seq+1).Scan(&f.factsBytes); err != nil {
		b.Fatal(err)
	}
	f.resetRequest(b)
	b.Logf("scenario=%s step=%d checkpoint_adopted=%t body_bytes=%d frozen_facts_bytes=%d body_sha256=%x", scenario, snap.State.Step, (f.request.CheckpointID != ""), len(f.request.Body), f.factsBytes, sha256.Sum256(f.request.Body))
	return f
}

func (f contextCostFixture) resetRequest(b *testing.B) {
	b.Helper()
	if _, err := f.db.Exec("DELETE FROM transitions WHERE run_id = ? AND seq = ? AND kind = 'request_frozen'", runID, f.snapshot.Seq+1); err != nil {
		b.Fatal(err)
	}
}

func (f contextCostFixture) reportBytes(b *testing.B) {
	b.ReportMetric(float64(len(f.request.Body)), "body_bytes")
	b.ReportMetric(float64(f.factsBytes), "frozen_facts_bytes")
}

func BenchmarkContextCost(b *testing.B) {
	for _, scenario := range []string{"short", "near_capacity", "retrieval_dense", "rejected_1MiB"} {
		b.Run(scenario, func(b *testing.B) {
			f := newContextCostFixture(b, scenario)
			ctx := context.Background()
			b.Run("prepare_freeze", func(b *testing.B) {
				f.resetRequest(b)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if _, err := f.loop.Step(ctx, runID); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if sha256.Sum256(f.probe.frozen.Body) != sha256.Sum256(f.request.Body) {
					b.Fatal("repeated prepare changed the frozen body")
				}
				f.reportBytes(b)
			})
			b.Run("commit", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					f.resetRequest(b)
					b.StartTimer()
					if _, err := f.runner.Commit(ctx, f.snapshot, core.RequestFrozen{}, harness.Facts{Request: f.request}); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				f.reportBytes(b)
			})
			b.Run("reload", func(b *testing.B) {
				f.resetRequest(b)
				if _, err := f.runner.Commit(ctx, f.snapshot, core.RequestFrozen{}, harness.Facts{Request: f.request}); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				var request harness.Request
				var err error
				for b.Loop() {
					if request, err = f.store.Request(ctx, runID, f.snapshot.State.Step, f.snapshot.State.RequestRev+1); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if sha256.Sum256(request.Body) != sha256.Sum256(f.request.Body) {
					b.Fatal("reload changed the frozen body")
				}
				f.reportBytes(b)
			})
		})
	}
}
