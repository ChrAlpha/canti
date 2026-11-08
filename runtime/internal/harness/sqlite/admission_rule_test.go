package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// TestAdmissionRefusesWhileAnyRunIsOpenOrAbandonedUnknown requires that a
// new run is admitted only when every run of the journal has returned or
// stopped and none was abandoned with an effect whose outcome stays unknown.
func TestAdmissionRefusesWhileAnyRunIsOpenOrAbandonedUnknown(t *testing.T) {
	type stepFn func(d *driver)
	ex := func(d *driver) { d.decide(execBody("printf ok"), harness.Usage{}) }
	unknown := func(d *driver) {
		d.do(core.OutcomeUnknown{EffectID: d.snap.State.EffectID}, harness.Facts{Failure: "lost"})
	}
	stop := func(r core.StopReason) stepFn {
		return func(d *driver) { d.do(core.StopRequested{Reason: r}, harness.Facts{}) }
	}
	resolve := func(e core.ExecutionState) stepFn {
		return func(d *driver) {
			d.do(core.Resolved{EffectID: d.snap.State.EffectID, Execution: e}, harness.Facts{Observation: observation(e, 0), Resolution: &harness.Resolution{Source: harness.ResolutionOperator, OwnerID: "operator-owner"}})
		}
	}
	scenarios := map[string][]stepFn{
		"fresh":                  nil,
		"frozen":                 {func(d *driver) { d.freeze() }},
		"sent":                   {func(d *driver) { d.freeze(); d.send() }},
		"saved":                  {func(d *driver) { d.freeze(); d.send(); d.save(execBody("x"), harness.Usage{}) }},
		"effect":                 {ex},
		"next step":              {ex, func(d *driver) { d.observe(core.ExecutionExited, 0) }},
		"returned":               {func(d *driver) { d.decide([]byte(`{"kind":"return","payload":"done"}`), harness.Usage{}) }},
		"stopped idle":           {stop(core.StopCancelled)},
		"cancelling":             {ex, stop(core.StopCancelled)},
		"cancelled":              {ex, stop(core.StopCancelled), func(d *driver) { d.observe(core.ExecutionCancelled, 0) }},
		"needs resolution":       {ex, unknown},
		"abandoned":              {ex, unknown, resolve(core.ExecutionUnknown)},
		"abandoned after a stop": {ex, unknown, stop(core.StopTimedOut), resolve(core.ExecutionUnknown)},
		"resolved after a stop":  {ex, unknown, stop(core.StopTimedOut), resolve(core.ExecutionExited)},
		"resolved and continued": {ex, unknown, resolve(core.ExecutionExited)},
		"jobless abandoned":      {ex, unknown, resolve(core.ExecutionUnknown)},
	}
	blocks := map[string]bool{"": false, "returned": false, "stopped idle": false, "cancelled": false, "resolved after a stop": false}
	for name, steps := range scenarios {
		for _, later := range []string{"", "returned", "abandoned"} {
			t.Run(fmt.Sprintf("%s then %q", name, later), func(t *testing.T) {
				s := open(t, tempPath(t))
				ctx := context.Background()
				drive := func(id string, steps []stepFn) {
					if _, err := s.db.Exec(`INSERT INTO runs (id, semantics, run, initial) VALUES (?, ?, ?, ?)`, id, core.SemanticsVersion,
						`{"ID":"`+id+`","Goal":"g","Constraints":"","Budget":{"WallTime":0,"Tokens":0,"OutputBytes":0,"JobTimeout":0},"Created":"0001-01-01T00:00:00Z","Config":"eyJleGVjdXRvciI6InN0dWIifQ=="}`,
						`{"RunID":"`+id+`","Step":1,"Attempt":1,"RequestRev":0,"Phase":"idle","Limits":{"MaxSteps":3,"MaxAttempts":2},"Decision":{"kind":"","payload":""},"EffectID":"","Reason":""}`); err != nil {
						t.Fatal(err)
					}
					d := newDriver(t, s, harness.Snapshot{State: fresh(t, id, limits)})
					for _, step := range steps {
						step(d)
					}
				}
				drive("run-a", steps)
				if later != "" {
					drive("run-b", scenarios[later])
				}
				want := blocksOf(blocks, name) || blocksOf(blocks, later)
				if err := s.CheckAdmission(ctx); (err != nil) != want {
					t.Fatalf("CheckAdmission = %v, want blocked %v", err, want)
				}
			})
		}
	}
}

func blocksOf(closed map[string]bool, scenario string) bool {
	blocked, listed := closed[scenario]
	return !listed || blocked
}

// TestCreateRunAdmitsInItsTransaction requires that CreateRun, like Submit,
// refuses a new run while another blocks, writing nothing, and still
// confirms an identical retry of a run that is itself open.
func TestCreateRunAdmitsInItsTransaction(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := createRun(t, s, "run-1", limits)
	if err := s.CreateRun(ctx, testRun("run-1"), first.State); err != nil {
		t.Fatalf("identical retry of the open run: %v", err)
	}
	if err := s.CreateRun(ctx, testRun("run-2"), fresh(t, "run-2", limits)); err == nil || !strings.Contains(err.Error(), `run "run-1" is unfinished`) {
		t.Fatalf("CreateRun beside an open run = %v", err)
	}
	if _, err := s.Run(ctx, "run-2"); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("refused run was recorded: %v", err)
	}
	d := newDriver(t, s, first)
	d.do(core.StopRequested{Reason: core.StopCancelled}, harness.Facts{})
	if err := s.CreateRun(ctx, testRun("run-2"), fresh(t, "run-2", limits)); err != nil {
		t.Fatalf("CreateRun after the run stopped: %v", err)
	}
}
