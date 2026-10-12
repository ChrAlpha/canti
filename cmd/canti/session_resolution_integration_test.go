//go:build integration

package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

func TestSessionUnknownResolutionPreservesTheRunningEffect(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		name := "resume"
		if stopping {
			name = "stop"
		}
		t.Run(name, func(t *testing.T) {
			f, image := recoveryFixture(t, map[int]core.Decision{
				1: {Kind: core.Exec, Payload: "echo x >> count; " + released + "echo original"},
				2: {Kind: core.Return, Payload: "first done"},
			})
			unavailable := filepath.Join(t.TempDir(), "unavailable")
			path := dockerProxy(t, unavailableInspection(unavailable))

			_, wait := f.start(path, "run", "-session", "s", "-input", "one", "-goal", "do once", "-image", image)
			awaitPhase(t, f, core.PhaseAwaitingObservation)
			waitFor(t, func() bool {
				data, err := os.ReadFile(filepath.Join(f.workspace, "count"))
				return err == nil && string(data) == "x\n"
			})
			writeFile(t, unavailable)
			if r := wait(); r.code != exitResolution {
				t.Fatalf("run = %+v", r)
			}
			before := f.history()
			s := f.status()
			if !slices.Equal(kinds(before), slices.Concat(stepKinds[:6], []string{"outcome_unknown"})) ||
				s.Phase != core.PhaseNeedsResolution || s.Run.SessionID != "s" || s.Run.InputID != "one" ||
				s.JobID == "" || s.EffectID == "" || !strings.Contains(before[len(before)-1].Facts.Failure, "job status unavailable") {
				t.Fatalf("unknown state: status=%+v history=%q", s, kinds(before))
			}
			if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].id != s.JobID || cs[0].state != "running" {
				t.Fatalf("running job = %+v, want %s", cs, s.JobID)
			}
			for _, args := range [][]string{
				{"-session", "s", "-input", "two", "-goal", "next"},
				{"-session", "other", "-input", "one", "-goal", "next"},
				{"-goal", "standalone next"},
			} {
				if r := f.cli(nil, "run", args...); r.code != exitError {
					t.Errorf("run %q = %+v, want refusal", args, r)
				}
			}
			if r := f.cli(nil, "resolve", "-execution", "unknown", "-note", "uncertain"); r.code != exitUsage || !strings.Contains(r.stderr, "unknown") {
				t.Fatalf("unknown resolution = %+v, want usage error", r)
			}
			if now := f.status(); now.Seq != s.Seq || now.Phase != s.Phase || now.EffectID != s.EffectID ||
				now.JobID != s.JobID || now.Run.ID != s.Run.ID {
				t.Fatalf("rejected operations changed state: before=%+v after=%+v", s, now)
			}
			if h := f.history(); !extends(h, before) || len(h) != len(before) {
				t.Fatalf("rejected operations changed history: %q", kinds(h))
			}
			if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].id != s.JobID || cs[0].state != "running" {
				t.Fatalf("rejected operations changed job: %+v", cs)
			}
			checkFile(t, filepath.Join(f.workspace, "count"), "x\n")
			if n := len(f.model.requests()); n != 1 {
				t.Fatalf("rejected operations sent %d model requests", n)
			}

			if stopping {
				if r := f.cli(nil, "stop"); r.code != exitResolution {
					t.Fatalf("stop while unresolved = %+v", r)
				}
				if now := f.status(); now.Phase != core.PhaseNeedsResolution || now.Reason != core.StopCancelled {
					t.Fatalf("stop state = %+v", now)
				}
			}
			if err := os.Remove(unavailable); err != nil {
				t.Fatal(err)
			}
			if r := f.cli(nil, "resolve", "-job"); r.code != exitOK {
				t.Fatalf("resolve -job = %+v", r)
			}
			resolved := f.history()
			last := resolved[len(resolved)-1]
			wantPhase := core.PhaseAwaitingObservation
			if stopping {
				wantPhase = core.PhaseCancelling
			}
			if !extends(resolved, before) || last.Kind != "job_bound" || last.From != core.PhaseNeedsResolution ||
				last.To != wantPhase || last.State.JobID != s.JobID || last.State.EffectID != s.EffectID {
				t.Fatalf("job resolution = %+v", last)
			}
			if !stopping {
				writeFile(t, filepath.Join(f.workspace, "release"))
			}
			r := f.cli(nil, "resume")
			final := f.history()
			wantKinds := slices.Concat(stepKinds[:6], []string{"outcome_unknown", "job_bound", "observed"}, stepKinds[:4])
			if stopping {
				wantKinds = slices.Concat(stepKinds[:6], []string{"outcome_unknown", "stop_requested", "job_bound", "observed"})
			}
			if !extends(final, resolved) || !slices.Equal(kinds(final), wantKinds) ||
				len(boundJobs(t, final)) != 1 || boundJobs(t, final)[s.EffectID] != s.JobID {
				t.Fatalf("final history = %q", kinds(final))
			}
			if stopping {
				if r.code != exitStopped || f.status().Phase != core.PhaseStopped {
					t.Fatalf("stopped resume = %+v, phase=%s", r, f.status().Phase)
				}
			} else if r.code != exitOK || r.stdout != "first done\n" || f.status().Phase != core.PhaseReturned {
				t.Fatalf("resumed run = %+v, phase=%s", r, f.status().Phase)
			}
			checkReleased(t, f, final)
			checkReplay(t, s.Run.ID, final)
			checkFile(t, filepath.Join(f.workspace, "count"), "x\n")
			f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"next done"}`})
			if r := f.cli(nil, "run", "-session", "s", "-input", "two", "-goal", "next", "-image", image); r.code != exitOK || r.stdout != "next done\n" {
				t.Fatalf("next input = %+v", r)
			}
			next := f.status()
			if next.Run.ID == s.Run.ID || next.Run.SessionSeq != 2 || next.Phase != core.PhaseReturned ||
				len(f.model.prompts(1)) != 2 {
				t.Fatalf("next state = %+v, step 1 requests=%d", next, len(f.model.prompts(1)))
			}
			checkFile(t, filepath.Join(f.workspace, "count"), "x\n")
			if cs := effectContainers(t, f.state); len(cs) != 0 {
				t.Errorf("containers after next input = %+v", cs)
			}
		})
	}
}
