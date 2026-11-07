//go:build integration

package main

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/hostrpc"
)

func TestAHostKeepsDrivingADockerJobAfterItsClientGoesAway(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "sleep 3; echo kept"}, 2: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	client, wait := f.start(nil, "run", "-host", "auto", "-goal", "g", "-input", "detached", "-image", image)
	awaitPhase(t, f, core.PhaseAwaitingObservation)
	client.Process.Kill()
	wait()
	if r := f.cli(nil, "resume", "-host", "auto"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume after the client went away: %+v", r)
	}
	h := f.history()
	if slicesContain(kinds(h), "stop_requested") {
		t.Fatalf("a client that went away stopped its run: %q", kinds(h))
	}
	checkReleased(t, f, h)
}

func TestAHostWithoutCredentialsStopsADockerJob(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "sleep 120"}})
	f.autoHost()
	client, wait := f.start(nil, "run", "-host", "auto", "-goal", "g", "-input", "stopped", "-image", image)
	awaitPhase(t, f, core.PhaseAwaitingObservation)
	syscall.Kill(f.hostPIDs()[0], syscall.SIGKILL)
	client.Process.Kill()
	wait()
	r := runCLI(t, nil, "stop", "-host", "auto", "-workspace", f.workspace, "-state", f.state)
	if r.code != exitStopped || len(f.hostPIDs()) != 2 {
		t.Fatalf("stop by a host without credentials: %+v, hosts %v", r, f.hostPIDs())
	}
	h := f.history()
	last := h[len(h)-1]
	if last.Kind != "observed" || last.Facts.Observation == nil || last.Facts.Observation.Execution != core.ExecutionCancelled {
		t.Fatalf("the stop ended with %+v", last)
	}
	checkReleased(t, f, h)
}

func TestAHostShutdownLeavesItsDockerJobRunning(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "sleep 5; echo survived"}, 2: {Kind: core.Return, Payload: "done"}})
	var mu sync.Mutex
	var calls []string
	proxy := dockerProxy(t, func(_ http.ResponseWriter, r *http.Request) bool {
		mu.Lock()
		calls = append(calls, r.Method+" "+r.URL.Path)
		mu.Unlock()
		return false
	})
	h, hwait := f.start(proxy, "host")
	waitFor(t, func() bool {
		_, err := (&hostrpc.Client{Path: f.socket()}).Hello(context.Background())
		return err == nil
	})
	client, cwait := f.start(nil, "run", "-host", "auto", "-goal", "g", "-input", "survives", "-image", image)
	awaitPhase(t, f, core.PhaseAwaitingObservation)
	before := kinds(f.history())
	h.Process.Signal(syscall.SIGTERM)
	if r := hwait(); r.code != exitOK {
		t.Fatalf("host: %+v", r)
	}
	client.Process.Kill()
	cwait()
	mu.Lock()
	for _, c := range calls {
		if strings.HasSuffix(c, "/stop") || strings.HasSuffix(c, "/kill") {
			t.Errorf("the host's shutdown called %s", c)
		}
	}
	mu.Unlock()
	if after := kinds(f.history()); strings.Join(after, " ") != strings.Join(before, " ") {
		t.Fatalf("shutdown changed the journal from %q to %q", before, after)
	}
	if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].state != "running" {
		t.Fatalf("after the host's shutdown the job is %+v, want it running", cs)
	}
	f.autoHost()
	if r := f.cli(nil, "resume", "-host", "auto"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume by the next host: %+v", r)
	}
	checkReleased(t, f, f.history())
}

func TestAHostKilledAfterACommitResumesFromTheJournal(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "sleep 1; echo once"}, 2: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	r := f.cli([]string{"CANTI_TEST_CRASH=after:job_started"}, "run", "-host", "auto", "-goal", "g", "-input", "crash", "-image", image)
	if r.code != exitError || !strings.Contains(r.stderr, "the host went away") {
		t.Fatalf("a client whose host was killed: %+v", r)
	}
	if r := f.cli(nil, "resume", "-host", "auto"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume: %+v", r)
	}
	h := f.history()
	checkReplay(t, f.status().Run.ID, h)
	checkReleased(t, f, h)
}
