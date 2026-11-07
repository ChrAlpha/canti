//go:build integration

package main

import (
	"context"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/hostrpc"
)

func TestAHostKeepsDrivingADockerJobAfterItsClientGoesAway(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: released + "echo kept"}, 2: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	client, wait := f.start(nil, "run", "-host", "auto", "-goal", "g", "-input", "detached", "-image", image)
	awaitRunningEffect(t, f)
	before := f.history()
	inspected := f.cli(nil, "inspect", "-host", "auto")
	var sample hostrpc.Inspection
	if inspected.code != exitOK || strict(inspected.stdout, &sample) != nil || sample.Receipt == nil || sample.Receipt.Status != "running" || sample.Receipt.OwnerID != effectOwner(t, f, before[len(before)-1].State.EffectID) || sample.SampledAt == "" {
		t.Fatalf("running inspection: %+v sample %+v", inspected, sample)
	}
	if after := f.history(); !reflect.DeepEqual(after, before) {
		t.Fatalf("inspection changed journal: %q", kinds(after))
	}
	client.Process.Kill()
	wait()
	writeFile(t, filepath.Join(f.workspace, "release"))
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
	awaitRunningEffect(t, f)
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
	awaitRunningEffect(t, f)
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
	r := f.cli([]string{"CANTI_TEST_EXECUTOR_CRASH=running"}, "run", "-host", "auto", "-goal", "g", "-input", "crash", "-image", image)
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

func TestInspectionSamplesAcceptedEffectWithoutLaunchingOrCommitting(t *testing.T) {
	for _, mode := range []string{"embedded", "auto"} {
		t.Run(mode, func(t *testing.T) {
			f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "echo once >> count"}, 2: {Kind: core.Return, Payload: "done"}})
			if mode == "auto" {
				f.autoHost()
			}
			crashRun(t, f, "after:response_loaded", "run", "-goal", "inspect accepted", "-image", image)
			before := f.history()
			effect := before[len(before)-1].State.EffectID
			r := f.cli(nil, "inspect", "-host", mode)
			var sample hostrpc.Inspection
			if r.code != exitOK || strict(r.stdout, &sample) != nil || sample.EffectID != effect || sample.SampledAt == "" || sample.Unavailable != "" || sample.Receipt == nil || sample.Receipt.Status != "absent" || sample.Receipt.OwnerID != "" {
				t.Fatalf("accepted inspection: %+v, sample %+v", r, sample)
			}
			if after := f.history(); !reflect.DeepEqual(after, before) {
				t.Fatalf("inspection committed %q", kinds(after))
			}
			if cs := effectContainers(t, f.state); len(cs) != 0 {
				t.Fatalf("inspection created containers: %+v", cs)
			}
			if r := f.cli(nil, "resume", "-host", mode); r.code != exitOK {
				t.Fatalf("resume: %+v", r)
			}
			checkFile(t, filepath.Join(f.workspace, "count"), "once\n")
			checkReleased(t, f, f.history())
		})
	}
}

func TestColdAutoInspectionDoesNotDriveADurablePendingStop(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "sleep 120"}})
	crashRun(t, f, "executor:running", "run", "-goal", "inspect pending stop", "-image", image)
	crashRun(t, f, "after:stop_requested", "stop")
	before := f.history()
	effect := before[len(before)-1].State.EffectID
	if before[len(before)-1].To != core.PhaseCancelling {
		t.Fatalf("pending stop: %q", kinds(before))
	}
	f.autoHost()
	r := f.cli(nil, "inspect", "-host", "auto")
	var sample hostrpc.Inspection
	if r.code != exitOK || strict(r.stdout, &sample) != nil || sample.Receipt == nil || sample.Receipt.Status != "running" || sample.Receipt.OwnerID != effectOwner(t, f, effect) {
		t.Fatalf("cold inspection: %+v, sample %+v", r, sample)
	}
	if after := f.history(); !reflect.DeepEqual(after, before) {
		t.Fatalf("cold inspection drove pending stop: before %q after %q", kinds(before), kinds(after))
	}
	if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].effect != effect || cs[0].state != "running" {
		t.Fatalf("cold inspection stopped owner: %+v", cs)
	}
	if len(f.model.requests()) != 1 {
		t.Fatalf("cold inspection sent model requests: %d", len(f.model.requests()))
	}
	if stopped := f.cli(nil, "stop", "-host", "embedded"); stopped.code != exitStopped {
		t.Fatalf("finish pending stop: %+v", stopped)
	}
	checkReleased(t, f, f.history())
}
