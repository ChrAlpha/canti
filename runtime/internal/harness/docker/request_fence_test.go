package docker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fencedRequest(kind string) requestFence {
	return requestFence{Format: 1, Kind: kind, BackendID: "daemon", EnvironmentID: "container", TargetID: "container", Incarnation: 1, Generation: 1}
}

func TestDelayedStopCannotBeSettledByALaterStoppedSample(t *testing.T) {
	entered, release, delivered := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var stopped atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release // Deliberately ignore the disconnected client's context.
		stopped.Store(true)
		w.WriteHeader(http.StatusNoContent)
		close(delivered)
	}))
	defer server.Close()
	releaseRequest := sync.OnceFunc(func() { close(release) })
	defer releaseRequest()
	request := fencedRequest("environment_stop")
	dir, err := beginRequest(t.TempDir(), request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/containers/container/stop", nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		response, err := server.Client().Do(req)
		if response != nil {
			response.Body.Close()
		}
		done <- err
	}()
	<-entered
	cancel() // Model controller death before acknowledgement publication.
	if err = <-done; err == nil {
		t.Fatal("disconnected request unexpectedly acknowledged")
	}
	stopped.Store(true) // Independent observation, not the held request's reply.
	settled, err := requestSettled(dir, request)
	if err != nil || settled || !stopped.Load() {
		t.Fatalf("stopped sample settled old request: %v %v", settled, err)
	}
	// A restarted controller must refuse same-ID admission here. The old request
	// remains able to arrive later, despite the stopped sample and released flock.
	releaseRequest()
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("old stop did not arrive")
	}
	settled, err = requestSettled(dir, request)
	if err != nil || settled {
		t.Fatalf("unacknowledged old stop reopened admission: %v %v", settled, err)
	}
}

func TestOnlyTheExactAcknowledgedMutationCanReopenAdmission(t *testing.T) {
	request := fencedRequest("environment_stop")
	dir, err := beginRequest(t.TempDir(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRequest(dir, request, http.StatusNoContent, false); err != nil {
		t.Fatal(err)
	}
	settled, err := requestSettled(dir, request)
	if err != nil || !settled {
		t.Fatalf("acknowledgement was not retained: %v %v", settled, err)
	}
	other := request
	other.Generation++
	if settled, err = requestSettled(dir, other); err == nil || settled {
		t.Fatal("acknowledgement settled another generation")
	}
}

func TestUnstartedExecCannotUseItsDefaultsAsAcknowledgement(t *testing.T) {
	request := fencedRequest("exec_start")
	request.TargetID = "exec"
	dir, err := beginRequest(t.TempDir(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRequest(dir, request, 0, false); err == nil {
		t.Fatal("unstarted exec defaults became launch acknowledgement")
	}
	settled, err := requestSettled(dir, request)
	if err != nil || settled {
		t.Fatal("unconfirmed start can be reused")
	}
	if err = acknowledgeRequest(dir, request, 0, true); err != nil {
		t.Fatal(err)
	}
	settled, err = requestSettled(dir, request)
	if err != nil || !settled {
		t.Fatal("positively started exact exec did not settle launch request")
	}
}

func TestFailedAcknowledgementPublicationKeepsMutationUnsettled(t *testing.T) {
	request := fencedRequest("environment_start")
	dir, err := beginRequest(t.TempDir(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(dir, "ack.json"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRequest(dir, request, http.StatusNoContent, false); err == nil {
		t.Fatal("ack publication unexpectedly succeeded")
	}
	if settled, _ := requestSettled(dir, request); settled {
		t.Fatal("failed acknowledgement opened admission")
	}
}

func TestStopCannotBeAcknowledgedByExecOrErrorEvidence(t *testing.T) {
	for _, response := range []int{0, http.StatusBadRequest, http.StatusInternalServerError} {
		request := fencedRequest("environment_stop")
		dir, err := beginRequest(t.TempDir(), request)
		if err != nil {
			t.Fatal(err)
		}
		if err = acknowledgeRequest(dir, request, response, true); err == nil {
			t.Fatal("exec evidence acknowledged environment stop")
		}
		if err = acknowledgeRequest(dir, request, response, false); err == nil {
			t.Fatal("error acknowledged environment stop")
		}
	}
}

func TestAnAcknowledgementCannotBeCopiedBetweenIdenticalRequests(t *testing.T) {
	authority := t.TempDir()
	request := fencedRequest("environment_stop")
	first, err := beginRequest(authority, request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := beginRequest(authority, request)
	if err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRequest(first, request, http.StatusNoContent, false); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(first, "ack.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(second, "ack.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if settled, err := requestSettled(second, request); err == nil || settled {
		t.Fatal("copied ack settled a distinct HTTP request")
	}
}
