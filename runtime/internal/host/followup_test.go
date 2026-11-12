package host

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestFollowUpInheritsThePredecessorConstraints(t *testing.T) {
	h := testHost(t, Options{})
	url := answering(t, core.Decision{Kind: core.Return, Payload: "done"})
	first := submit(t, h, Submission{InputID: "first", Constraints: "Never change production", ExplicitConstraints: true, Binding: binding(url)})
	submit(t, h, Submission{InputID: "next", Goal: "next", Delivery: harness.DeliveryFollowUp, Binding: binding(url)})
	drive, err := h.Drive(context.Background(), first.Run.ID, binding(url), DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	<-drive.Done()
	waitFor(t, func() bool {
		last, err := h.store.LastRun(context.Background())
		if err != nil || last.InputID != "next" {
			return false
		}
		snap, err := h.store.Latest(context.Background(), last.ID)
		return err == nil && snap.State.Phase == core.PhaseReturned
	})
	last, err := h.store.LastRun(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if last.Constraints != "Never change production" {
		t.Fatalf("follow-up constraints = %q, want the predecessor's %q", last.Constraints, "Never change production")
	}
}

func TestFollowUpChainInheritsTheOpenedPredecessor(t *testing.T) {
	h := testHost(t, Options{})
	url := answering(t, core.Decision{Kind: core.Return, Payload: "done"})
	first := submit(t, h, Submission{InputID: "first", Constraints: "C1", ExplicitConstraints: true, Binding: binding(url)})
	submit(t, h, Submission{InputID: "chain-1", Goal: "chain one", Delivery: harness.DeliveryFollowUp,
		Constraints: "C2", ExplicitConstraints: true, Binding: binding(url)})
	submit(t, h, Submission{InputID: "chain-2", Goal: "chain two", Delivery: harness.DeliveryFollowUp, Binding: binding(url)})
	drive, err := h.Drive(context.Background(), first.Run.ID, binding(url), DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	<-drive.Done()
	waitFor(t, func() bool {
		last, err := h.store.LastRun(context.Background())
		if err != nil || last.InputID != "chain-2" {
			return false
		}
		snap, err := h.store.Latest(context.Background(), last.ID)
		return err == nil && snap.State.Phase == core.PhaseReturned
	})
	last, err := h.store.LastRun(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if last.Constraints != "C2" {
		t.Fatalf("chained follow-up constraints = %q, want the opened predecessor's %q", last.Constraints, "C2")
	}
}

func TestFollowUpBindingIsRememberedBeforeItCanOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	releaseModel := make(chan struct{})
	requested := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requested <- struct{}{}:
		default:
		}
		select {
		case <-releaseModel:
		case <-r.Context().Done():
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"id": "msg", "type": "message", "role": "assistant", "model": "test-model",
			"stop_reason": "end_turn", "content": []map[string]string{{"type": "text", "text": `{"kind":"return","payload":"done"}`}},
			"usage": map[string]int{"input_tokens": 1, "output_tokens": 1}})
	}))
	defer srv.Close()
	committed := make(chan struct{})
	releaseSubmit := make(chan struct{})
	var armed atomic.Bool
	h := testHost(t, Options{Checkpoint: func(p string) {
		if p == PointSubmitted && armed.Load() {
			close(committed)
			<-releaseSubmit
		}
	}})
	first := submit(t, h, Submission{InputID: "first", Binding: binding(srv.URL)})
	if _, err := h.Drive(ctx, first.Run.ID, binding(srv.URL), DriveOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requested:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	armed.Store(true)
	submitted := make(chan error, 1)
	go func() {
		_, err := h.Submit(ctx, Submission{InputID: "next", Goal: "next", Delivery: harness.DeliveryFollowUp,
			Spec: testSpec(h), Binding: binding(srv.URL)})
		submitted <- err
	}()
	select {
	case <-committed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(releaseModel)
	// Give an uncoordinated follow-up start the window it had before the
	// binding and the admission lock were one critical section.
	time.Sleep(100 * time.Millisecond)
	close(releaseSubmit)
	if err := <-submitted; err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		last, err := h.store.LastRun(ctx)
		if err != nil || last.InputID != "next" {
			return false
		}
		snap, err := h.store.Latest(ctx, last.ID)
		return err == nil && snap.State.Phase == core.PhaseReturned
	})
	if err := h.DriveError(first.Run.ID); err != nil {
		t.Fatalf("original drive error: %v", err)
	}
}
