package hostrpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/host"
	sdk "github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func TestHistoryBeforeIsExclusiveForACallerPosition(t *testing.T) {
	h, c := serving(t)
	ctx := context.Background()
	spec := host.Spec{Config: host.RunConfig{Ephemeral: true, Workspace: h.Workspace(), Image: "debian:bookworm-slim", Network: "none",
		Memory: "2g", Pids: 512, OutputLimit: 8 << 20, API: host.APIMessages, Model: "m", MaxTokens: 1000, Structured: true,
		RequestBytes: 256 << 10, ContextVersion: host.ContextVersion},
		Limits: core.Limits{MaxSteps: 20, MaxAttempts: 3},
		Budget: harness.Budget{WallTime: time.Hour, ContextTokens: 200_000, OutputBytes: 1 << 20, JobTimeout: time.Minute}}
	if _, err := h.Submit(ctx, host.Submission{Goal: "goal", Spec: spec, Now: time.Now(),
		Binding: host.Binding{Provider: "http://127.0.0.1:1", Keys: host.Keys{AnthropicKey: "k"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Submit(ctx, host.Submission{InputID: "steer", Goal: "steer", Delivery: harness.DeliverySteer, Spec: spec, Now: time.Now(), Binding: host.Binding{Provider: "http://127.0.0.1:1", Keys: host.Keys{AnthropicKey: "k"}}}); err != nil {
		t.Fatal(err)
	}
	first, err := c.History(ctx, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if first.Older == "" {
		t.Fatal("first page has no continuation")
	}
	page, err := c.HistoryPage(ctx, first.Older, first.Line, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range page.Items {
		if item.Pos >= 2 {
			t.Fatalf("History({pos:2}) included position %d; Before must be exclusive", item.Pos)
		}
	}
}

func TestHistoryCursorRejectsABarePosition(t *testing.T) {
	if _, err := host.ParseHistoryCursor("pos:2"); err == nil {
		t.Fatal("a bare position was accepted as a server-issued cursor")
	}
	if _, err := host.ParseHistoryCursor(`{"pos":2}`); err == nil {
		t.Fatal("a position-only JSON cursor was accepted")
	}
	if _, err := host.ParseHistoryCursor(`{"seq":{}}`); err == nil {
		t.Fatal("a cursor without a position was accepted")
	}
	if c, err := host.ParseHistoryCursor(`{"pos":2,"seq":{"run_1":3},"session":"s"}`); err != nil || c.Pos != 2 || c.Seq["run_1"] != 3 {
		t.Fatalf("server cursor = %+v, %v", c, err)
	}
}

func TestHistoryRejectsAForgedSnapshotCursor(t *testing.T) {
	h, c := serving(t)
	ctx := context.Background()
	spec := host.Spec{Config: host.RunConfig{Ephemeral: true, Workspace: h.Workspace(), Image: "debian:bookworm-slim", Network: "none",
		Memory: "2g", Pids: 512, OutputLimit: 8 << 20, API: host.APIMessages, Model: "m", MaxTokens: 1000, Structured: true,
		RequestBytes: 256 << 10, ContextVersion: host.ContextVersion},
		Limits: core.Limits{MaxSteps: 20, MaxAttempts: 3},
		Budget: harness.Budget{WallTime: time.Hour, ContextTokens: 200_000, OutputBytes: 1 << 20, JobTimeout: time.Minute}}
	if _, err := h.Submit(ctx, host.Submission{Goal: "goal", Spec: spec, Now: time.Now(), Binding: host.Binding{Provider: "http://127.0.0.1:1", Keys: host.Keys{AnthropicKey: "k"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Submit(ctx, host.Submission{InputID: "steer", Goal: "steer", Delivery: harness.DeliverySteer, Spec: spec, Now: time.Now(), Binding: host.Binding{Provider: "http://127.0.0.1:1", Keys: host.Keys{AnthropicKey: "k"}}}); err != nil {
		t.Fatal(err)
	}
	page, err := c.History(ctx, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if page.Older == "" {
		t.Fatal("first page has no continuation")
	}
	if _, err := c.HistoryPage(ctx, `{"pos":2,"seq":{}}`, page.Line, 1); !errors.Is(err, sdk.ErrCursor) {
		t.Fatalf("forged cursor error = %v", err)
	}
}
