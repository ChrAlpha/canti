package hostrpc

import (
	"context"
	"errors"
	"testing"
	"time"

	sdk "github.com/ChrAlpha/canti/sdk/go/hostrpc"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/host"
)

func TestHistoryLineTokenPinsPaginationAcrossAFreshLine(t *testing.T) {
	h, c := serving(t)
	ctx := context.Background()
	spec := host.Spec{Config: host.RunConfig{Ephemeral: true, Workspace: h.Workspace(), Image: "debian:bookworm-slim", Network: "none",
		Memory: "2g", Pids: 512, OutputLimit: 8 << 20, API: host.APIMessages, Model: "m", MaxTokens: 1000, Structured: true,
		RequestBytes: 256 << 10, ContextVersion: host.ContextVersion},
		Limits: core.Limits{MaxSteps: 20, MaxAttempts: 3},
		Budget: harness.Budget{WallTime: time.Hour, ContextTokens: 200_000, OutputBytes: 1 << 20, JobTimeout: time.Minute}}
	binding := host.Binding{Provider: "http://127.0.0.1:1", Keys: host.Keys{AnthropicKey: "test-credential-key"}}
	first, err := h.Submit(ctx, host.Submission{Goal: "first line", Spec: spec, Binding: binding, Now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Stop(ctx, first.Run.ID); err != nil {
		t.Fatal(err)
	}
	page, err := c.History(ctx, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if page.Line == "" || page.Older == "" || len(page.Items) != 1 || page.Items[0].RunID != first.Run.ID {
		t.Fatalf("first page = %+v, want the first line's newest row", page)
	}
	second, err := h.Submit(ctx, host.Submission{Goal: "second line", Spec: spec, Binding: binding, Fresh: true, Now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	older, err := c.HistoryPage(ctx, page.Older, page.Line, 64)
	if err != nil {
		t.Fatal(err)
	}
	if older.Line != page.Line || older.Latest != nil {
		t.Fatalf("older page = %+v, want the same line without a latest summary", older)
	}
	for _, item := range older.Items {
		if item.RunID != first.Run.ID {
			t.Fatalf("older page crossed into the fresh line %s: %+v", second.Run.ID, item)
		}
	}
	if _, err := c.HistoryPage(ctx, "", "not-a-line", 1); !errors.Is(err, sdk.ErrCursor) {
		t.Fatalf("an invalid line token = %v, want ErrCursor", err)
	}
}
