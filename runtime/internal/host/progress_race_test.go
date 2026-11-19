package host

import (
	"context"
	"runtime"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

// TestProgressQuiescentPageCarriesTheTerminal requires that a page that
// reports quiescence was read in a snapshot that also contains the run's
// terminal row. The old two-read projection could sample rows before a return
// and quiescence after it, leaving the page with no returned item.
func TestProgressQuiescentPageCarriesTheTerminal(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		h := testHost(t, Options{})
		url := answering(t, core.Decision{Kind: core.Return, Payload: "done"})
		ack := submit(t, h, Submission{Goal: "race", Binding: binding(url)})
		go func() {
			d, err := h.Drive(context.Background(), ack.Run.ID, binding(url), DriveOptions{})
			if err == nil {
				_, _ = d.Result()
			}
		}()
		var items []Item
		cursor := Cursor(0)
		for {
			page, err := h.Progress(context.Background(), cursor, 64)
			if err != nil {
				t.Fatal(err)
			}
			items = append(items, page.Items...)
			cursor = page.Next
			if page.Quiescent {
				returned := false
				for _, item := range items {
					if item.Kind == ItemReturned {
						returned = true
					}
				}
				if !returned {
					t.Fatalf("iteration %d: quiescent page has no returned item; items = %+v", iteration, items)
				}
				break
			}
			if !page.More && len(page.Items) == 0 {
				runtime.Gosched()
			}
		}
	}
}
