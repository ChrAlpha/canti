package harness_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestByteOnlyWindowTriggerPersistsHandoverAcrossReopen(t *testing.T) {
	for _, tc := range []struct {
		name      string
		threshold int
		requested bool
	}{{"threshold reached", 4096, true}, {"below threshold", 16384, false}} {
		t.Run(tc.name, func(t *testing.T) {
			archive := t.TempDir()
			evidence := "BYTE_WINDOW_ORIGINAL_EVIDENCE:" + strings.Repeat("x", 5000)
			if err := os.WriteFile(filepath.Join(archive, "evidence"), []byte(evidence), 0600); err != nil {
				t.Fatal(err)
			}
			replies := []reply{execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"recover original evidence","limit":8192}`)}
			if tc.requested {
				replies = append(replies, execReply(`canti:handover {"run_id":"run-1","first_step":1,"text":"Continue using original evidence references as needed."}`))
			}
			replies = append(replies, returnReply("done"))
			f := newFixture(t, limits, harness.Budget{}, replies...)
			f.cfg.Reader = &harness.Reader{Imports: archive}
			f.cfg.WindowBytes = tc.threshold
			ctx := f.bounded()
			for {
				s, err := f.fresh().Step(ctx, runID)
				if err != nil {
					t.Fatal(err)
				}
				if s.State.Intent() == core.IntentHalt {
					if s.State.Phase != core.PhaseReturned {
						t.Fatalf("unexpected halt: %+v", s.State)
					}
					break
				}
			}
			request, err := f.store.Request(ctx, runID, 2, 1)
			if err != nil || request.Window == nil || request.Window.FirstStep != 1 || request.Window.Requested != tc.requested {
				t.Fatalf("threshold %d window=%+v err=%v", tc.threshold, request.Window, err)
			}
			if !strings.Contains(string(request.Body), evidence) {
				t.Fatal("byte threshold discarded original evidence before handover")
			}
			if tc.requested {
				request, err = f.store.Request(ctx, runID, 3, 1)
				if err != nil || request.Window == nil || request.Window.FirstStep != 3 || request.Window.SourceStep != 2 || request.Window.Requested {
					t.Fatalf("new byte-triggered window=%+v err=%v", request.Window, err)
				}
				body := string(request.Body)
				if !strings.Contains(body, "Continue using original evidence references") || strings.Contains(body, evidence) || strings.Contains(body, "### Step 1") || strings.Contains(body, "### Step 2") {
					t.Fatal("new byte-triggered window did not retain only the accepted handover")
				}
			}
		})
	}
}
