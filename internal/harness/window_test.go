package harness

import "testing"

func TestWindowProvenance(t *testing.T) {
	for _, w := range []Window{{FirstStep: 1}, {FirstStep: 8, SourceStep: 7, Handover: "verify step 5"}} {
		if err := w.Validate(8); err != nil {
			t.Fatal(err)
		}
	}
	for _, w := range []Window{{}, {FirstStep: 9}, {FirstStep: 1, Handover: "stale"}, {FirstStep: 8, SourceStep: 3, Handover: "stale"}, {FirstStep: 8, SourceStep: 7}} {
		if err := w.Validate(8); err == nil {
			t.Fatalf("accepted %#v", w)
		}
	}
}
