package harness

import (
	"strings"
	"testing"
)

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

func TestHandoverReportsExactUTF8ByteOverflow(t *testing.T) {
	h := Handover{RunID: "run", FirstStep: 7, Text: strings.Repeat("界", 683)}
	err := h.Validate("run", 7, 2048)
	if err == nil || !strings.Contains(err.Error(), "2049 UTF-8 bytes; maximum 2048") {
		t.Fatalf("overflow is not actionable: %v", err)
	}
	h.Text = "continue"
	if err := h.Validate("run", 8, 2048); err == nil || !strings.Contains(err.Error(), "first_step must be 8, got 7") {
		t.Fatalf("page mismatch: %v", err)
	}
	if err := h.Validate("other", 7, 2048); err == nil || !strings.Contains(err.Error(), "run_id") {
		t.Fatalf("run mismatch: %v", err)
	}
}
