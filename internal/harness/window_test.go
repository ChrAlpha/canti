package harness

import (
	"strings"
	"testing"
)

func TestWindowProvenance(t *testing.T) {
	for _, w := range []Window{{FirstStep: 1}, {FirstStep: 8, Through: 50, Summary: "verify earlier evidence"}, {FirstStep: 1, Through: 50, Summary: "previous run"}} {
		if err := w.Validate(8); err != nil {
			t.Fatal(err)
		}
	}
	for _, w := range []Window{{}, {FirstStep: 9}, {FirstStep: 1, Summary: "uncovered"}, {FirstStep: 8, Through: -1}, {FirstStep: 8, Through: 50}} {
		if err := w.Validate(8); err == nil {
			t.Fatalf("accepted %+v", w)
		}
	}
}

func TestSummaryReportsExactUTF8ByteOverflow(t *testing.T) {
	err := ValidateSummary(strings.Repeat("界", 683), 2048)
	if err == nil || !strings.Contains(err.Error(), "2049 bytes; limit 2048") {
		t.Fatalf("overflow: %v", err)
	}
}
