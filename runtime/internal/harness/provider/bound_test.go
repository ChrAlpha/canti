package provider

import (
	"strings"
	"testing"
)

func TestBoundReaderNeverExposesTheOverflowByte(t *testing.T) {
	r := &boundReader{Reader: strings.NewReader("abcd"), remaining: 3, limit: 3}
	buf := make([]byte, 16)
	n, err := r.Read(buf)
	if n != 3 || err != nil || string(buf[:n]) != "abc" {
		t.Fatalf("first read = %q, %v", buf[:n], err)
	}
	n, err = r.Read(buf)
	if n != 0 || err == nil || !strings.Contains(err.Error(), "stream exceeds") {
		t.Fatalf("overflow read = %q, %v", buf[:n], err)
	}
	if string(buf[:n]) == "d" {
		t.Fatal("overflow byte escaped")
	}
}

func TestBoundDiagnosticKeepsAMarkerThatExactlyFits(t *testing.T) {
	marker := []byte(unparseableMarker)
	got := boundDiagnostic(append([]byte("prefix"), marker...), int64(len(marker)))
	if string(got) != string(marker) {
		t.Fatalf("boundDiagnostic = %q, want the marker", got)
	}
}
