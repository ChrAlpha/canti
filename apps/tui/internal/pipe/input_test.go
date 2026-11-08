package pipe

import (
	"bufio"
	"io"
	"strings"
	"testing"
)

func TestPipeBlocksAndBoundedLines(t *testing.T) {
	for _, tc := range []struct{ in, want string }{{"one\n", "one"}, {"<<EOF\none\ntwo\nEOF\n", "one\ntwo"}, {"<<'EOF'\na\nEOF\n", "a"}, {"<<-END\n\ta\n\tEND\n", "a"}} {
		got, err := readInput(bufio.NewReader(strings.NewReader(tc.in)), io.Discard, 128)
		if err != nil || got != tc.want {
			t.Fatalf("%q => %q %v", tc.in, got, err)
		}
	}
	r := bufio.NewReader(strings.NewReader(strings.Repeat("x", 10000) + "\nnext\n"))
	got, err := readInput(r, io.Discard, 8)
	if err != nil || len(got) != 9 {
		t.Fatalf("oversized input %q %v", got, err)
	}
	got, err = readInput(r, io.Discard, 8)
	if err != nil || got != "next" {
		t.Fatal("oversized line consumed next input")
	}
}
func TestPipeRejectsIncompleteAndInvalidBlocks(t *testing.T) {
	for _, in := range []string{"<<bad!\n", "<<EOF\nincomplete\n"} {
		if _, err := readInput(bufio.NewReader(strings.NewReader(in)), io.Discard, 128); err == nil {
			t.Fatalf("accepted %q", in)
		}
	}
}
