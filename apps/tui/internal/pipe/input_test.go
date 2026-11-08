package pipe

import (
	"bufio"
	"errors"
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
	if err == nil || got != "" {
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

func TestOverflowCannotBeHiddenByWhitespaceNormalization(t *testing.T) {
	for _, text := range []string{"hello    world", " aaaaaaaab", strings.Repeat(" ", 200) + "end"} {
		for _, block := range []bool{false, true} {
			wire := text + "\nnext\n"
			if block {
				wire = "<<END\n" + text + "\nEND\nnext\n"
			}
			r := bufio.NewReader(strings.NewReader(wire))
			got, err := readInput(r, io.Discard, 8)
			var overflow *overflowError
			if got != "" || !errors.As(err, &overflow) {
				t.Fatalf("block=%v input=%q: got %q err %v", block, text, got, err)
			}
			got, err = readInput(r, io.Discard, 8)
			if err != nil || got != "next" {
				t.Fatalf("boundary lost: %q %v", got, err)
			}
		}
	}
}
