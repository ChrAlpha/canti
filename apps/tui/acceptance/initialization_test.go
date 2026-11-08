//go:build acceptance

package acceptance

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTUIInputInitializesAnEmptyJournal(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "done"}})
	if err := os.Mkdir(f.state, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.state, "canti.db"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_, wait := f.startWith(strings.NewReader("g\n"), nil, tuiCommand)
	if r := wait(); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("TUI over an empty journal: %+v", r)
	}
}
