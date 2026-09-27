//go:build integration

package main

import (
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

func TestChatShowsCommandsAndTheirOutput(t *testing.T) {
	f, image := dockerFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: `printf 'hello\033[2J\nworld\n'; seq 20; exit 3`},
		2: {Kind: core.Return, Payload: "done"},
	})
	_, wait := f.startWith(strings.NewReader("go\n"), nil, "chat", "-image", image)
	r := wait()
	if r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("chat: %+v", r)
	}
	want := "$ printf 'hello\\033[2J\\nworld\\n'; seq 20; exit 3\n  … 14 more lines\n  13\n  14\n  15\n  16\n  17\n  18\n  19\n  20\n  exit 3\n"
	if !strings.Contains(r.stderr, want) || strings.Contains(r.stderr, "\x1b") {
		t.Fatalf("stderr:\n%s", r.stderr)
	}
}
