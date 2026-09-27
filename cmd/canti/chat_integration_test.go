//go:build integration

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

func TestChatShowsCommandsAndTheirOutput(t *testing.T) {
	f, image := dockerFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "printf 'hello\\033[2J\\nworld\\n'\nseq 20\n:\n:\n:\n:\n:\n:\nexit 3"},
		2: {Kind: core.Return, Payload: "done"},
	})
	_, wait := f.startWith(strings.NewReader("go\n"), nil, "chat", "-image", image)
	r := wait()
	if r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("chat: %+v", r)
	}
	want := "$ printf 'hello\\033[2J\\nworld\\n'\n  seq 20\n  :\n  :\n  :\n  :\n  :\n  :\n  … 1 more line\n│ …\n│ 13\n│ 14\n│ 15\n│ 16\n│ 17\n│ 18\n│ 19\n│ 20\n└ exit 3\n"
	if !strings.Contains(r.stderr, want) || strings.Contains(r.stderr, "\x1b") {
		t.Fatalf("stderr:\n%s", r.stderr)
	}
}

func TestChatShowsATimedOutJob(t *testing.T) {
	f, image := dockerFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "sleep 60"},
		2: {Kind: core.Return, Payload: "gave up"},
	})
	_, wait := f.startWith(strings.NewReader("wait\n"), nil, "chat", "-image", image, "-job-timeout", "2s")
	r := wait()
	if r.code != exitOK || r.stdout != "gave up\n" || !strings.Contains(r.stderr, "$ sleep 60\n└ timed_out") || strings.Contains(r.stderr, "exit 1") {
		t.Fatalf("chat: %+v", r)
	}
}

func TestChatShowsAllOfACommandWithCarriageReturns(t *testing.T) {
	f, image := dockerFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "touch hidden # \r echo shown"},
		2: {Kind: core.Return, Payload: "done"},
	})
	_, wait := f.startWith(strings.NewReader("go\n"), nil, "chat", "-image", image)
	r := wait()
	if r.code != exitOK || !strings.Contains(r.stderr, "$ \"touch hidden # \\r echo shown\"\n└ exit 0\n") {
		t.Fatalf("chat: %+v", r)
	}
	if _, err := os.Stat(filepath.Join(f.workspace, "hidden")); err != nil {
		t.Fatal(err)
	}
}
