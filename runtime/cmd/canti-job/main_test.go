package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestProjectReadFailsWhenItsOutputCannotBeWrittenBeforeTheDeadline(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 64; i++ {
		dir := filepath.Join(root, ".agents", "skills", strings.Repeat("s", 20)+string(rune('a'+i%26))+string(rune('a'+i/26)))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		body := "---\nname: skill-" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + "\ndescription: " + strings.Repeat("d", 1024) + "\n---\n"
		if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	binary := filepath.Join(t.TempDir(), "canti-job")
	build := exec.Command("go", "build", "-o", binary, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build helper: %v %s", err, out)
	}
	cmd := exec.Command(binary, "-project-read", "-root", root, "-workdir", root, "-deadline", "20ms")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = stdout // deliberately not consumed
	if err := cmd.Run(); err == nil {
		t.Fatal("project read succeeded after its output deadline")
	}
}

func TestProjectReadSucceedsWithTheWatchdogEnabled(t *testing.T) {
	if code := run([]string{"-project-read", "-root", t.TempDir()}, true); code != 0 {
		t.Fatalf("project read exit = %d", code)
	}
}

func TestProjectWatchdogStopsWithoutExitingAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan int, 1)
	stop := watchProjectDeadline(ctx, true, func(code int) { exited <- code })
	cancel()
	stop()
	select {
	case code := <-exited:
		t.Fatalf("watchdog exited with %d after cancellation", code)
	default:
	}
}

func TestProjectReadRejectsAPositionalArgument(t *testing.T) {
	if code := run([]string{"-project-read", "-root", t.TempDir(), "stray"}, false); code != 125 {
		t.Fatalf("project read exit = %d", code)
	}
}
