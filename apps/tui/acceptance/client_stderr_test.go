//go:build acceptance

package acceptance

import (
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestTUIRefusesUnboundedOutputBeforeStateWithBlockedStderr(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can reopen regardless of mode")
	}
	f := newFixture(t, nil)
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer outR.Close()
	defer outW.Close()
	if err = unix.Fchmod(int(outW.Fd()), 0); err != nil {
		t.Fatal(err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer errR.Close()
	defer errW.Close()
	fillPipe(t, errW)
	cmd := cliCommand(t, []string{"ANTHROPIC_API_KEY=" + f.key}, tuiCommand, "-workspace", f.workspace, "-state", f.state)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = strings.NewReader("never\n"), outW, errW
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		cmd.Process.Kill()
		<-done
		t.Fatal("setup error blocked on stderr")
	}
	if cmd.ProcessState.ExitCode() != exitError {
		t.Fatal(cmd.ProcessState)
	}
	if _, err = os.Stat(f.state); !os.IsNotExist(err) {
		t.Fatalf("setup created state: %v", err)
	}
}
