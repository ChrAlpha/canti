// Command canti-tui presents terminal and pipe chat through the runtime protocol.
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/mattn/go-isatty"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

const version = "dev"

const (
	exitOK         = 0
	exitError      = 1
	exitUsage      = 2
	exitStopped    = 3
	exitResolution = 4
	exitActive     = 5
	exitLocked     = 6
)

const stopCleanup = 30 * time.Second

func main() { os.Exit(cli(context.Background(), os.Args[1:], os.Stdout, os.Stderr)) }

type action func(context.Context) (int, error)

type app struct {
	stdout, stderr              io.Writer
	display                     *stderrWriter
	log                         *slog.Logger
	workspace, state            string
	steps                       io.Writer
	signal                      os.Signal
	stopSignals                 chan os.Signal
	displayFailure              <-chan error
	cred                        hostrpc.Binding
	runtimePath                 string
	goalLimit, constraintsLimit int
	child                       *exec.Cmd
	childDone                   chan error
	hostMode                    string
	hostWait                    time.Duration
	detach                      chan struct{} // closed by a chat that ends attached to a host process
}

func cli(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		switch args[0] {
		case "version", "-version", "--version":
			fmt.Fprintln(stdout, version)
			return exitOK
		case "help":
			args = []string{"-h"}
		case "chat":
			args = args[1:]
		}
	}
	tui := isatty.IsTerminal(os.Stdin.Fd())
	var queue *stderrWriter
	setupNote := func(s string) { writeBounded(stderr, s) }
	if !tui {
		queue = newStderrWriter(stderr)
		defer queue.Close()
		setupNote = func(s string) { io.WriteString(finalStderr{queue}, s) }
	}
	displayFailure := make(chan error, 2)
	out, restoreOut, err := newChatOutput(stdout, displayFailure)
	if err != nil {
		setupNote(fmt.Sprintf("canti-tui: standard output: %v\n", err))
		return exitError
	}
	defer restoreOut()
	note := blockingNote("standard output", out)
	stdout = out
	if tui {
		diagnostic, restoreErr, err := newChatOutput(stderr, displayFailure)
		if err != nil {
			setupNote(fmt.Sprintf("canti-tui: standard error: %v\n", err))
			return exitError
		}
		defer restoreErr()
		stderr = diagnostic
		setupNote = func(s string) { io.WriteString(diagnostic, s) }
		note += blockingNote("standard error", diagnostic)
	}
	if note != "" {
		setupNote(note)
	}
	a := &app{stdout: stdout, stderr: stderr, displayFailure: displayFailure}
	defer a.closeHost()
	fs := flag.NewFlagSet("canti-tui", flag.ContinueOnError)
	var parseErr strings.Builder
	fs.SetOutput(&parseErr)
	fs.Usage = func() {}
	workspace := fs.String("workspace", ".", "workspace `directory` of the run")
	state := fs.String("state", "", "state `directory` (default $XDG_STATE_HOME/canti/<hash of the workspace path>)")
	level := fs.String("log-level", "info", "log `level`: debug, info, warn, or error")
	act := chatCommand(fs, a)
	help := func(w io.Writer) {
		fmt.Fprint(w, "Usage: canti-tui [flags]\n\nFlags:\n")
		fs.SetOutput(w)
		fs.PrintDefaults()
	}
	switch err := fs.Parse(args); {
	case errors.Is(err, flag.ErrHelp):
		help(stdout)
		return exitOK
	case err != nil || fs.NArg() > 0:
		if err == nil {
			fmt.Fprintf(&parseErr, "canti-tui: unexpected argument %q\n", fs.Arg(0))
		}
		var usage strings.Builder
		help(&usage)
		setupNote(parseErr.String())
		setupNote(usage.String())
		return exitUsage
	}
	final := stderr
	if queue != nil {
		stderr, a.stderr, a.display, final = queue, queue, queue, finalStderr{queue}
	}
	var lv slog.Level
	code, err := exitUsage, lv.UnmarshalText([]byte(*level))
	if err == nil {
		a.log = slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: lv}))
		code, err = exitError, a.locate(*workspace, *state)
	}
	if err == nil {
		a.cred = credentials()
		code, err = act(ctx)
	}
	if err != nil {
		var out strings.Builder
		fmt.Fprintf(&out, "canti-tui: %v\n", err)
		switch {
		case code == exitUsage:
			help(&out)
		case errors.Is(err, errLocked):
			code = exitLocked
		}
		io.WriteString(final, out.String())
		code = cmp.Or(code, exitError)
	}
	return code
}

func (a *app) locate(workspace, state string) error {
	ws, err := filepath.Abs(workspace)
	if err == nil {
		ws, err = filepath.EvalSymlinks(ws)
	}
	if err != nil {
		return fmt.Errorf("workspace: %w", err)
	}
	if state == "" {
		state, err = defaultState(ws)
	} else {
		state, err = filepath.Abs(state)
	}
	a.workspace, a.state = ws, state
	return err
}

func modelURL(fs *flag.FlagSet) *string {
	return fs.String("model-url", "", "model API base `URL`: default $ANTHROPIC_BASE_URL, else https://api.anthropic.com, for anthropic-messages; "+
		"$OPENAI_BASE_URL, else https://api.openai.com/v1, for openai-responses; not saved with the run")
}

// stopControl applies each stop signal of a drive to the next of its
// cancels. Chat sends its signals on a.stopSignals, and each one handled is
// kept on seen for release to record.
type stopControl struct {
	sig, seen chan os.Signal
	relay     bool
	cancels   []context.CancelFunc
}

func (a *app) stopControl() *stopControl {
	c := &stopControl{sig: make(chan os.Signal, 2), seen: make(chan os.Signal, 2)}
	if a.stopSignals == nil {
		signal.Notify(c.sig, os.Interrupt, syscall.SIGTERM)
		signal.Notify(c.seen, os.Interrupt, syscall.SIGTERM)
	} else {
		c.sig, c.relay = a.stopSignals, true
	}
	return c
}

func (c *stopControl) handle(s os.Signal, cancel context.CancelFunc) {
	if c.relay {
		c.seen <- s
	}
	cancel()
}

func (c *stopControl) pending() int {
	for n, cancel := range c.cancels {
		select {
		case s := <-c.sig:
			c.handle(s, cancel)
		default:
			return n
		}
	}
	return len(c.cancels)
}

func (c *stopControl) watch(done <-chan struct{}, start <-chan int) {
	var handled int
	select {
	case handled = <-start:
	case <-done:
		return
	}
	for _, cancel := range c.cancels[handled:] {
		select {
		case s := <-c.sig:
			c.handle(s, cancel)
		case <-done:
			return
		}
	}
	signal.Stop(c.sig)
	signal.Stop(c.seen)
}

func (a *app) record(signals chan os.Signal) {
	for len(signals) > 0 {
		if s := <-signals; a.signal != syscall.SIGTERM {
			a.signal = s
		}
	}
}

var errLocked = errors.New("workspace locked")
