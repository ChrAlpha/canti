// Command canti-tui composes native terminal or pipe chat with the runtime SDK.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ChrAlpha/canti/apps/tui/internal/chat"
	"github.com/ChrAlpha/canti/apps/tui/internal/pipe"
	"github.com/ChrAlpha/canti/apps/tui/internal/terminal"
	"github.com/ChrAlpha/canti/apps/tui/internal/ui"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
	"github.com/mattn/go-isatty"
)

const version = "dev"

func main() { os.Exit(cli(context.Background(), os.Args[1:], os.Stdout, os.Stderr)) }

type options struct {
	cfg                          chat.Config
	workspace, state, url, level string
}

func parse(args []string, out io.Writer) (options, int, error) {
	var o options
	fs := flag.NewFlagSet("canti-tui", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.StringVar(&o.workspace, "workspace", ".", "workspace `directory` of the run")
	fs.StringVar(&o.state, "state", "", "state `directory` (default $XDG_STATE_HOME/canti/<workspace hash>)")
	fs.StringVar(&o.level, "log-level", "warn", "log `level`: debug, info, warn, or error")
	fs.StringVar(&o.url, "model-url", "", "model API base `URL` (default the API's environment or public endpoint)")
	fs.BoolVar(&o.cfg.Fresh, "fresh", false, "open a new line: the first input starts a new line, with no link to the previous run and no inherited constraints")
	fs.StringVar(&o.cfg.Constraints, "constraints", "", "rules the agent must follow")
	fs.StringVar(&o.cfg.RuntimePath, "runtime", "", "canti runtime executable (default canti on PATH)")
	fs.StringVar(&o.cfg.HostMode, "host", chat.Auto, "runtime lifecycle: auto keeps the host running after detach; owned owns a separate runtime process for each input")
	fs.DurationVar(&o.cfg.HostWait, "host-wait", 10*time.Second, "`time` to wait for a host process to start serving")
	registerSpec(fs, &o.cfg.Spec)
	fs.Usage = func() { fmt.Fprint(out, "Usage: canti-tui [flags]\n\nFlags:\n"); fs.PrintDefaults() }
	err := fs.Parse(args)
	if errors.Is(err, flag.ErrHelp) {
		return o, chat.OK, err
	}
	if err == nil && fs.NArg() > 0 {
		err = fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if err != nil {
		return o, chat.Usage, err
	}
	if err := executionFlags(fs, &o.cfg.Spec); err != nil {
		return o, chat.Usage, err
	}
	var lv slog.Level
	if err = lv.UnmarshalText([]byte(o.level)); err != nil {
		return o, chat.Usage, err
	}
	o.cfg.LogLevel = lv
	fs.Visit(func(f *flag.Flag) { o.cfg.ExplicitConstraints = o.cfg.ExplicitConstraints || f.Name == "constraints" })
	return o, chat.OK, nil
}
func (o *options) locate() error {
	ws, err := filepath.Abs(o.workspace)
	if err == nil {
		ws, err = filepath.EvalSymlinks(ws)
	}
	if err != nil {
		return fmt.Errorf("workspace: %w", err)
	}
	state := o.state
	if state == "" {
		state, err = defaultState(ws)
	} else {
		state, err = filepath.Abs(state)
	}
	if err != nil {
		return err
	}
	o.cfg.Workspace, o.cfg.State = ws, state
	o.cfg.Spec.Workspace = ws
	if o.cfg.Spec.Environment != "" {
		o.cfg.Spec.Environment, err = filepath.Abs(o.cfg.Spec.Environment)
		if err != nil {
			return err
		}
		if canonical, e := filepath.EvalSymlinks(o.cfg.Spec.Environment); e == nil {
			o.cfg.Spec.Environment = canonical
		}
	}
	if o.cfg.Spec.HistoryImports != "" {
		o.cfg.Spec.HistoryImports, err = filepath.Abs(o.cfg.Spec.HistoryImports)
	}
	return err
}
func cli(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		switch args[0] {
		case "version", "-version", "--version":
			fmt.Fprintln(stdout, version)
			return chat.OK
		case "help":
			args = []string{"-h"}
		}
	}
	failures := make(chan error, 2)
	diagnostic, closeErr, err := terminal.NewOutput(stderr, failures)
	if err != nil {
		if fallback, closeFallback, e := terminal.NewOutput(stdout, failures); e == nil {
			fmt.Fprintf(fallback, "canti-tui: standard error: %v\n", err)
			closeFallback()
		}
		return chat.Error
	}
	defer closeErr()
	out, closeOut, err := terminal.NewOutput(stdout, failures)
	if err != nil {
		fmt.Fprintf(diagnostic, "canti-tui: standard output: %v\n", err)
		return chat.Error
	}
	defer closeOut()
	var usage strings.Builder
	o, code, err := parse(args, &usage)
	if errors.Is(err, flag.ErrHelp) {
		io.WriteString(out, usage.String())
		return chat.OK
	}
	if err == nil {
		err = o.locate()
	}
	if err != nil {
		fmt.Fprintf(diagnostic, "canti-tui: %v\n%s", err, usage.String())
		return max(code, chat.Usage)
	}
	o.cfg.Binding = binding(credentials(), o.cfg.Spec.API, o.url)
	o.cfg.Environment = runtimeEnvironment()
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(signals)
	c := chat.New(o.cfg)
	if code, err = setup(ctx, c, signals); err != nil {
		fmt.Fprintf(diagnostic, "canti-tui: %v\n", err)
		return code
	}
	if isatty.IsTerminal(os.Stdin.Fd()) {
		code, err = ui.Run(ctx, c, os.Stdin, diagnostic, signals, failures)
	} else {
		code, err = pipe.Run(ctx, c, os.Stdin, out, diagnostic, signals, failures)
	}
	if err != nil {
		fmt.Fprintf(diagnostic, "canti-tui: %v\n", err)
		if code == chat.Usage {
			var help strings.Builder
			_, _, _ = parse([]string{"-h"}, &help)
			if _, writeErr := io.WriteString(diagnostic, help.String()); writeErr != nil {
				return chat.Error
			}
		}
		if code == chat.OK {
			code = errorCode(err)
		}
	}
	return code
}
func errorCode(err error) int {
	if errors.Is(err, chat.ErrLocked) {
		return chat.Locked
	}
	if errors.As(err, new(hostrpc.SpecError)) {
		return chat.Usage
	}
	return chat.Error
}
