// Command canti drives an agent run in a workspace: every step asks a model
// for one decision and runs it in a container, and every fact is committed
// to a journal in the state directory, so any later command continues from
// the last committed state.
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
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/docker"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
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

var errLocked = errors.New("locked by another canti process")

var wrapJournal = func(j harness.Journal) harness.Journal { return j }

const usage = `canti drives an agent run in a workspace until it returns or stops.

Usage:
  canti run -goal TEXT [-constraints TEXT] [flags]  start a run and drive it until it halts
  canti resume [-run ID] [flags]                    continue the latest or the named run
  canti stop [-run ID] [-cleanup 30s]               stop a run, cancelling and recording its job
  canti resolve [-run ID] (-job | -execution STATE [-exit-code N] [-note TEXT])
                                                    record the outcome of a run's unknown effect
  canti status [-run ID]                            print the state of a run as JSON
  canti history [-run ID]                           print the transitions of a run as JSON Lines
  canti version                                     print the version

Run "canti COMMAND -h" for the flags of a command. run and resume read the
model credential from ANTHROPIC_API_KEY or else ANTHROPIC_AUTH_TOKEN.

Exit status: 0 returned or succeeded, 1 error, 2 usage, 3 stopped,
4 needs resolution, 5 still active after -once, 6 workspace locked.
`

func main() { os.Exit(cli(context.Background(), os.Args[1:], os.Stdout, os.Stderr)) }

type action func(context.Context) (int, error)

var commands = map[string]func(*flag.FlagSet, *app) action{
	"run": runCommand, "resume": resumeCommand, "stop": stopCommand,
	"resolve": resolveCommand, "status": statusCommand, "history": historyCommand,
}

type app struct {
	stdout, stderr   io.Writer
	log              *slog.Logger
	workspace, state string
	runID            string
}

func cli(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return exitUsage
	}
	switch args[0] {
	case "version", "-version", "--version":
		fmt.Fprintln(stdout, version)
		return exitOK
	case "help", "-h", "-help", "--help":
		fmt.Fprint(stdout, usage)
		return exitOK
	}
	name := args[0]
	build, ok := commands[name]
	if !ok {
		fmt.Fprintf(stderr, "canti: unknown command %q\n\n%s", name, usage)
		return exitUsage
	}
	a := &app{stdout: stdout, stderr: stderr}
	fs := flag.NewFlagSet("canti "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {}
	workspace := fs.String("workspace", ".", "workspace `directory` of the run")
	state := fs.String("state", "", "state `directory` (default $XDG_STATE_HOME/canti/<hash of the workspace path>)")
	level := fs.String("log-level", "info", "log `level`: debug, info, warn, or error")
	act := build(fs, a)
	help := func(w io.Writer) {
		fmt.Fprintf(w, "Usage: canti %s [flags]\n\nFlags:\n", name)
		fs.SetOutput(w)
		fs.PrintDefaults()
	}
	switch err := fs.Parse(args[1:]); {
	case errors.Is(err, flag.ErrHelp):
		help(stdout)
		return exitOK
	case err != nil:
		help(stderr)
		return exitUsage
	case fs.NArg() > 0:
		fmt.Fprintf(stderr, "canti %s: unexpected argument %q\n", name, fs.Arg(0))
		help(stderr)
		return exitUsage
	}
	var lv slog.Level
	code, err := exitUsage, lv.UnmarshalText([]byte(*level))
	if err == nil {
		a.log = slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: lv}))
		code, err = exitError, a.locate(*workspace, *state)
	}
	if err == nil {
		code, err = act(ctx)
	}
	if err != nil {
		fmt.Fprintf(stderr, "canti %s: %v\n", name, err)
		switch {
		case code == exitUsage:
			help(stderr)
		case errors.Is(err, errLocked):
			code = exitLocked
		}
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

func runFlag(fs *flag.FlagSet, a *app) {
	fs.StringVar(&a.runID, "run", "", "run `ID` (default the latest run)")
}

func modelURL(fs *flag.FlagSet) *string {
	return fs.String("model-url", cmp.Or(os.Getenv("ANTHROPIC_BASE_URL"), "https://api.anthropic.com"),
		"Anthropic API base `URL` (default $ANTHROPIC_BASE_URL); not stored with the run")
}

func runCommand(fs *flag.FlagSet, a *app) action {
	goal := fs.String("goal", "", "the task of the run; required")
	constraints := fs.String("constraints", "", "rules the agent must follow")
	once := fs.Bool("once", false, "perform exactly one step after creating the run")
	url := modelURL(fs)
	var s spec
	s.register(fs)
	return func(ctx context.Context) (int, error) {
		if strings.TrimSpace(*goal) == "" {
			return exitUsage, errors.New("-goal is required")
		}
		s.config.Workspace = a.workspace
		run, initial, err := newRun(*goal, *constraints, s, time.Now())
		if err != nil {
			return exitUsage, err
		}
		m, err := newModel(s.config, *url)
		if err != nil {
			return exitError, err
		}
		if err := os.MkdirAll(a.state, 0o700); err != nil {
			return exitError, err
		}
		x, err := a.executor(s.config)
		if err != nil {
			return exitError, err
		}
		store, done, err := a.open(ctx, true, true)
		if err != nil {
			return exitError, err
		}
		defer done()
		if err := a.closed(ctx, store); err != nil {
			return exitError, err
		}
		l, err := a.loop(store, x, m, s.config, run.Budget)
		if err == nil {
			err = store.CreateRun(ctx, run, initial)
		}
		if err != nil {
			return exitError, err
		}
		a.log.Info("created run", "run_id", run.ID, "workspace", a.workspace, "state", a.state)
		snap, err := a.drive(ctx, l, run.ID, *once)
		return a.report(snap, err, exitActive)
	}
}

func resumeCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	once := fs.Bool("once", false, "perform exactly one step")
	url := modelURL(fs)
	return func(ctx context.Context) (int, error) {
		if _, err := credentials(); err != nil {
			return exitError, err
		}
		model := func(c config) (harness.Model, error) { return newModel(c, *url) }
		return a.with(ctx, model, exitActive, func(l *harness.Loop, id string) (harness.Snapshot, error) {
			return a.drive(ctx, l, id, *once)
		})
	}
}

func stopCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	cleanup := fs.Duration("cleanup", stopCleanup, "`deadline` for cancelling the job and recording its result")
	return func(ctx context.Context) (int, error) {
		if *cleanup <= 0 {
			return exitUsage, errors.New("-cleanup must be positive")
		}
		ctx, cancel := context.WithTimeout(ctx, *cleanup)
		defer cancel()
		return a.with(ctx, withoutModel, exitActive, func(l *harness.Loop, id string) (harness.Snapshot, error) {
			return l.Stop(ctx, id, core.StopCancelled)
		})
	}
}

var resolvable = []core.ExecutionState{core.ExecutionExited, core.ExecutionFailedToStart, core.ExecutionCancelled, core.ExecutionTimedOut, core.ExecutionUnknown}

func resolveCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	job := fs.Bool("job", false, "record the job the executor finds for the unknown effect")
	execution := fs.String("execution", "", "record the outcome `state`: exited, failed_to_start, cancelled, timed_out, or unknown")
	exitCode := fs.Int("exit-code", 0, "exit `code` of an exited effect; required for exited only")
	note := fs.String("note", "", "operator `text` recorded as the output preview")
	return func(ctx context.Context) (int, error) {
		set := map[string]bool{}
		fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
		obs := harness.Observation{Execution: core.ExecutionState(*execution), Preview: *note, Output: harness.ArtifactRef{Detail: "resolved by operator"}}
		switch {
		case *job == set["execution"]:
			return exitUsage, errors.New("give exactly one of -job and -execution")
		case *job && (set["exit-code"] || set["note"]):
			return exitUsage, errors.New("-job takes no -exit-code or -note")
		case !*job && !slices.Contains(resolvable, obs.Execution):
			return exitUsage, fmt.Errorf("-execution must be one of %v", resolvable)
		case !*job && set["exit-code"] != (obs.Execution == core.ExecutionExited):
			return exitUsage, errors.New("-exit-code is required for exited and allowed for no other state")
		}
		if set["exit-code"] {
			obs.ExitCode = exitCode
		}
		return a.with(ctx, withoutModel, exitOK, func(l *harness.Loop, id string) (harness.Snapshot, error) {
			if *job {
				return l.ResolveJob(ctx, id)
			}
			return l.Resolve(ctx, id, obs)
		})
	}
}

func (a *app) open(ctx context.Context, lock, create bool) (*sqlite.Store, func(), error) {
	path := filepath.Join(a.state, "canti.db")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) && !create {
		return nil, nil, fmt.Errorf("no runs in workspace %s", a.workspace)
	}
	release := func() {}
	if lock {
		f, err := os.OpenFile(filepath.Join(a.state, "lock"), os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			return nil, nil, err
		}
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			f.Close()
			if errors.Is(err, syscall.EWOULDBLOCK) {
				err = fmt.Errorf("workspace %s is %w", a.workspace, errLocked)
			}
			return nil, nil, err
		}
		release = func() { f.Close() }
	}
	store, err := sqlite.Open(ctx, path)
	if err != nil {
		release()
		return nil, nil, err
	}
	return store, func() { store.Close(); release() }, nil
}

func (a *app) find(ctx context.Context, store *sqlite.Store) (harness.Run, error) {
	if a.runID != "" {
		run, err := store.Run(ctx, a.runID)
		if errors.Is(err, harness.ErrNotFound) {
			err = fmt.Errorf("run %q not found in workspace %s", a.runID, a.workspace)
		}
		return run, err
	}
	run, err := store.LastRun(ctx)
	if errors.Is(err, harness.ErrNotFound) {
		err = fmt.Errorf("no runs in workspace %s", a.workspace)
	}
	return run, err
}

func (a *app) closed(ctx context.Context, store *sqlite.Store) error {
	last, err := store.LastRun(ctx)
	var snap harness.Snapshot
	if err == nil {
		snap, err = store.Latest(ctx, last.ID)
	}
	switch p := snap.State.Phase; {
	case errors.Is(err, harness.ErrNotFound):
		return nil
	case err == nil && p != core.PhaseReturned && p != core.PhaseStopped:
		return fmt.Errorf("run %s in workspace %s is %s: resume or stop it first", last.ID, a.workspace, p)
	}
	return err
}

func (a *app) executor(c config) (*docker.Executor, error) {
	x, err := docker.New(executorConfig(c, a.state))
	if err != nil {
		return nil, fmt.Errorf("executor for workspace %s and state %s: %w", c.Workspace, a.state, err)
	}
	return x, nil
}

func (a *app) loop(j harness.Journal, x harness.Executor, m harness.Model, c config, b harness.Budget) (*harness.Loop, error) {
	return harness.NewLoop(wrapJournal(j), x, m, harness.LoopConfig{
		Environment: environment(c, b), ArtifactDir: docker.ArtifactDir, Workspace: c.Workspace,
		NotesFile: ".canti/notes.md", NotesPath: docker.WorkDir + "/.canti/notes.md",
		History: c.History, Logger: a.log,
	})
}

// with locks the workspace, rebuilds the loop of the selected run from its
// stored config and the model that model builds from it, and reports what op
// does with the loop.
func (a *app) with(ctx context.Context, model func(config) (harness.Model, error), active int, op func(*harness.Loop, string) (harness.Snapshot, error)) (int, error) {
	store, done, err := a.open(ctx, true, false)
	if err != nil {
		return exitError, err
	}
	defer done()
	run, err := a.find(ctx, store)
	var l *harness.Loop
	if err == nil {
		l, err = a.rebuild(store, run, model)
	}
	if err != nil {
		return exitError, err
	}
	snap, err := op(l, run.ID)
	return a.report(snap, err, active)
}

func (a *app) rebuild(store *sqlite.Store, run harness.Run, model func(config) (harness.Model, error)) (*harness.Loop, error) {
	c, err := decodeConfig(run)
	if err == nil && c.Workspace != a.workspace {
		err = fmt.Errorf("run %s belongs to workspace %s", run.ID, c.Workspace)
	}
	if err != nil {
		return nil, err
	}
	m, err := model(c)
	if err != nil {
		return nil, err
	}
	x, err := a.executor(c)
	if err != nil {
		return nil, err
	}
	return a.loop(store, x, m, c, run.Budget)
}

// drive runs the loop, or one step of it with once. The first SIGINT or
// SIGTERM shuts the controller down and then stops the run persistently
// within the cleanup deadline, which a second signal cuts short.
func (a *app) drive(ctx context.Context, l *harness.Loop, id string, once bool) (harness.Snapshot, error) {
	loopCtx, release := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer release()
	step := l.Run
	if once {
		step = l.Step
	}
	snap, err := step(loopCtx, id)
	if loopCtx.Err() == nil || ctx.Err() != nil {
		return snap, err
	}
	a.log.Warn("stopping the run after a signal", "run_id", id)
	timeout, cancel := context.WithTimeout(ctx, stopCleanup)
	defer cancel()
	cleanup, release := signal.NotifyContext(timeout, os.Interrupt, syscall.SIGTERM)
	defer release()
	return l.Stop(cleanup, id, core.StopCancelled)
}
