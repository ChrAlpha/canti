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
	"unicode/utf8"

	"github.com/mattn/go-isatty"

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
  canti chat [-session ID] [flags]                  talk with the agent, one run per input
  canti resume [-run ID] [flags]                    continue the latest or the named run
  canti stop [-run ID] [-cleanup 30s]               stop a run, cancelling and recording its job;
                                                    an unknown outcome must be resolved first
  canti resolve [-run ID] (-job | -execution STATE [-exit-code N] [-note TEXT])
                                                    record the outcome of a run's unknown effect
  canti status [-run ID]                            print the state of a run as JSON
  canti history [-run ID]                           print the transitions of a run as JSON Lines
  canti context [-run ID] [-step N] [-revision N]   inspect a frozen request and its recorded context
  canti version                                     print the version

Run "canti COMMAND -h" for the flags of a command. run, chat, and resume read the
model credential from ANTHROPIC_API_KEY or else ANTHROPIC_AUTH_TOKEN.

Exit status: 0 returned or succeeded, 1 error, 2 usage, 3 stopped,
4 needs resolution, 5 still active after -once, 6 workspace locked.
`

func main() { os.Exit(cli(context.Background(), os.Args[1:], os.Stdout, os.Stderr)) }

type action func(context.Context) (int, error)

var commands = map[string]func(*flag.FlagSet, *app) action{
	"run": runCommand, "chat": chatCommand, "resume": resumeCommand, "stop": stopCommand,
	"resolve": resolveCommand, "status": statusCommand, "history": historyCommand,
	"context": contextCommand,
}

type app struct {
	stdout, stderr   io.Writer
	display          *stderrWriter
	log              *slog.Logger
	workspace, state string
	runID            string
	steps            io.Writer
	signal           os.Signal
	stopSignals      chan os.Signal
	displayFailure   <-chan error
	cred             credential
	credErr          error
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
	var displayFailure chan error
	if name == "chat" {
		displayFailure = make(chan error, 2)
		out, restoreOut, err := newChatOutput(stdout, displayFailure)
		if err != nil {
			fmt.Fprintf(stderr, "canti chat: standard output: %v\n", err)
			return exitError
		}
		defer restoreOut()
		stdout = out
		if isatty.IsTerminal(os.Stdin.Fd()) {
			diagnostic, restoreErr, err := newChatOutput(stderr, displayFailure)
			if err != nil {
				fmt.Fprintf(stderr, "canti chat: standard error: %v\n", err)
				return exitError
			}
			defer restoreErr()
			stderr = diagnostic
		}
	}
	a := &app{stdout: stdout, stderr: stderr, displayFailure: displayFailure}
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
	final := stderr
	if name != "chat" || !isatty.IsTerminal(os.Stdin.Fd()) {
		asyncStderr := newStderrWriter(stderr)
		defer asyncStderr.Close()
		stderr, a.stderr, a.display, final = asyncStderr, asyncStderr, asyncStderr, finalStderr{asyncStderr}
	}
	var lv slog.Level
	code, err := exitUsage, lv.UnmarshalText([]byte(*level))
	if err == nil {
		a.log = slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: lv}))
		code, err = exitError, a.locate(*workspace, *state)
	}
	if err == nil {
		if name != "context" {
			a.cred, a.credErr = credentials()
		}
		code, err = act(ctx)
	}
	if err != nil {
		var out strings.Builder
		fmt.Fprintf(&out, "canti %s: %v\n", name, err)
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

func runFlag(fs *flag.FlagSet, a *app) {
	fs.StringVar(&a.runID, "run", "", "run `ID` (default the latest run)")
}

func modelURL(fs *flag.FlagSet) *string {
	return fs.String("model-url", "", "Anthropic API base `URL` (default $ANTHROPIC_BASE_URL, else https://api.anthropic.com); not saved with the run")
}

func runCommand(fs *flag.FlagSet, a *app) action {
	goal := fs.String("goal", "", "the task of the run; required")
	constraints := fs.String("constraints", "", "rules the agent must follow")
	session := fs.String("session", "", "serial conversation ID; requires -input; reuse the same workspace and state")
	input := fs.String("input", "", "submission ID within the session; reuse only to retry this input")
	once := fs.Bool("once", false, "perform exactly one step after creating the run")
	url := modelURL(fs)
	var s spec
	s.register(fs)
	return func(ctx context.Context) (int, error) {
		if err := checkTask("-goal", *goal, *constraints); err != nil {
			return exitUsage, err
		}
		s.config.Workspace = a.workspace
		if *session != "" || *input != "" {
			setConstraints := false
			fs.Visit(func(f *flag.Flag) {
				if f.Name == "constraints" {
					setConstraints = true
				}
			})
			return a.submitSession(ctx, *session, *input, *goal, *constraints, setConstraints, s, *url, *once)
		}
		run, initial, err := newRun(*goal, *constraints, s, time.Now())
		if err != nil {
			return exitUsage, err
		}
		if a.credErr != nil {
			return exitError, a.credErr
		}
		drive, release := a.drive(ctx, *once)
		defer release()
		m, err := newModel(s.config, *url, a.cred)
		if err != nil {
			return exitError, err
		}
		if err := docker.CheckLayout(a.workspace, a.state); err != nil {
			return exitError, err
		}
		if err := os.MkdirAll(a.state, 0o700); err != nil {
			return exitError, err
		}
		x, err := newExecutor(s.config, a.state)
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
		snap, err := drive(l, run.ID)
		return a.report(snap, err, exitActive)
	}
}

func checkTask(label, goal, constraints string) error {
	limits, hint := harness.DefaultPromptLimits(), "; put a long specification in a workspace file and reference it from the goal"
	switch g := strings.TrimSpace(goal); {
	case g == "":
		return fmt.Errorf("%s is required", label)
	case !utf8.ValidString(goal) || !utf8.ValidString(constraints):
		return fmt.Errorf("%s and -constraints must be valid UTF-8", label)
	case len(g) > limits.Goal:
		return fmt.Errorf("%s must be at most %d bytes%s", label, limits.Goal, hint)
	case len(strings.TrimSpace(constraints)) > limits.Constraints:
		return fmt.Errorf("-constraints must be at most %d bytes%s", limits.Constraints, hint)
	}
	return nil
}

func resumeCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	once := fs.Bool("once", false, "perform exactly one step")
	url := modelURL(fs)
	return func(ctx context.Context) (int, error) {
		if a.credErr != nil {
			return exitError, a.credErr
		}
		drive, release := a.drive(ctx, *once)
		defer release()
		model := func(c config) (harness.Model, error) { return newModel(c, *url, a.cred) }
		return a.with(ctx, model, exitActive, drive)
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
			return stop(ctx, l, id)
		})
	}
}

var resolvable = []core.ExecutionState{core.ExecutionExited, core.ExecutionFailedToStart, core.ExecutionCancelled, core.ExecutionTimedOut}

func resolveCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	job := fs.Bool("job", false, "record the job the executor finds for the unknown effect")
	execution := fs.String("execution", "", "record a confirmed outcome `state`: exited, failed_to_start, cancelled, or timed_out")
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
		case !*job && obs.Execution == core.ExecutionUnknown:
			return exitUsage, errors.New("unknown is not a completed resolution; leave the run in needs_resolution until the outcome is confirmed, then use -job or a known -execution state")
		case !*job && !slices.Contains(resolvable, obs.Execution):
			return exitUsage, fmt.Errorf("-execution must be one of %v", resolvable)
		case !*job && set["exit-code"] != (obs.Execution == core.ExecutionExited):
			return exitUsage, errors.New("-exit-code is required for exited and allowed for no other state")
		}
		if set["exit-code"] {
			obs.ExitCode = exitCode
		}
		return a.with(ctx, withoutModel, exitOK, func(l *harness.Loop, id string) (harness.Snapshot, error) {
			if !*job {
				return l.Resolve(ctx, id, obs)
			}
			snap, err := l.ResolveJob(ctx, id)
			if errors.Is(err, harness.ErrJobUnknown) {
				err = fmt.Errorf("%w; recording the job cannot resolve effect %s: inspect it, as with \"docker logs canti-%[2]s\", "+
					"then record its outcome with \"canti resolve -run %s -execution STATE\"", err, snap.State.EffectID, id)
			}
			return snap, err
		})
	}
}

// open opens the workspace journal, holding the workspace lock until done
// when lock is set. Unless create is set, a missing journal is an error.
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

// find returns the selected run and its stored config.
func (a *app) find(ctx context.Context, store *sqlite.Store) (harness.Run, config, error) {
	var run harness.Run
	var err error
	if a.runID != "" {
		if run, err = store.Run(ctx, a.runID); errors.Is(err, harness.ErrNotFound) {
			err = fmt.Errorf("run %q not found in workspace %s", a.runID, a.workspace)
		}
	} else if run, err = store.LastRun(ctx); errors.Is(err, harness.ErrNotFound) {
		err = fmt.Errorf("no runs in workspace %s", a.workspace)
	}
	if err != nil {
		return run, config{}, err
	}
	c, err := decodeConfig(run, a.workspace)
	return run, c, err
}

// closed fails unless the latest run of the state directory, if any, is
// closed and its step records are published.
func (a *app) closed(ctx context.Context, store *sqlite.Store) error {
	last, err := store.LastRun(ctx)
	if errors.Is(err, harness.ErrNotFound) {
		return nil
	}
	var snap harness.Snapshot
	if err == nil {
		_, err = decodeConfig(last, a.workspace)
	}
	if err == nil {
		snap, err = store.Latest(ctx, last.ID)
	}
	switch p := snap.State.Phase; {
	case err != nil:
		return err
	case p == core.PhaseReturned || p == core.PhaseStopped:
		if err := store.CheckAdmission(ctx); err != nil {
			return err
		}
		if err := harness.PublishHistory(ctx, store, historyRoot(a.state), docker.ArtifactDir, last.ID); err != nil {
			return fmt.Errorf("publishing the step records of run %s: %w", last.ID, err)
		}
		return nil
	case p == core.PhaseNeedsResolution:
		return fmt.Errorf("run %s needs resolution: resolve it first", last.ID)
	}
	return fmt.Errorf("run %s is %s: resume or stop it first", last.ID, snap.State.Phase)
}

func (a *app) loop(j harness.Journal, x harness.Executor, m harness.Model, c config, b harness.Budget) (*harness.Loop, error) {
	j = wrapJournal(j)
	if a.steps != nil {
		j = rendering{j.(harness.SessionJournal), a.steps}
	}
	reader := &harness.Reader{History: historyRoot(a.state), Artifacts: filepath.Join(a.state, "artifacts"), Imports: c.HistoryImports}
	return harness.NewLoop(j, x, m, harness.LoopConfig{
		Environment: environment(c, b), ArtifactDir: docker.ArtifactDir,
		History: historyRoot(a.state), RunsPath: runsDir,
		Limits: promptLimits(c), Logger: a.log,
		Reader: reader, WindowSteps: c.WindowSteps, WindowBytes: c.WindowBytes, HandoverBytes: c.HandoverBytes,
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
	run, c, err := a.find(ctx, store)
	var m harness.Model
	var x *docker.Executor
	var l *harness.Loop
	if err == nil {
		m, err = model(c)
	}
	if err == nil {
		x, err = newExecutor(c, a.state)
	}
	if err == nil {
		l, err = a.loop(store, x, m, c, run.Budget)
	}
	if err != nil {
		return exitError, err
	}
	snap, err := op(l, run.ID)
	return a.report(snap, err, active)
}

// drive catches SIGINT and SIGTERM until release and returns op, which runs
// the loop, or one step of it with once. The first signal, also one caught
// before op, shuts the controller down and then stops the run persistently
// within the cleanup deadline; the second abandons the cleanup. release
// records in a.signal SIGTERM if it was caught, else the first signal.
func (a *app) drive(ctx context.Context, once bool) (op func(*harness.Loop, string) (harness.Snapshot, error), release func()) {
	sig, seen := make(chan os.Signal, 2), make(chan os.Signal, 2)
	if a.stopSignals == nil {
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		signal.Notify(seen, os.Interrupt, syscall.SIGTERM)
	} else {
		sig = a.stopSignals
	}
	loopCtx, shutdown := context.WithCancel(ctx)
	cleanupCtx, abandon := context.WithCancel(context.WithoutCancel(ctx))
	startWatch := make(chan int, 1)
	handled := make(chan struct{})
	go func() {
		defer close(handled)
		var handledBeforeOp int
		select {
		case handledBeforeOp = <-startWatch:
		case <-cleanupCtx.Done():
			return
		}
		for _, cancel := range []context.CancelFunc{shutdown, abandon}[handledBeforeOp:] {
			select {
			case s := <-sig:
				if a.stopSignals != nil {
					seen <- s
				}
				cancel()
			case <-cleanupCtx.Done():
				return
			}
		}
		signal.Stop(sig)
		signal.Stop(seen)
	}()
	op = func(l *harness.Loop, id string) (harness.Snapshot, error) {
		handledBeforeOp := 0
	preOp:
		for _, cancel := range []context.CancelFunc{shutdown, abandon} {
			select {
			case s := <-sig:
				if a.stopSignals != nil {
					seen <- s
				}
				cancel()
				handledBeforeOp++
			default:
				break preOp
			}
		}
		startWatch <- handledBeforeOp
		step := l.Run
		if once {
			step = l.Step
		}
		snap, err := step(loopCtx, id)
		if loopCtx.Err() == nil || ctx.Err() != nil {
			return snap, err
		}
		cleanup, cancel := context.WithTimeout(cleanupCtx, stopCleanup)
		defer cancel()
		return stop(cleanup, l, id)
	}
	return op, func() {
		abandon()
		shutdown()
		<-handled
		signal.Stop(sig)
		signal.Stop(seen)
		for len(sig) > 0 {
			if s := <-sig; a.signal != syscall.SIGTERM {
				a.signal = s
			}
		}
		for len(seen) > 0 {
			if s := <-seen; a.signal != syscall.SIGTERM {
				a.signal = s
			}
		}
	}
}

func stop(ctx context.Context, l *harness.Loop, id string) (harness.Snapshot, error) {
	snap, err := l.Stop(ctx, id, core.StopCancelled)
	if err != nil {
		err = fmt.Errorf("stopping run %s: %w; finish with \"canti stop -run %[1]s\", with a longer -cleanup if the job wrote a large output", id, err)
	}
	return snap, err
}
