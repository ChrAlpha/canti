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

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	"github.com/ChrAlpha/canti/runtime/internal/host"
	hostserver "github.com/ChrAlpha/canti/runtime/internal/hostrpc"
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

// wrapJournal and checkpoint are the host options that tests set to fail,
// kill, or pause a process at a commit or a named point.
var (
	wrapJournal = func(j harness.Journal) harness.Journal { return j }
	checkpoint  = func(string) {}
)

const usage = `canti drives an agent run in a workspace until it returns or stops.

Usage:
  canti run -goal TEXT [-constraints TEXT] [flags]  start a run and drive it until it halts
  canti-tui [-session ID] [flags]                   chat with the independent client
  canti resume [-run ID] [flags]                    continue the latest or the named run
  canti stop [-run ID] [-cleanup 30s]               stop a run, cancelling and recording its job;
                                                    an unknown outcome must be resolved first
  canti resolve [-run ID] (-job | -execution STATE -note TEXT [-exit-code N] [-owner ID])
    exited requires -exit-code and -owner; timed_out requires -owner.
                                                    record the outcome of a run's unknown effect
  canti inspect [-run ID] [-host MODE]
  canti status [-run ID]                            print the state of a run as JSON
  canti history [-run ID]                           print the transitions of a run as JSON Lines
  canti context [-run ID] [-step N] [-revision N]   inspect a frozen request and its recorded context
  canti environment init|attach|inspect|stop|start|detach|retire -environment DIRECTORY
                                                    manage a prepared development environment
  canti version                                     print the version

Run "canti COMMAND -h" for the flags of a command. A run names its model API
(-api anthropic-messages or openai-responses) and model (-model); canti has no
default for either. run and resume read the credential of the run's API:
ANTHROPIC_API_KEY or else ANTHROPIC_AUTH_TOKEN for anthropic-messages,
OPENAI_API_KEY for openai-responses. No process canti starts inherits any of
them.

Exit status: 0 returned or succeeded, 1 error, 2 usage, 3 stopped,
4 needs resolution, 5 still active after -once, 6 workspace locked.
`

func main() { os.Exit(cli(context.Background(), os.Args[1:], os.Stdout, os.Stderr)) }

type action func(context.Context) (int, error)

var commands = map[string]func(*flag.FlagSet, *app) action{
	"run": runCommand, "resume": resumeCommand, "stop": stopCommand,
	"resolve": resolveCommand, "status": statusCommand, "inspect": inspectCommand, "history": historyCommand,
	"context": contextCommand, "host": hostCommand,
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
	cred             host.Keys
	credErr          error
	bound            map[host.Pair]host.Keys // the credentials a foreground host binds
	hostMode         string
	hostWait         time.Duration
	starting         <-chan struct{} // closes when this client's launched host exits
	detach           chan struct{}   // closed by a chat that ends attached to a host process
}

func cli(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "environment" {
		return environmentCLI(ctx, args[1:], stdout, stderr)
	}
	if len(args) > 0 && args[0] == "preflight" {
		if len(args) != 1 {
			fmt.Fprintln(stderr, "canti preflight takes one JSON request on stdin and no flags")
			return exitUsage
		}
		return preflight(ctx, os.Stdin, stdout)
	}
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
	if name == "chat" {
		fmt.Fprintln(stderr, "canti chat moved to the independent client; run canti-tui with the same chat flags and -runtime PATH if canti is not on PATH")
		return exitUsage
	}
	build, ok := commands[name]
	if !ok {
		fmt.Fprintf(stderr, "canti: unknown command %q\n\n%s", name, usage)
		return exitUsage
	}
	queue := newStderrWriter(stderr)
	defer queue.Close()
	setupNote := func(s string) { io.WriteString(finalStderr{queue}, s) }
	a := &app{stdout: stdout, stderr: stderr}
	fs := flag.NewFlagSet("canti "+name, flag.ContinueOnError)
	var parseErr strings.Builder
	fs.SetOutput(&parseErr)
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
	case err != nil || fs.NArg() > 0:
		if err == nil {
			fmt.Fprintf(&parseErr, "canti %s: unexpected argument %q\n", name, fs.Arg(0))
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
		if name != "context" {
			a.cred, a.credErr = credentials()
		}
		if name == "host" {
			a.credErr = nil
		}
		code, err = act(ctx)
	}
	if err != nil {
		var out strings.Builder
		fmt.Fprintf(&out, "canti %s: %v\n", name, err)
		switch {
		case code == exitUsage:
			help(&out)
		case errors.Is(err, host.ErrLocked):
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
	if err != nil {
		return err
	}
	state, err = host.CanonicalState(ws, state)
	if err != nil {
		return err
	}
	a.workspace, a.state = ws, state
	return nil
}

func runFlag(fs *flag.FlagSet, a *app) {
	fs.StringVar(&a.runID, "run", "", "run `ID` (default the latest run)")
}

func modelURL(fs *flag.FlagSet) *string {
	return fs.String("model-url", "", "model API base `URL`: default $ANTHROPIC_BASE_URL, else https://api.anthropic.com, for anthropic-messages; "+
		"$OPENAI_BASE_URL, else https://api.openai.com/v1, for openai-responses; not saved with the run")
}

func runCommand(fs *flag.FlagSet, a *app) action {
	goal := fs.String("goal", "", "the task of the run; required")
	constraints := fs.String("constraints", "", "rules the agent must follow")
	session := fs.String("session", "", "serial conversation ID; requires -input; reuse the same workspace and state")
	input := fs.String("input", "", "submission ID, within the session if any; a retry with the same ID names the same run")
	once := fs.Bool("once", false, "perform exactly one step after creating the run")
	url := modelURL(fs)
	var s host.Spec
	registerSpec(fs, &s)
	hostFlags(fs, a)
	return func(ctx context.Context) (int, error) {
		if err := a.prepareRunConfig(fs, &s.Config, *goal, *constraints, *input != ""); err != nil {
			return exitUsage, err
		}
		if *session != "" || *input != "" {
			setConstraints := false
			fs.Visit(func(f *flag.Flag) {
				if f.Name == "constraints" {
					setConstraints = true
				}
			})
			return a.submitSession(ctx, *session, *input, *goal, *constraints, setConstraints, s, *url, *once)
		}
		if _, _, err := host.NewRun(*goal, *constraints, s, time.Now()); err != nil {
			return exitUsage, err
		}
		if err := host.Validate(ctx, a.state, host.Submission{Goal: *goal, Constraints: *constraints, Spec: s}); err != nil {
			return exitUsage, err
		}
		if err := a.checkCredential(ctx, s.Config, *url); err != nil {
			return exitError, err
		}
		drive, release := a.drive(ctx, *once)
		defer release()
		if err := docker.CheckLayout(a.workspace, a.state); err != nil {
			return exitError, err
		}
		if a.remoteMode() {
			return a.remoteSubmit(ctx, hostrpc.Submission{Goal: *goal, Constraints: *constraints, ExplicitConstraints: true, Spec: hostserver.SpecOf(s)},
				a.binding(s.Config.API, *url), *once)
		}
		if err := os.MkdirAll(a.state, 0o700); err != nil {
			return exitError, err
		}
		h, err := a.openHost(ctx, true)
		if err != nil {
			return exitError, err
		}
		defer h.Close()
		b := a.binding(s.Config.API, *url)
		ack, err := h.Submit(ctx, host.Submission{Goal: *goal, Constraints: *constraints, Spec: s, Binding: b, Now: time.Now()})
		if err != nil {
			return submitCode(err), err
		}
		a.log.Info("created run", "run_id", ack.Run.ID, "workspace", a.workspace, "state", a.state)
		snap, err := drive(h, ack.Run.ID, b)
		h.Close()
		return a.report(snap, err, exitActive, ack.Config)
	}
}

// submitCode is the exit status of a submission that err refused: usage for
// a task or configuration no run can be created from.
func submitCode(err error) int {
	if errors.As(err, new(host.SpecError)) || errors.As(err, new(hostrpc.SpecError)) {
		return exitUsage
	}
	return exitError
}

func checkTask(label, goal, constraints string) error {
	limits, hint := harness.DefaultPromptLimits(), "; put a long specification in a workspace file and reference it from the goal"
	switch g := strings.TrimSpace(goal); {
	case g == "":
		return fmt.Errorf("%s is required", label)
	case !utf8.ValidString(goal) || !utf8.ValidString(constraints):
		return fmt.Errorf("%s and -constraints must be valid UTF-8", label)
	case len(goal) > limits.Goal:
		return fmt.Errorf("%s must be at most %d bytes%s", label, limits.Goal, hint)
	case len(constraints) > limits.Constraints:
		return fmt.Errorf("-constraints must be at most %d bytes%s", limits.Constraints, hint)
	}
	return nil
}

func resumeCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	once := fs.Bool("once", false, "perform exactly one step")
	url := modelURL(fs)
	hostFlags(fs, a)
	return func(ctx context.Context) (int, error) {
		if err := a.checkHostMode(); err != nil {
			return exitUsage, err
		}
		if a.credErr != nil && !a.remoteMode() {
			return exitError, a.credErr
		}
		if a.remoteMode() {
			return a.remoteRun(ctx, func(c *hostrpc.Client, r hostrpc.Run) (int, error) {
				return a.remoteResume(ctx, c, r, *url, *once)
			})
		}
		drive, release := a.drive(ctx, *once)
		defer release()
		return a.with(ctx, exitActive, func(h *host.Host, run harness.Run, c host.RunConfig) (harness.Snapshot, error) {
			return drive(h, run.ID, a.binding(c.API, *url))
		})
	}
}

func stopCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	cleanup := fs.Duration("cleanup", stopCleanup, "`deadline` for cancelling the job and recording its result")
	hostFlags(fs, a)
	return func(ctx context.Context) (int, error) {
		if *cleanup <= 0 {
			return exitUsage, errors.New("-cleanup must be positive")
		}
		if err := a.checkHostMode(); err != nil {
			return exitUsage, err
		}
		if a.remoteMode() {
			return a.remoteRun(ctx, func(c *hostrpc.Client, r hostrpc.Run) (int, error) {
				v, err := c.Stop(ctx, r.RunID, *cleanup)
				if err != nil {
					err = fmt.Errorf("stopping run %s: %w; finish with \"canti stop -run %[1]s\", with a longer -cleanup if the job wrote a large output", r.RunID, err)
				}
				return a.remoteReport(v, err)
			})
		}
		ctx, cancel := context.WithTimeout(ctx, *cleanup)
		defer cancel()
		return a.with(ctx, exitActive, func(h *host.Host, run harness.Run, _ host.RunConfig) (harness.Snapshot, error) {
			return stop(ctx, h, run.ID)
		})
	}
}

var resolvable = []core.ExecutionState{core.ExecutionExited, core.ExecutionFailedToStart, core.ExecutionCancelled, core.ExecutionTimedOut}

func resolveCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	var flags resolutionFlags
	fs.BoolVar(&flags.job, "job", false, "record the job the executor finds for the unknown effect")
	fs.StringVar(&flags.execution, "execution", "", "record a confirmed outcome `state`: exited, failed_to_start, cancelled, or timed_out")
	fs.IntVar(&flags.exitCode, "exit-code", 0, "exit `code` of an exited effect; required for exited only")
	fs.StringVar(&flags.note, "note", "", "operator evidence `text`; required for a known resolution")
	fs.StringVar(&flags.owner, "owner", "", "physical owner `ID`; required for exited or timed_out assertions")
	hostFlags(fs, a)
	return func(ctx context.Context) (int, error) {
		if err := a.checkHostMode(); err != nil {
			return exitUsage, err
		}
		set := map[string]bool{}
		fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
		obs, err := flags.observation(set)
		if err != nil {
			return exitUsage, err
		}
		if a.remoteMode() {
			return a.remoteRun(ctx, func(c *hostrpc.Client, r hostrpc.Run) (int, error) {
				v, err := c.Resolve(ctx, r.RunID, hostserver.ResolutionOf(flags.job, obs, flags.owner))
				if flags.job && err != nil && strings.Contains(err.Error(), harness.ErrJobUnknown.Error()) {
					err = fmt.Errorf("%w; recording the job cannot resolve effect %s: inspect it, as with \"docker logs canti-%[2]s\", "+
						"then record its outcome with \"canti resolve -run %s -execution STATE\"", err, v.EffectID, r.RunID)
				}
				code, rerr := a.remoteReport(v, err)
				return cmpCode(code, exitOK), rerr
			})
		}
		return a.with(ctx, exitOK, func(h *host.Host, run harness.Run, _ host.RunConfig) (harness.Snapshot, error) {
			id := run.ID
			if !flags.job {
				return h.Resolve(ctx, id, obs, flags.owner)
			}
			snap, err := h.ResolveJob(ctx, id)
			if errors.Is(err, harness.ErrJobUnknown) {
				err = fmt.Errorf("%w; recording the job cannot resolve effect %s: inspect it, as with \"docker logs canti-%[2]s\", "+
					"then record its outcome with \"canti resolve -run %s -execution STATE\"", err, snap.State.EffectID, id)
			}
			return snap, err
		})
	}
}

type resolutionFlags struct {
	job                    bool
	execution, note, owner string
	exitCode               int
}

func (f resolutionFlags) observation(set map[string]bool) (harness.Observation, error) {
	obs := harness.Observation{Execution: core.ExecutionState(f.execution), Preview: f.note, Output: harness.ArtifactRef{Detail: "resolved by operator"}}
	switch {
	case f.job == set["execution"]:
		return harness.Observation{}, errors.New("give exactly one of -job and -execution")
	case f.job && (set["exit-code"] || set["note"] || set["owner"]):
		return harness.Observation{}, errors.New("-job takes no -exit-code, -note or -owner")
	case !f.job && obs.Execution == core.ExecutionUnknown:
		return harness.Observation{}, errors.New("unknown is not a completed resolution; leave the run in needs_resolution until the outcome is confirmed, then use -job or a known -execution state")
	case !f.job && !slices.Contains(resolvable, obs.Execution):
		return harness.Observation{}, fmt.Errorf("-execution must be one of %v", resolvable)
	case !f.job && set["exit-code"] != (obs.Execution == core.ExecutionExited):
		return harness.Observation{}, errors.New("-exit-code is required for exited and allowed for no other state")
	}
	if !f.job && strings.TrimSpace(f.note) == "" {
		return harness.Observation{}, errors.New("-note is required for an operator resolution")
	}
	if !f.job && (obs.Execution == core.ExecutionExited || obs.Execution == core.ExecutionTimedOut) && strings.TrimSpace(f.owner) == "" {
		return harness.Observation{}, errors.New("-owner is required for exited or timed_out resolution")
	}
	if set["exit-code"] {
		obs.ExitCode = &f.exitCode
	}
	return obs, nil
}

// openHost opens the host of the state directory, holding its lock until it
// is closed. Unless create is set, a missing journal is an error.
func (a *app) openHost(ctx context.Context, create bool) (*host.Host, error) {
	return host.Open(ctx, host.Options{Workspace: a.workspace, State: a.state, Logger: a.log, WrapJournal: wrapJournal, Checkpoint: checkpoint,
		Credentials: a.bound}, create)
}

// binding is what a run of model API api is driven with: the provider url
// names, else the API's base URL variable for a run that stores none, and
// the keys of this process.
func (a *app) binding(api, url string) host.Binding {
	return host.Binding{Provider: url, Fallback: baseURL(api), Keys: a.cred}
}

// baseURL is the value of the base URL variable of model API api.
func baseURL(api string) string {
	if variable := map[string]string{host.APIMessages: "ANTHROPIC_BASE_URL", host.APIResponses: "OPENAI_BASE_URL"}[api]; variable != "" {
		return os.Getenv(variable)
	}
	return ""
}

// with locks the workspace, finds the selected run, and reports what op
// does with it. Unsupported configurations fail before the operation.
func (a *app) with(ctx context.Context, active int, op func(*host.Host, harness.Run, host.RunConfig) (harness.Snapshot, error)) (int, error) {
	h, err := a.openHost(ctx, false)
	if err != nil {
		return exitError, err
	}
	defer h.Close()
	run, c, err := h.Find(ctx, a.runID)
	if err != nil {
		return exitError, err
	}
	snap, err := op(h, run, c)
	h.Close()
	return a.report(snap, err, active, c)
}

// drive catches SIGINT and SIGTERM until release and returns op, which drives
// the run through its host to a halt, or one step with once. The first
// signal, also one caught before op, shuts the run's controller down and
// then stops the run persistently within the cleanup deadline; the second
// abandons the cleanup. release records in a.signal SIGTERM if it was
// caught, else the first signal. While op drives, a.steps shows the run's
// progress, read from the journal, never from the commit path.
func (a *app) drive(ctx context.Context, once bool) (op func(*host.Host, string, host.Binding) (harness.Snapshot, error), release func()) {
	c := a.stopControl()
	loopCtx, shutdown := context.WithCancel(ctx)
	cleanupCtx, abandon := context.WithCancel(context.WithoutCancel(ctx))
	c.cancels = []context.CancelFunc{shutdown, abandon}
	startWatch := make(chan int, 1)
	handled := make(chan struct{})
	go func() {
		defer close(handled)
		c.watch(cleanupCtx.Done(), startWatch)
	}()
	op = func(h *host.Host, id string, b host.Binding) (harness.Snapshot, error) {
		startWatch <- c.pending()
		f, err := a.follow(h, id)
		if err != nil {
			return harness.Snapshot{}, err
		}
		defer f.finish()
		if loopCtx.Err() == nil {
			d, err := h.Drive(loopCtx, id, b, host.DriveOptions{Once: once})
			if err != nil {
				return harness.Snapshot{}, err
			}
			f.start()
			select {
			case <-d.Done():
				return d.Result()
			case <-loopCtx.Done():
			}
			if ctx.Err() != nil {
				h.Interrupt(id)
				return d.Result()
			}
		}
		cleanup, cancel := context.WithTimeout(cleanupCtx, stopCleanup)
		defer cancel()
		return stop(cleanup, h, id)
	}
	return op, func() {
		abandon()
		shutdown()
		<-handled
		signal.Stop(c.sig)
		signal.Stop(c.seen)
		a.record(c.sig)
		a.record(c.seen)
	}
}

// stopControl applies each stop signal of a drive to the next of its
// cancels. An explicit signal source uses a.stopSignals; each signal handled
// is kept on seen for release to record.
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

func stop(ctx context.Context, h *host.Host, id string) (harness.Snapshot, error) {
	snap, err := h.Stop(ctx, id)
	if err != nil {
		err = fmt.Errorf("stopping run %s: %w; finish with \"canti stop -run %[1]s\", with a longer -cleanup if the job wrote a large output", id, err)
	}
	return snap, err
}
