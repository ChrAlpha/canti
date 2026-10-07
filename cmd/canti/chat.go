package main

import (
	"bufio"
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/mattn/go-isatty"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/docker"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

var stdinTerminal = func() bool {
	return isatty.IsTerminal(os.Stdin.Fd())
}

func chatCommand(fs *flag.FlagSet, a *app) action {
	session := fs.String("session", "", "conversation `ID` to continue (default a new one)")
	constraints := fs.String("constraints", "", "rules the agent must follow")
	url := modelURL(fs)
	var s spec
	s.register(fs)
	level := fs.Lookup("log-level")
	level.DefValue = "warn"
	level.Value.Set(level.DefValue)
	return func(ctx context.Context) (int, error) {
		explicit := false
		fs.Visit(func(f *flag.Flag) { explicit = explicit || f.Name == "constraints" })
		s.config.Workspace = a.workspace
		id := cmp.Or(*session, strings.ToLower(rand.Text()))
		if !sqlite.ValidKey(id) {
			return exitUsage, errors.New("-session must be 1 to 128 ASCII letters, digits, underscores, or hyphens")
		}
		if err := docker.CheckLayout(a.workspace, a.state); err != nil {
			return exitError, err
		}
		fixed := *constraints
		if !explicit {
			var err error
			if fixed, err = inherited(ctx, a, id); err != nil {
				return exitError, err
			}
		}
		if err := checkTask("input", "chat", fixed); err != nil {
			return exitUsage, err
		}
		if _, _, err := newRun("chat", fixed, s, time.Now()); err != nil {
			return exitUsage, err
		}
		if a.credErr != nil {
			return exitError, a.credErr
		}
		if _, err := newModel(s.config, *url, a.cred); err != nil {
			return exitError, err
		}
		if err := os.MkdirAll(a.state, 0o700); err != nil {
			return exitError, err
		}
		if _, err := newExecutor(s.config, a.state); err != nil {
			return exitError, err
		}
		if isatty.IsTerminal(os.Stdin.Fd()) {
			display := &chatDisplay{}
			terminalOutput := a.stderr
			a.stdout, a.stderr, a.steps = display, display, display
			var lv slog.Level
			_ = lv.UnmarshalText([]byte(level.Value.String()))
			a.log = slog.New(slog.NewTextHandler(display, &slog.HandlerOptions{Level: lv}))
			a.stopSignals = make(chan os.Signal, 2)
			signals := make(chan os.Signal, 2)
			signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
			defer signal.Stop(signals)
			submit := func(goal string) (int, error, os.Signal) {
				a.signal = nil
				if err := checkTask("input", goal, fixed); err != nil {
					return exitUsage, err, nil
				}
				code, err := a.submitSession(ctx, id, "in_"+rand.Text(), goal, *constraints, explicit, s, *url, false)
				if code == exitUsage {
					if !explicit {
						var readErr error
						if fixed, readErr = inherited(ctx, a, id); readErr != nil {
							return exitError, readErr, a.signal
						}
					}
					if _, _, fixedErr := newRun("chat", fixed, s, time.Now()); fixedErr != nil {
						return exitUsage, errNoInputFits{fixedErr}, a.signal
					}
				}
				return code, err, a.signal
			}
			interrupt := func(sig os.Signal) {
				select {
				case a.stopSignals <- sig:
				default:
				}
			}
			prepare := func() { a.stopSignals = make(chan os.Signal, 2) }
			code, err := runChatTUI(id, prepare, submit, interrupt, signals, display, a.displayFailure, os.Stdin, terminalOutput)
			fmt.Fprintf(terminalOutput, "canti: continue session with \"canti chat -session %s\"; new runs use the current launch flags.\n", id)
			return code, err
		}
		terminal := stdinTerminal()
		prompt := a.stderr
		if terminal {
			prompt = syncStderr{a.display}
		}
		fmt.Fprintf(a.stderr, "canti: session %s; continue it with \"canti chat -session %[1]s\".\n", id)
		if terminal {
			fmt.Fprintln(a.stderr, "canti: input typed during a run is discarded; wait for the next prompt.")
		}
		a.steps, a.stdout = a.stderr, cleaned{a.stdout}
		in := bufio.NewReader(os.Stdin)
		discardInput := false
		for {
			if terminal {
				if err := a.display.Flush(); err != nil {
					return exitError, fmt.Errorf("showing chat prompt: %w", err)
				}
			}
			if discardInput {
				in.Reset(os.Stdin)
				discardInput = false
			}
			var promptErr error
			if terminal {
				promptErr = a.display.Prompt(int(os.Stdin.Fd()))
			} else {
				_, promptErr = fmt.Fprint(prompt, "> ")
			}
			if promptErr != nil {
				return exitError, fmt.Errorf("showing chat prompt: %w", promptErr)
			}
			goal, readErr := readInput(in, prompt)
			var opener *openerError
			if errors.As(readErr, &opener) {
				fmt.Fprintf(a.stderr, "canti chat: %v\n", readErr)
				continue
			}
			if goal != "" {
				if err := checkTask("input", goal, fixed); err != nil {
					fmt.Fprintf(a.stderr, "canti chat: %v\n", err)
				} else {
					code, err := a.submitSession(ctx, id, "in_"+rand.Text(), goal, *constraints, explicit, s, *url, false)
					discardInput = terminal
					select {
					case displayErr := <-a.displayFailure:
						return exitError, fmt.Errorf("chat display: %w", displayErr)
					default:
					}
					switch {
					case a.signal != nil && (a.signal != os.Interrupt || !terminal):
						return code, err
					case code == exitUsage:
						if !explicit {
							var readErr error
							if fixed, readErr = inherited(ctx, a, id); readErr != nil {
								return exitError, readErr
							}
						}
						if _, _, fixedErr := newRun("chat", fixed, s, time.Now()); fixedErr != nil {
							return code, fixedErr
						}
						fmt.Fprintf(a.stderr, "canti chat: the input does not fit -context-bytes (%s)\n", strings.TrimPrefix(err.Error(), "-context-bytes: "))
					case err != nil || code != exitOK && code != exitStopped:
						return code, err
					}
					a.signal = nil
				}
			}
			switch {
			case errors.Is(readErr, io.EOF):
				fmt.Fprintln(a.stderr)
				return exitOK, nil
			case readErr != nil:
				return exitError, readErr
			}
		}
	}
}

// errNoInputFits ends the TUI with a usage error, as it ends line mode, when
// the constraints and flags leave no room for any input.
type errNoInputFits struct{ error }

// inherited returns the constraints the next input of session inherits.
func inherited(ctx context.Context, a *app, session string) (string, error) {
	path := filepath.Join(a.state, "canti.db")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	store, err := sqlite.OpenReadOnly(ctx, path)
	if err != nil {
		return "", err
	}
	defer store.Close()
	prior, err := store.SessionRuns(ctx, session, 0, 1)
	if errors.Is(err, sqlite.ErrNoSessions) {
		return "", nil
	}
	if err != nil || len(prior) == 0 {
		return "", err
	}
	return prior[0].Constraints, nil
}

// openerError reports a line that starts with << but opens no block.
type openerError struct{}

func (*openerError) Error() string {
	return "start a block with <<WORD, <<'WORD', or <<-WORD, WORD made of ASCII letters, digits, underscores, or hyphens; put an input that starts with << in a block"
}

// readInput reads one input: a line, or after a line <<WORD or <<'WORD' the
// lines up to a line that is exactly WORD, prompting on prompt for each. As
// in a shell, <<-WORD strips leading tabs from those lines.
func readInput(in *bufio.Reader, prompt io.Writer) (string, error) {
	line, err := in.ReadString('\n')
	rest, ok := strings.CutPrefix(strings.TrimSpace(line), "<<")
	if !ok {
		return strings.TrimSpace(line), err
	}
	rest, tabs := strings.CutPrefix(rest, "-")
	word := strings.TrimSpace(rest)
	if len(word) > 2 && word[0] == '\'' && word[len(word)-1] == '\'' {
		word = word[1 : len(word)-1]
	}
	if !sqlite.ValidKey(word) {
		return "", &openerError{}
	}
	var b strings.Builder
	for err == nil {
		if _, writeErr := fmt.Fprint(prompt, "… "); writeErr != nil {
			return "", fmt.Errorf("showing chat prompt: %w", writeErr)
		}
		if line, err = in.ReadString('\n'); tabs {
			line = strings.TrimLeft(line, "\t")
		}
		if strings.TrimRight(line, "\r\n") == word {
			return strings.TrimSpace(b.String()), nil
		}
		b.WriteString(line)
	}
	if errors.Is(err, io.EOF) {
		err = fmt.Errorf("input ended inside <<%s; it was not submitted", word)
	}
	return "", err
}

// rendering prints each committed command and observation of the journal's
// runs to w.
type rendering struct {
	harness.Journal
	w io.Writer
}

func (r rendering) Commit(ctx context.Context, t harness.Transition) error {
	if err := r.Journal.Commit(ctx, t); err != nil {
		return err
	}
	if _, ok := t.Event.(core.ResponseLoaded); ok && t.To.Decision.Kind == core.Exec {
		fmt.Fprintf(r.w, "$ %s\n", strings.Join(excerpt(t.To.Decision.Payload, false), "\n  "))
	}
	if o := t.Facts.Observation; o != nil {
		status := string(o.Execution)
		if o.Execution == core.ExecutionExited && o.ExitCode != nil {
			status = fmt.Sprintf("exit %d", *o.ExitCode)
		}
		if d := excerpt(o.Output.Detail, false); len(d) > 0 {
			status += " (" + strings.Join(d, " ") + ")"
		}
		for _, line := range excerpt(o.Preview, true) {
			fmt.Fprintf(r.w, "│ %s\n", line)
		}
		fmt.Fprintf(r.w, "└ %s\n", status)
	}
	return nil
}

// cleaned writes what clean keeps of each write.
type cleaned struct{ io.Writer }

func (w cleaned) Write(p []byte) (int, error) {
	if _, err := io.WriteString(w.Writer, clean(string(p))); err != nil {
		return 0, err
	}
	return len(p), nil
}

// clean drops every character but newlines, tabs, and graphic characters.
func clean(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || unicode.IsGraphic(r) {
			return r
		}
		return -1
	}, s)
}

const excerptLines, excerptWidth = 8, 200

// excerpt returns at most excerptLines lines of s, each cut to excerptWidth
// bytes. With tail, it returns the last lines as a terminal shows output:
// each cut to what follows its last carriage return and cleaned. Otherwise
// it quotes, as Go does and marked (quoted), each line holding a character
// clean would drop.
func excerpt(s string, tail bool) []string {
	if s = strings.TrimRight(s, "\r\n"); s == "" {
		return nil
	}
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if l = strings.TrimSuffix(l, "\r"); tail {
			l = clean(l[strings.LastIndexByte(l, '\r')+1:])
		}
		short := l
		clipped := len(l) > excerptWidth
		if clipped {
			k := excerptWidth
			for k > 0 && !utf8.RuneStart(l[k]) {
				k--
			}
			short = l[:k]
		}
		if !tail && strings.ContainsFunc(l, hidden) {
			short = "(quoted) " + strconv.Quote(short)
		}
		if clipped {
			short += "…"
		}
		lines[i] = short
	}
	n := len(lines) - excerptLines
	switch {
	case n <= 0:
		return lines
	case tail:
		return append([]string{"…"}, lines[n:]...)
	}
	return append(lines[:excerptLines], fmt.Sprintf("… %d more %s", n, map[bool]string{true: "line", false: "lines"}[n == 1]))
}

// hidden reports a character that is not a tab or a graphic character.
func hidden(r rune) bool { return r != '\t' && (!unicode.IsGraphic(r) || r == utf8.RuneError) }
