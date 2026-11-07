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

	"github.com/ChrAlpha/canti/internal/harness/docker"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
	"github.com/ChrAlpha/canti/internal/host"
)

func chatCommand(fs *flag.FlagSet, a *app) action {
	session := fs.String("session", "", "conversation `ID` to continue (default a new one)")
	constraints := fs.String("constraints", "", "rules the agent must follow")
	url := modelURL(fs)
	var s host.Spec
	registerSpec(fs, &s)
	hostFlags(fs, a)
	level := fs.Lookup("log-level")
	level.DefValue = "warn"
	level.Value.Set(level.DefValue)
	return func(ctx context.Context) (int, error) {
		if err := canonicalImports(&s.Config, false); err != nil {
			return exitUsage, err
		}
		c := &chatSession{a: a, s: s, url: *url, constraints: *constraints}
		fs.Visit(func(f *flag.Flag) { c.explicit = c.explicit || f.Name == "constraints" })
		c.s.Config.Workspace = a.workspace
		c.id = cmp.Or(*session, strings.ToLower(rand.Text()))
		if code, err := c.setUp(ctx); err != nil {
			return code, err
		}
		if isatty.IsTerminal(os.Stdin.Fd()) {
			return c.tui(ctx, level.Value.String())
		}
		return c.lines(ctx)
	}
}

// chatSession is one chat of session id: the flags its inputs run with and
// the constraints they inherit unless set explicitly.
type chatSession struct {
	a           *app
	id          string
	s           host.Spec
	url         string
	constraints string
	explicit    bool
	fixed       string
}

// setUp checks the session, the layout, the constraints, the flags, and
// the credential, and creates the state directory, before any input.
func (c *chatSession) setUp(ctx context.Context) (int, error) {
	a := c.a
	if !sqlite.ValidKey(c.id) {
		return exitUsage, errors.New("-session must be 1 to 128 ASCII letters, digits, underscores, or hyphens")
	}
	if err := a.checkHostMode(); err != nil {
		return exitUsage, err
	}
	if err := docker.CheckLayout(a.workspace, a.state); err != nil {
		return exitError, err
	}
	c.fixed = c.constraints
	if !c.explicit {
		var err error
		if c.fixed, err = inherited(ctx, a, c.id); err != nil {
			return exitError, err
		}
	}
	if err := checkTask("input", "chat", c.fixed); err != nil {
		return exitUsage, err
	}
	if _, _, err := host.NewRun("chat", c.fixed, c.s, time.Now()); err != nil {
		return exitUsage, err
	}
	if err := a.checkCredential(ctx, c.s.Config, c.url); err != nil {
		return exitError, err
	}
	if err := os.MkdirAll(a.state, 0o700); err != nil {
		return exitError, err
	}
	if _, err := host.NewExecutor(c.s.Config, a.state); err != nil {
		return exitError, err
	}
	return exitOK, nil
}

// submit runs goal as the session's next input.
func (c *chatSession) submit(ctx context.Context, goal string) (int, error) {
	return c.a.submitSession(ctx, c.id, "in_"+rand.Text(), goal, c.constraints, c.explicit, c.s, c.url, false)
}

// fits rereads the constraints the next input inherits after an input did
// not fit, and returns why no input fits beside them, or nil.
func (c *chatSession) fits(ctx context.Context) (noFit, err error) {
	if !c.explicit {
		if c.fixed, err = inherited(ctx, c.a, c.id); err != nil {
			return nil, err
		}
	}
	_, _, noFit = host.NewRun("chat", c.fixed, c.s, time.Now())
	return noFit, nil
}

// tui runs the chat in the terminal UI, logging at level.
func (c *chatSession) tui(ctx context.Context, level string) (int, error) {
	a := c.a
	display := &chatDisplay{}
	terminalOutput := a.stderr
	a.stdout, a.stderr, a.steps = display, display, display
	var lv slog.Level
	_ = lv.UnmarshalText([]byte(level))
	a.log = slog.New(slog.NewTextHandler(display, &slog.HandlerOptions{Level: lv}))
	a.stopSignals = make(chan os.Signal, 2)
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(signals)
	submit := func(goal string) (int, error, os.Signal) {
		a.signal = nil
		if err := checkTask("input", goal, c.fixed); err != nil {
			return exitUsage, err, nil
		}
		code, err := c.submit(ctx, goal)
		if code == exitUsage {
			noFit, readErr := c.fits(ctx)
			if readErr != nil {
				return exitError, readErr, a.signal
			}
			if noFit != nil {
				return exitUsage, errNoInputFits{noFit}, a.signal
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
	prepare := func() { a.stopSignals, a.detach = make(chan os.Signal, 2), make(chan struct{}, 1) }
	var abandon func()
	if a.remoteMode() {
		abandon = func() {
			select {
			case a.detach <- struct{}{}:
			default:
			}
		}
	}
	code, err := runChatTUI(c.id, prepare, submit, interrupt, abandon, signals, display, a.displayFailure, os.Stdin, terminalOutput)
	fmt.Fprintf(terminalOutput, "canti: continue session with \"canti chat -session %s\"; new runs use the current launch flags.\n", c.id)
	return code, err
}

// lines runs the chat over standard input that is not a terminal: one input
// per line or block, until end of input, a signal, or a failed input.
func (c *chatSession) lines(ctx context.Context) (int, error) {
	a := c.a
	fmt.Fprintf(a.stderr, "canti: session %s; continue it with \"canti chat -session %[1]s\".\n", c.id)
	a.steps, a.stdout = a.stderr, cleaned{a.stdout}
	in := bufio.NewReader(os.Stdin)
	for {
		if _, err := fmt.Fprint(a.stderr, "> "); err != nil {
			return exitError, fmt.Errorf("showing chat prompt: %w", err)
		}
		goal, readErr := readInput(in, a.stderr)
		var opener *openerError
		if errors.As(readErr, &opener) {
			fmt.Fprintf(a.stderr, "canti chat: %v\n", readErr)
			continue
		}
		if goal != "" {
			if code, err, done := c.line(ctx, goal); done {
				return code, err
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

// line runs one input of lines and reports whether the chat ends with it.
func (c *chatSession) line(ctx context.Context, goal string) (int, error, bool) {
	a := c.a
	if err := checkTask("input", goal, c.fixed); err != nil {
		fmt.Fprintf(a.stderr, "canti chat: %v\n", err)
		return exitOK, nil, false
	}
	code, err := c.submit(ctx, goal)
	select {
	case displayErr := <-a.displayFailure:
		return exitError, fmt.Errorf("chat display: %w", displayErr), true
	default:
	}
	switch {
	case a.signal != nil:
		return code, err, true
	case code == exitUsage:
		noFit, readErr := c.fits(ctx)
		if readErr != nil {
			return exitError, readErr, true
		}
		if noFit != nil {
			return code, noFit, true
		}
		fmt.Fprintf(a.stderr, "canti chat: the input does not fit -context-bytes (%s)\n", strings.TrimPrefix(err.Error(), "-context-bytes: "))
	case err != nil || code != exitOK && code != exitStopped:
		return code, err, true
	}
	return exitOK, nil, false
}

// errNoInputFits ends the TUI with a usage error, as it ends line mode, when
// the constraints and flags leave no room for any input.
type errNoInputFits struct{ error }

// inherited returns the constraints the next input of session inherits.
func inherited(ctx context.Context, a *app, session string) (string, error) {
	path := filepath.Join(a.state, "canti.db")
	if fi, err := os.Stat(path); errors.Is(err, os.ErrNotExist) || err == nil && fi.Size() == 0 {
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
