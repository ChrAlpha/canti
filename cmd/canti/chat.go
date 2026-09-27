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
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

var stdinTerminal = func() bool {
	fi, err := os.Stdin.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
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
		if err := checkTask("input", "chat", *constraints); err != nil {
			return exitUsage, err
		}
		if _, _, err := newRun("chat", *constraints, s, time.Now()); err != nil {
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
		terminal := stdinTerminal()
		fmt.Fprintf(a.stderr, "canti: session %s; continue it with \"canti chat -session %[1]s\".\n", id)
		a.steps, a.stdout = a.stderr, cleaned{a.stdout}
		in := bufio.NewReader(os.Stdin)
		for {
			fmt.Fprint(a.stderr, "> ")
			goal, readErr := readInput(in)
			if goal != "" {
				if err := checkTask("input", goal, *constraints); err != nil {
					fmt.Fprintf(a.stderr, "canti chat: %v\n", err)
				} else {
					code, err := a.submitSession(ctx, id, "in_"+rand.Text(), goal, *constraints, explicit, s, *url, false)
					switch {
					case a.signal != nil && (a.signal != os.Interrupt || !terminal):
						return code, err
					case code == exitUsage:
						fmt.Fprintf(a.stderr, "canti chat: %v\n", err)
					case err != nil || code != exitOK && code != exitStopped:
						return code, err
					}
					a.signal = nil
				}
			}
			if errors.Is(readErr, io.EOF) {
				fmt.Fprintln(a.stderr)
				return exitOK, nil
			}
			if readErr != nil {
				return exitError, readErr
			}
		}
	}
}

// readInput reads one input: a line, or after a line <<WORD the lines up to
// a line that is exactly WORD.
func readInput(in *bufio.Reader) (string, error) {
	line, err := in.ReadString('\n')
	word, ok := strings.CutPrefix(strings.TrimSpace(line), "<<")
	if !ok || !sqlite.ValidKey(word) {
		return strings.TrimSpace(line), err
	}
	var b strings.Builder
	for err == nil {
		if line, err = in.ReadString('\n'); strings.TrimRight(line, "\r\n") == word {
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
	harness.SessionJournal
	w io.Writer
}

func (r rendering) Commit(ctx context.Context, t harness.Transition) error {
	if err := r.SessionJournal.Commit(ctx, t); err != nil {
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

// excerpt returns at most excerptLines cleaned lines of s, each cut to
// excerptWidth bytes. With tail, it returns the last lines, each cut to what
// follows its last carriage return, as a terminal shows output.
func excerpt(s string, tail bool) []string {
	if s = strings.TrimRight(s, "\r\n"); s == "" {
		return nil
	}
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if tail {
			l = strings.TrimSuffix(l, "\r")
			l = l[strings.LastIndexByte(l, '\r')+1:]
		}
		l = clean(l)
		if len(l) > excerptWidth {
			l = strings.ToValidUTF8(l[:excerptWidth], "") + "…"
		}
		lines[i] = l
	}
	n := len(lines) - excerptLines
	switch {
	case n <= 0:
		return lines
	case tail:
		return append([]string{"…"}, lines[n:]...)
	}
	return append(lines[:excerptLines], fmt.Sprintf("… %d more lines", n))
}
