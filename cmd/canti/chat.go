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
)

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
		if err := checkTask("chat", *constraints); err != nil {
			return exitUsage, err
		}
		if _, _, err := newRun("chat", *constraints, s, time.Now()); err != nil {
			return exitUsage, err
		}
		if a.credErr != nil {
			return exitError, a.credErr
		}
		id := cmp.Or(*session, strings.ToLower(rand.Text()))
		fmt.Fprintf(a.stderr, "canti: session %s; continue it with \"canti chat -session %[1]s\".\n", id)
		a.steps = a.stderr
		in := bufio.NewReader(os.Stdin)
		for {
			fmt.Fprint(a.stderr, "> ")
			line, readErr := in.ReadString('\n')
			if goal := strings.TrimSpace(line); goal != "" {
				err := checkTask(goal, *constraints)
				if err == nil {
					_, err = a.submitSession(ctx, id, "in_"+rand.Text(), goal, *constraints, explicit, s, *url, false)
				}
				if err != nil {
					fmt.Fprintf(a.stderr, "canti chat: %v\n", err)
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
		if o.ExitCode != nil {
			status = fmt.Sprintf("exit %d", *o.ExitCode)
		}
		for _, line := range append(excerpt(o.Preview, true), status) {
			fmt.Fprintf(r.w, "  %s\n", line)
		}
	}
	return nil
}

const excerptLines, excerptWidth = 8, 200

// excerpt returns at most excerptLines lines of s without control
// characters, the last ones when tail is set, each cut to excerptWidth bytes.
func excerpt(s string, tail bool) []string {
	s = strings.TrimRight(strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || !unicode.IsControl(r) {
			return r
		}
		return -1
	}, s), "\n")
	if s == "" {
		return nil
	}
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if len(l) > excerptWidth {
			lines[i] = strings.ToValidUTF8(l[:excerptWidth], "") + "…"
		}
	}
	n := len(lines) - excerptLines
	switch {
	case n <= 0:
		return lines
	case tail:
		return append([]string{fmt.Sprintf("… %d more lines", n)}, lines[n:]...)
	}
	return append(lines[:excerptLines], fmt.Sprintf("… %d more lines", n))
}
