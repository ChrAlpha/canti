package present

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
	"github.com/charmbracelet/x/ansi"
)

// clean drops every character but newlines, tabs, and graphic characters.
func Clean(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || unicode.IsGraphic(r) {
			return r
		}
		return -1
	}, ansi.Strip(s))
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
			l = Clean(l[strings.LastIndexByte(l, '\r')+1:])
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

// render shows item it: the command of an accepted exec decision, and the
// result of a completed step.
func Item(w io.Writer, it hostrpc.Item) {
	switch {
	case it.Kind == "withdrawn":
		fmt.Fprintf(w, "↩ withdrew %d input(s): %s\n", len(it.Withdrawn), strings.Join(it.Withdrawn, ", "))
	case it.Kind == "context_reset":
		fmt.Fprintf(w, "↳ context reset at sequence %d; continuing step %d from archive navigation\n", it.AnchorSeq, it.Step)
	case it.Kind == "decided":
		fmt.Fprintf(w, "$ %s\n", strings.Join(excerpt(it.Command, false), "\n  "))
	case it.Kind == "completed" && it.Outcome != nil:
		for _, line := range excerpt(outcomeSummary(*it.Outcome), true) {
			fmt.Fprintf(w, "│ %s\n", line)
		}
		fmt.Fprintf(w, "└ %s\n", it.Outcome.Status)
	case it.Kind == "completed":
		status := string(it.Execution)
		if (it.Execution == "cancelled" || it.Execution == "timed_out") && it.ExitCode == nil {
			status += " (exit code unavailable)"
		}
		if it.Execution == "exited" && it.ExitCode != nil {
			status = fmt.Sprintf("exit %d", *it.ExitCode)
		}
		if d := excerpt(it.Detail, false); len(d) > 0 {
			status += " (" + strings.Join(d, " ") + ")"
		}
		for _, line := range excerpt(it.Preview, true) {
			fmt.Fprintf(w, "│ %s\n", line)
		}
		fmt.Fprintf(w, "└ %s\n", status)
	}
}

// outcomeSummary describes the outcome of an action the harness performed,
// which ran no process.
func outcomeSummary(o hostrpc.Outcome) string {
	switch {
	case o.Detail != "":
		return o.Detail
	case o.Retrieval && o.RetrievalError != "":
		return "retrieval error: " + o.RetrievalError
	case o.Retrieval:
		return fmt.Sprintf("retrieved %d sources and %d directory entries", o.Sources, o.Entries)
	case o.HandoverBytes > 0:
		return fmt.Sprintf("handover recorded: %d bytes", o.HandoverBytes)
	}
	return ""
}
