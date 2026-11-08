package pipe

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
)

type openerError struct{}

func (*openerError) Error() string {
	return "start a block with <<WORD, <<'WORD', or <<-WORD, WORD made of ASCII letters, digits, underscores, or hyphens; put an input that starts with << in a block"
}
func validDelimiter(s string) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

type overflowError struct{ limit int }

func (e *overflowError) Error() string {
	return fmt.Sprintf("input must be at most %d bytes; oversized input was not submitted", e.limit)
}

// line consumes one complete line while retaining a bounded prefix. Overflow
// is a separate fact: whitespace normalization must never hide missing bytes.
func line(in *bufio.Reader, limit int) (text string, overflow bool, err error) {
	var b strings.Builder
	for {
		part, e := in.ReadSlice('\n')
		n := min(len(part), max(0, limit+2-b.Len()))
		b.Write(part[:n])
		overflow = overflow || n < len(part)
		if !errors.Is(e, bufio.ErrBufferFull) {
			return b.String(), overflow, e
		}
	}
}
func readInput(in *bufio.Reader, prompt io.Writer, limit int) (string, error) {
	first, overflow, err := line(in, max(limit, 132))
	rest, ok := strings.CutPrefix(strings.TrimSpace(first), "<<")
	if !ok {
		text := strings.TrimSpace(first)
		if overflow || len(text) > limit {
			return "", errors.Join(&overflowError{limit}, err)
		}
		return text, err
	}
	rest, tabs := strings.CutPrefix(rest, "-")
	word := strings.TrimSpace(rest)
	if len(word) > 2 && word[0] == '\'' && word[len(word)-1] == '\'' {
		word = word[1 : len(word)-1]
	}
	if overflow || !validDelimiter(word) {
		return "", &openerError{}
	}
	var b strings.Builder
	for err == nil {
		if _, e := fmt.Fprint(prompt, "… "); e != nil {
			return "", fmt.Errorf("showing chat prompt: %w", e)
		}
		var text string
		var clipped bool
		text, clipped, err = line(in, max(limit, 132))
		if tabs {
			text = strings.TrimLeft(text, "\t")
		}
		if !clipped && strings.TrimRight(text, "\r\n") == word {
			goal := strings.TrimSpace(b.String())
			if overflow || len(goal) > limit {
				return "", &overflowError{limit}
			}
			return goal, nil
		}
		n := min(len(text), max(0, limit+2-b.Len()))
		b.WriteString(text[:n])
		overflow = overflow || clipped || n < len(text)
	}
	if errors.Is(err, io.EOF) {
		err = fmt.Errorf("input ended inside <<%s; it was not submitted", word)
	}
	return "", err
}
