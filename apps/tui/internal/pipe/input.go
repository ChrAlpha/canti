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

// line retains at most limit+1 bytes while consuming one pipe line. The caller
// can reject an oversized input without losing the next line or block boundary.
func line(in *bufio.Reader, limit int) (string, error) {
	var b strings.Builder
	for {
		part, err := in.ReadSlice('\n')
		if n := min(len(part), max(0, limit+1-b.Len())); n > 0 {
			b.Write(part[:n])
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return b.String(), err
		}
	}
}
func readInput(in *bufio.Reader, prompt io.Writer, limit int) (string, error) {
	first, err := line(in, limit)
	rest, ok := strings.CutPrefix(strings.TrimSpace(first), "<<")
	if !ok {
		return strings.TrimSpace(first), err
	}
	rest, tabs := strings.CutPrefix(rest, "-")
	word := strings.TrimSpace(rest)
	if len(word) > 2 && word[0] == '\'' && word[len(word)-1] == '\'' {
		word = word[1 : len(word)-1]
	}
	if !validDelimiter(word) {
		return "", &openerError{}
	}
	var b strings.Builder
	for err == nil {
		if _, e := fmt.Fprint(prompt, "… "); e != nil {
			return "", fmt.Errorf("showing chat prompt: %w", e)
		}
		var text string
		text, err = line(in, max(limit, 130))
		if tabs {
			text = strings.TrimLeft(text, "\t")
		}
		if strings.TrimRight(text, "\r\n") == word {
			return strings.TrimSpace(b.String()), nil
		}
		if n := min(len(text), max(0, limit+1-b.Len())); n > 0 {
			b.WriteString(text[:n])
		}
	}
	if errors.Is(err, io.EOF) {
		err = fmt.Errorf("input ended before block delimiter %s", word)
	}
	return "", err
}
