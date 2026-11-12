package harness

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// HandoverBytes bounds one handover text, in bytes.
const HandoverBytes = 2048

// Validate checks that h is a handover a successful performance yields: plain
// text, not blank, valid UTF-8, within HandoverBytes.
func (h Handover) Validate() error {
	if strings.TrimSpace(h.Text) == "" {
		return errors.New("harness: handover requires non-blank text")
	}
	if len(h.Text) > HandoverBytes {
		return fmt.Errorf("harness: handover text exceeds %d bytes", HandoverBytes)
	}
	if !utf8.ValidString(h.Text) {
		return errors.New("harness: handover text is not valid UTF-8")
	}
	return nil
}

// handover performs a handover, whose arguments are the text after the action
// word and its one separator, verbatim. It changes no source and reads nothing
// outside committed facts.
func (l *Loop) handover(args string) (Outcome, error) {
	h := Handover{Text: args}
	if err := h.Validate(); err != nil {
		return failed(err), nil
	}
	return Outcome{Status: OutcomeDone, Handover: &h}, nil
}
