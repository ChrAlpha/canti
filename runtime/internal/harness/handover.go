package harness

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

// HandoverBytes bounds one handover note, in bytes.
const HandoverBytes = 2048

// ValidateHandoverNote checks the note of a handover: valid UTF-8 and within
// HandoverBytes. It is optional, so blank text is valid.
func ValidateHandoverNote(text string) error {
	if len(text) > HandoverBytes {
		return fmt.Errorf("harness: handover note exceeds %d bytes", HandoverBytes)
	}
	if !utf8.ValidString(text) {
		return errors.New("harness: handover note is not valid UTF-8")
	}
	return nil
}

// handoverClaim reports whether the committed payload claims a successful
// handover (the exact word and a done outcome), and whether that claim holds:
// a valid note and exactly the closed done outcome shape.
func handoverClaim(payload string, o *Outcome) (claimed, success bool) {
	word, _, reserved := actionWord(payload)
	if !reserved || word != handoverName || o == nil || o.Status != OutcomeDone {
		return false, false
	}
	text, err := HandoverText(payload)
	valid := err == nil && ValidateHandoverNote(text) == nil
	return true, valid && *o == Outcome{Status: OutcomeDone}
}

// handover performs a handover, whose arguments are the note after the action
// word and its one separator, verbatim. It changes no source and reads nothing
// outside committed facts. The note is optional; the committed payload is its
// only copy.
func (l *Loop) handover(args string) (Outcome, error) {
	if err := ValidateHandoverNote(args); err != nil {
		return failed(err), nil
	}
	return Outcome{Status: OutcomeDone}, nil
}
