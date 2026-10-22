package harness

import (
	"errors"
	"unicode/utf8"
)

// Window describes the adopted context projection, not execution progress.
// Through is a session source cursor. FirstStep is the first retained step of
// this run; earlier runs have their own identities and step numbering.
type Window struct {
	FirstStep int
	Through   int64
	Summary   string
}

func (w Window) Validate(step int) error {
	if w.FirstStep < 1 || w.FirstStep > step || w.Through < 0 || !utf8.ValidString(w.Summary) || (w.Through == 0 && w.Summary != "") || (w.Through > 0 && w.Summary == "") {
		return errors.New("harness: invalid compacted context window")
	}
	return nil
}
