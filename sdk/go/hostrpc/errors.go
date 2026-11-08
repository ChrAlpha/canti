package hostrpc

import (
	"errors"
	"time"
)

var (
	ErrConflict = errors.New("hostrpc: input conflict")
	ErrClosing  = errors.New("hostrpc: host is closing")
	ErrDriving  = errors.New("hostrpc: run is already driving")
	ErrCursor   = errors.New("hostrpc: invalid cursor")
)

type SpecError struct{ Err error }

func (e SpecError) Error() string { return e.Err.Error() }
func (e SpecError) Unwrap() error { return e.Err }

const maxResponse = 6*((8<<20)+(8<<20)+(16<<20)) + 1<<20
const maxWait = 30 * time.Second
