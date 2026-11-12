package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ChrAlpha/canti/runtime/core"
)

// ActionNamespace begins every harness action: an exec payload whose first
// word, after leading ActionSpace, begins with it is never a command.
// ActionSpace holds the bytes that may precede that word and the one byte
// that may follow an action's name. RetrievePrefix and HandoverPrefix are the
// forms the prompt teaches. DiscoveryMarker begins a shell command that
// searches the step records; discovery skips its steps and those of every
// harness action.
const (
	ActionNamespace = "canti:"
	ActionSpace     = " \t\n\r"
	retrieveName    = ActionNamespace + "retrieve"
	handoverName    = ActionNamespace + "handover"
	RetrievePrefix  = retrieveName + " "
	HandoverPrefix  = handoverName + " "
	DiscoveryMarker = "# canti:discovery"
)

type ActionName string

const (
	ActionCommand  ActionName = "command"
	ActionRetrieve ActionName = "retrieve"
	ActionHandover ActionName = "handover"
	ActionUnknown  ActionName = "unknown"
)

// Recovery is the recovery contract that the trusted implementation of an
// action guarantees.
type Recovery string

const (
	// RecoveryExternal may change the world outside canti; it runs only
	// through the executor and is never run again.
	RecoveryExternal Recovery = "external_effect"
	// RecoveryRead reads sources outside the journal and changes none; it
	// may be performed again until its outcome commits.
	RecoveryRead Recovery = "repeatable_read"
	// RecoveryLocal is computed from committed facts and changes only
	// harness state; performing it again yields identical facts.
	RecoveryLocal Recovery = "local_change"
)

// Local reports whether the harness carries the action out itself.
func (r Recovery) Local() bool { return r == RecoveryRead || r == RecoveryLocal }

// The stable codes an Action records for an action it cannot perform or that
// the request refuses, and the texts its outcome shows for them. A code is
// part of the recorded action; its text may be worded anew without changing
// a committed action.
const (
	codeRetrievalDisabled = "retrieval_disabled"
	codeUnknownAction     = "unknown_action"
	retrievalDisabled     = "retrieval is disabled for this run"
	unknownAction         = "unknown harness action; the actions are " + retrieveName + " and " + handoverName
)

func problemText(code string) string {
	return map[string]string{codeRetrievalDisabled: retrievalDisabled, codeUnknownAction: unknownAction}[code]
}
func (a Action) Problem() string { return problemText(a.Invalid) }

// MaxActionBytes bounds the JSON encoding of every valid Action, whose
// strings are codes of a closed set and whose offset is an int.
const MaxActionBytes = 192

// Action is the harness's reading of an accepted exec decision, committed
// once with the ResponseLoaded that accepts it. It copies no agent text: a
// command is the decision payload, and the arguments of any other action
// are the payload from Args, which only performance interprets. Invalid
// and Refused hold stable codes.
type Action struct {
	Name     ActionName
	Recovery Recovery
	Args     int    `json:",omitempty"`
	Invalid  string `json:",omitempty"`
}

// NormalizeAction reads payload, accepted under a request, by a loop that has
// a reader when reader is set.
// The loop commits its result once, with the acceptance. Afterwards only
// CheckEffect, which compares the two at the commit, and Matches, which ties
// a read action to its payload by rules no version changes, look at the
// payload's word again; performance alone interprets its arguments.
func NormalizeAction(payload string, reader bool) Action {
	a := Action{Name: ActionCommand, Recovery: RecoveryExternal}
	if word, args, reserved := actionWord(payload); reserved {
		a.Args = args
		switch word {
		case retrieveName:
			a.Name, a.Recovery = ActionRetrieve, RecoveryRead
			if !reader {
				a.Invalid = codeRetrievalDisabled
			}
		case handoverName:
			a.Name, a.Recovery = ActionHandover, RecoveryLocal
		default:
			a.Name, a.Recovery, a.Invalid = ActionUnknown, RecoveryLocal, codeUnknownAction
		}
	}

	return a
}

// actionWord returns the first word of payload after leading ActionSpace and
// the offset of the arguments after it and one byte of ActionSpace, and
// reports whether the word is in ActionNamespace.
func actionWord(payload string) (word string, args int, reserved bool) {
	rest := strings.TrimLeft(payload, ActionSpace)
	end, after := len(rest), len(rest)
	if i := strings.IndexAny(rest, ActionSpace); i >= 0 {
		end, after = i, i+1
	}
	return rest[:end], len(payload) - len(rest) + after, strings.HasPrefix(rest, ActionNamespace)
}

// Matches reports whether a, a valid action, is a reading of payload by
// rules that no later version changes: a command exactly for a payload
// outside ActionNamespace, the offset of the arguments after its word, and
// the word of a retrieval or handover. An unknown action matches any word
// of the namespace, so that a version that learns its word does not
// reinterpret it.
func (a Action) Matches(payload string) error {
	word, args, reserved := actionWord(payload)
	switch {
	case (a.Name == ActionCommand) == reserved:
		return fmt.Errorf("harness: a %s action records a payload %s the reserved namespace", a.Name, map[bool]string{true: "in", false: "outside"}[reserved])
	case a.Name != ActionCommand && a.Args != args:
		return fmt.Errorf("harness: the %s action records arguments at %d, not at %d", a.Name, a.Args, args)
	case a.Name == ActionHandover && word != handoverName, a.Name == ActionRetrieve && word != retrieveName:
		return fmt.Errorf("harness: the %s action records the word %q", a.Name, word)
	}
	return nil
}

// Validate checks that a recorded action is one that normalization yields
// for some payload.
func (a Action) Validate() error {
	recovery, ok := map[ActionName]Recovery{ActionCommand: RecoveryExternal, ActionRetrieve: RecoveryRead, ActionHandover: RecoveryLocal, ActionUnknown: RecoveryLocal}[a.Name]
	if !ok || a.Recovery != recovery {
		return fmt.Errorf("harness: invalid action recovery")
	}
	if (a.Name == ActionCommand) != (a.Args == 0) || a.Args < 0 {
		return fmt.Errorf("harness: invalid action argument offset")
	}
	invalid := map[ActionName]string{ActionUnknown: codeUnknownAction}[a.Name]
	if a.Invalid != invalid && !(a.Name == ActionRetrieve && a.Invalid == codeRetrievalDisabled) {
		return fmt.Errorf("harness: invalid action problem code")
	}
	return nil
}

// arguments returns the arguments of a, accepted for payload.
func (a Action) arguments(payload string) (string, error) {
	if a.Args < 0 || a.Args > len(payload) {
		return "", fmt.Errorf("harness: argument offset %d is outside the %d-byte payload", a.Args, len(payload))
	}
	return payload[a.Args:], nil
}

// Accepted is the committed normalized action and, for external work, its
// execution identity. No effect is interpreted again after acceptance.
type Accepted struct {
	Semantics int
	Action    Action
	Execution *ExecutionIdentity
}

func (a Accepted) Local() bool { return a.Action.Recovery.Local() }

// Acceptance validates a current-format accepted action and execution identity.
func Acceptance(semantics int, action *Action, execution *ExecutionIdentity) (Accepted, error) {
	if err := core.KnownSemantics(semantics); err != nil {
		return Accepted{}, err
	}
	if action == nil {
		return Accepted{}, fmt.Errorf("harness: acceptance has no action")
	}
	if err := action.Validate(); err != nil {
		return Accepted{}, err
	}
	if (action.Recovery == RecoveryExternal) != (execution != nil) {
		return Accepted{}, fmt.Errorf("harness: execution identity does not match action recovery")
	}
	if execution != nil {
		if err := execution.Validate(); err != nil {
			return Accepted{}, err
		}
	}
	return Accepted{Semantics: semantics, Action: *action, Execution: execution}, nil
}

// ActionOf reads the committed acceptance and checks that it still describes
// the current decision. Corrupt local work cannot route to the executor.
func ActionOf(ctx context.Context, j Journal, s core.State) (Accepted, error) {
	if s.EffectID == "" {
		return Accepted{}, ErrNotFound
	}
	a, err := j.Accepted(ctx, s.RunID, s.Step, s.Attempt)
	if err != nil {
		return Accepted{}, err
	}
	if _, err := Acceptance(a.Semantics, &a.Action, a.Execution); err != nil {
		return Accepted{}, fmt.Errorf("%w: %v", ErrIncompatible, err)
	}
	if err := a.Action.Matches(s.Decision.Payload); err != nil {
		return Accepted{}, fmt.Errorf("%w: %v", ErrIncompatible, err)
	}
	return a, nil
}

// ErrIncompatible identifies corrupt or unsupported committed harness facts.
var ErrIncompatible = errors.New("harness: incompatible record")
