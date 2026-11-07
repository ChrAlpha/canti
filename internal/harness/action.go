package harness

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ChrAlpha/canti/core"
)

// ActionNamespace begins every harness action: an exec payload whose first
// word, after leading ActionSpace, begins with it is never a command.
// ActionSpace holds the bytes that may precede that word and the one byte
// that may follow an action's name. RetrievePrefix and HandoverPrefix are
// the forms the system prompt teaches. DiscoveryMarker begins a shell
// command that searches the step records; discovery skips its steps and
// those of every harness action.
const (
	ActionNamespace = "canti:"
	ActionSpace     = " \t\n\r"
	retrieveName    = ActionNamespace + "retrieve"
	handoverName    = ActionNamespace + "handover"
	RetrievePrefix  = retrieveName + " "
	HandoverPrefix  = handoverName + " "
	DiscoveryMarker = "# canti:discovery"
)

// ActionSemantics is the first semantics version whose accepting
// ResponseLoaded carries an Action: the version that added core.Performed.
const ActionSemantics = 4

// ActionName names what an accepted exec decision asks for.
type ActionName string

const (
	ActionCommand  ActionName = "command"
	ActionRetrieve ActionName = "retrieve"
	ActionHandover ActionName = "handover"
	ActionUnknown  ActionName = "unknown"
	// ActionLegacy names the action of an acceptance committed before
	// actions were recorded. It asserts no intent and is never recorded.
	ActionLegacy ActionName = "legacy"
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
	codeHandoverRequired  = "handover_required"

	retrievalDisabled = "retrieval is disabled for this run"
	unknownAction     = "unknown harness action; the actions are " + retrieveName + " and " + handoverName
	handoverRequired  = "handover is required before any more work; use " + handoverName
)

// problemText returns the text that an outcome shows for code.
func problemText(code string) string {
	return map[string]string{codeRetrievalDisabled: retrievalDisabled, codeUnknownAction: unknownAction, codeHandoverRequired: handoverRequired}[code]
}

// Problem returns the text of the refusal of a, or else of why it cannot be
// performed, or "" when neither applies: the detail of its outcome.
func (a Action) Problem() string { return problemText(cmp.Or(a.Refused, a.Invalid)) }

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
	Refused  string `json:",omitempty"`
}

// NormalizeAction reads payload, accepted under a request that requires a
// handover when requested, by a loop that has a reader when reader is set.
// The loop commits its result once, with the acceptance. Afterwards only
// CheckEffect, which compares the two at the commit, and Matches, which ties
// a read action to its payload by rules no version changes, look at the
// payload's word again; performance alone interprets its arguments.
func NormalizeAction(payload string, requested, reader bool) Action {
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
	if requested && a.Name != ActionHandover {
		a.Recovery, a.Refused = RecoveryLocal, codeHandoverRequired
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
// the word of a retrieval or a handover. An unknown action matches any word
// of the namespace, so that a version that learns its word does not
// reinterpret it.
func (a Action) Matches(payload string) error {
	word, args, reserved := actionWord(payload)
	switch {
	case (a.Name == ActionCommand) == reserved:
		return fmt.Errorf("harness: a %s action records a payload %s the reserved namespace", a.Name, map[bool]string{true: "in", false: "outside"}[reserved])
	case a.Name != ActionCommand && a.Args != args:
		return fmt.Errorf("harness: the %s action records arguments at %d, not at %d", a.Name, a.Args, args)
	case a.Name == ActionRetrieve && word != retrieveName, a.Name == ActionHandover && word != handoverName:
		return fmt.Errorf("harness: the %s action records the word %q", a.Name, word)
	}
	return nil
}

// Validate checks that a recorded action is one that normalization yields
// for some payload.
func (a Action) Validate() error {
	want := map[ActionName]Recovery{ActionCommand: RecoveryExternal, ActionRetrieve: RecoveryRead, ActionHandover: RecoveryLocal, ActionUnknown: RecoveryLocal}
	recovery, ok := want[a.Name]
	switch {
	case !ok:
		return fmt.Errorf("harness: unknown action %q", a.Name)
	case a.Refused != "" && (a.Refused != codeHandoverRequired || a.Name == ActionHandover):
		return fmt.Errorf("harness: %s action cannot be refused with %q", a.Name, a.Refused)
	case a.Refused != "":
		recovery = RecoveryLocal
	}
	invalid := map[ActionName]string{ActionUnknown: codeUnknownAction}[a.Name]
	switch {
	case a.Recovery != recovery:
		return fmt.Errorf("harness: %s action has recovery %q, want %q", a.Name, a.Recovery, recovery)
	case (a.Name == ActionCommand) != (a.Args == 0) || a.Args < 0:
		return fmt.Errorf("harness: %s action has argument offset %d", a.Name, a.Args)
	case a.Invalid != invalid && !(a.Name == ActionRetrieve && a.Invalid == codeRetrievalDisabled):
		return fmt.Errorf("harness: %s action is invalid for %q", a.Name, a.Invalid)
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

// Provenance tells whether an acceptance recorded its action.
type Provenance string

const (
	ProvenanceRecorded Provenance = "recorded"
	ProvenanceLegacy   Provenance = "legacy"
)

// Accepted is the committed acceptance of an exec decision: the semantics
// version of the accepting transition and its action. A legacy acceptance,
// committed before ActionSemantics, has the action ActionLegacy and no
// recovery; InProcess, which ActionOf sets from the effect's job, says only
// how stopping it proceeds.
type Accepted struct {
	Semantics  int
	Provenance Provenance
	Action     Action
	InProcess  bool
}

// Local reports whether the harness carries out or cancels the accepted
// effect itself.
func (a Accepted) Local() bool { return a.Action.Recovery.Local() || a.InProcess }

// Acceptance returns the acceptance that a transition committed under
// semantics with action a records. It fails unless the transition of a
// version before ActionSemantics holds no action and one of a later version
// holds a valid one, so a missing action is never read as legacy.
func Acceptance(semantics int, a *Action) (Accepted, error) {
	if err := core.KnownSemantics(semantics); err != nil {
		return Accepted{}, err
	}
	if semantics < ActionSemantics {
		if a != nil {
			return Accepted{}, fmt.Errorf("harness: an acceptance of semantics version %d holds an action", semantics)
		}
		return Accepted{Semantics: semantics, Provenance: ProvenanceLegacy, Action: Action{Name: ActionLegacy}}, nil
	}
	if a == nil {
		return Accepted{}, fmt.Errorf("harness: an acceptance of semantics version %d holds no action", semantics)
	}
	if err := a.Validate(); err != nil {
		return Accepted{}, err
	}
	return Accepted{Semantics: semantics, Provenance: ProvenanceRecorded, Action: *a}, nil
}

// LegacyInProcess reports whether a legacy effect was carried out in
// process: its job is the virtual job that versions before ActionSemantics
// bound for harness actions. It is the only reading of that ID.
func LegacyInProcess(effectID, jobID string) bool { return jobID == "read_"+effectID }

// ActionOf returns the acceptance of the effect of s, or ErrNotFound when s
// holds none. A recorded action that does not match the decision of s fails
// with ErrIncompatible, so that no corrupted acceptance routes a reserved
// payload to the executor.
func ActionOf(ctx context.Context, j Journal, s core.State) (Accepted, error) {
	if s.EffectID == "" {
		return Accepted{}, ErrNotFound
	}
	a, err := j.Accepted(ctx, s.RunID, s.Step, s.Attempt)
	switch {
	case err != nil:
		return Accepted{}, err
	case a.Provenance == ProvenanceLegacy:
		a.InProcess = LegacyInProcess(s.EffectID, s.JobID)
	default:
		if err := a.Action.Matches(s.Decision.Payload); err != nil {
			return Accepted{}, fmt.Errorf("%w: step %d of run %q: %v", ErrIncompatible, s.Step, s.RunID, err)
		}
	}
	return a, nil
}

// ErrIncompatible reports a committed record that contradicts the facts it
// belongs with, such as an action that is no reading of its decision. Like a
// store's incompatible record, it is refused, never acted on.
var ErrIncompatible = errors.New("harness: committed record contradicts its facts")

// errLegacyEffect reports an effect accepted before actions were recorded,
// which this harness only stops.
var errLegacyEffect = errors.New("harness: the effect was accepted before actions were recorded and can only be stopped")
