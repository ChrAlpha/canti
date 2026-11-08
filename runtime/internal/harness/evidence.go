package harness

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

// MaxFocus bounds the source ranges that one focus selection retains.
const MaxFocus = archive.MaxSources

// EvidenceRef names one source range that a retrieval committed: the
// sequence of the transition that performed the retrieval step, the step,
// the index of the source in the retrieval, and the length and SHA-256 of
// its bytes. A reference selected again keeps its original identity; the
// path, offset, and bytes are read from that transition, never copied.
type EvidenceRef struct {
	SourceSeq int64
	Step      int
	Source    int
	Length    int
	SHA256    string
}

func (r EvidenceRef) key() [2]int64 { return [2]int64{r.SourceSeq, int64(r.Source)} }

// compareRefs orders references by sequence, then source.
func compareRefs(a, b EvidenceRef) int {
	return cmp.Or(cmp.Compare(a.SourceSeq, b.SourceSeq), cmp.Compare(a.Source, b.Source))
}

// validateFocus checks refs as a context or an accepted focus holds them:
// at most MaxFocus, in ascending order of sequence and source without a
// repeat, each naming a source of a step before step by its length and a
// well-formed hash.
func validateFocus(refs []EvidenceRef, step int) error {
	if len(refs) > MaxFocus {
		return fmt.Errorf("harness: %d selected ranges exceed %d", len(refs), MaxFocus)
	}
	for i, r := range refs {
		if r.SourceSeq < 1 || r.Step < 1 || r.Step >= step || r.Source < 0 || r.Source >= archive.MaxSources ||
			r.Length < 0 || r.Length > archive.MaxBytes || !validSHA256(r.SHA256) {
			return fmt.Errorf("harness: invalid selected range %+v before step %d", r, step)
		}
		if i > 0 && compareRefs(refs[i-1], r) >= 0 {
			return errors.New("harness: selected ranges are not in ascending order without repeats")
		}
	}
	return nil
}

func validSHA256(s string) bool {
	_, err := hex.DecodeString(s)
	return len(s) == 2*sha256.Size && err == nil && strings.ToLower(s) == s
}

// FocusedEvidence is the committed observation a selected reference names:
// the reference and, from the
// retrieval that read it, the operation, source, continuation, and bytes.
type FocusedEvidence struct {
	Ref        EvidenceRef
	Operation  string
	Path       string
	Offset     int64
	More       bool
	NextOffset int64
	Incomplete bool
	Bytes      string
}

// newIntegrityError reports committed evidence that no longer matches the
// reference that names it: damage to the journal, which no other selection
// repairs and nothing reads again from its source, so it stops the run.
func newIntegrityError(format string, args ...any) error {
	return contextError{fmt.Errorf("harness: journal integrity: "+format, args...)}
}

// evidenceOf returns the evidence that ref names in rec, the step record of
// the transition at ref.SourceSeq, or an integrity error when rec holds it
// otherwise than ref describes it.
func evidenceOf(rec StepRecord, ref EvidenceRef) (FocusedEvidence, error) {
	o := rec.Outcome
	if rec.Seq != ref.SourceSeq || rec.Step != ref.Step || o == nil || o.Status != OutcomeDone || o.Retrieval == nil {
		return FocusedEvidence{}, newIntegrityError("transition %d holds no retrieval of step %d", ref.SourceSeq, ref.Step)
	}
	r := o.Retrieval
	if ref.Source < 0 || ref.Source >= len(r.Sources) || len(r.Evidence) != len(r.Sources) {
		return FocusedEvidence{}, newIntegrityError("the retrieval of step %d holds no evidence of source %d", ref.Step, ref.Source)
	}
	src, text := r.Sources[ref.Source], r.Evidence[ref.Source]
	sum := sha256.Sum256([]byte(text))
	if src.Length != ref.Length || len(text) != ref.Length || src.SHA256 != ref.SHA256 || hex.EncodeToString(sum[:]) != ref.SHA256 {
		return FocusedEvidence{}, newIntegrityError("the evidence of step %d source %d does not match its length %d and sha256 %s", ref.Step, ref.Source, ref.Length, ref.SHA256)
	}
	return FocusedEvidence{Ref: ref, Operation: r.Query.Operation, Path: src.Path, Offset: src.Offset,
		More: r.More, NextOffset: r.NextOffset, Incomplete: r.Incomplete, Bytes: text}, nil
}

// ValidateEvidence checks a reference against the exact committed observation.
func ValidateEvidence(record StepRecord, ref EvidenceRef) error {
	_, err := evidenceOf(record, ref)
	return err
}

// focusedEvidence resolves refs, references a committed transition of this
// run admitted, to their committed evidence, from known step records by
// sequence or else by one point read per transition. A reference whose
// transition is gone, is not a valid completion, or holds other evidence is
// journal damage, an integrity error; a done context or a failed read is
// returned as it is.
func (l *Loop) focusedEvidence(ctx context.Context, runID string, refs []EvidenceRef, known map[int64]StepRecord) ([]FocusedEvidence, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	if known == nil {
		known = map[int64]StepRecord{}
	}
	out := make([]FocusedEvidence, 0, len(refs))
	for _, ref := range refs {
		rec, ok := known[ref.SourceSeq]
		if !ok {
			var err error
			rec, err = l.journal.Completion(ctx, runID, ref.SourceSeq)
			switch {
			case errors.Is(err, ErrNotFound):
				return nil, newIntegrityError("no transition %d holds the evidence of step %d source %d", ref.SourceSeq, ref.Step, ref.Source)
			case errors.Is(err, ErrIncompatible):
				return nil, newIntegrityError("transition %d, which holds the evidence of step %d source %d, is no valid completion: %v", ref.SourceSeq, ref.Step, ref.Source, err)
			}
			if err != nil {
				return nil, err
			}
			known[ref.SourceSeq] = rec
		}
		c, err := evidenceOf(rec, ref)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}
