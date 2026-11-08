package harness

import (
	"context"
	"errors"
	"slices"

	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

type focusEntry struct {
	Seq    int64 `json:"seq"`
	Source *int  `json:"source"`
}

func (l *Loop) focus(ctx context.Context, snap Snapshot, args string) (Outcome, error) {
	var proposal struct {
		Sources *[]focusEntry `json:"sources"`
	}
	if err := decodeArguments(args, &proposal); err != nil {
		return failed(err), nil
	}
	if proposal.Sources == nil {
		return failed(errors.New("focus requires sources, an array that replaces the selection")), nil
	}
	if len(*proposal.Sources) > archive.MaxSources {
		return failed(errors.New("focus exceeds 32 source ranges")), nil
	}
	refs := make([]EvidenceRef, 0, len(*proposal.Sources))
	known := map[int64]StepRecord{}
	for _, entry := range *proposal.Sources {
		if entry.Seq < 1 || entry.Seq >= snap.Seq || entry.Source == nil || *entry.Source < 0 {
			return failed(errors.New("focus requires a prior retrieval sequence and nonnegative source")), nil
		}
		rec, ok := known[entry.Seq]
		if !ok {
			var err error
			rec, err = l.journal.Completion(ctx, snap.State.RunID, entry.Seq)
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrIncompatible) {
				return failed(errors.New("focus sequence is not a completed retrieval")), nil
			}
			if err != nil {
				return Outcome{}, err
			}
			known[entry.Seq] = rec
		}
		retrieval := rec.retrieval()
		if rec.Step >= snap.State.Step || rec.Outcome == nil || rec.Outcome.Status != OutcomeDone || retrieval == nil || *entry.Source >= len(retrieval.Sources) {
			return failed(errors.New("focus source does not name an earlier successful retrieval")), nil
		}
		source := retrieval.Sources[*entry.Source]
		ref := EvidenceRef{SourceSeq: entry.Seq, Step: rec.Step, Source: *entry.Source, Length: source.Length, SHA256: source.SHA256}
		if _, err := evidenceOf(rec, ref); err != nil {
			return Outcome{}, err
		}
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, compareRefs)
	if err := validateCarry(refs, snap.State.Step); err != nil {
		return failed(err), nil
	}
	return Outcome{Status: OutcomeDone, Focus: &FocusSelection{Sources: refs}}, nil
}
