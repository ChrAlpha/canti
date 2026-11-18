package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// PriorUsage reads the last settled attempt of the previous frozen request
// revision of runID before seq. It never substitutes an older attempt, so a
// previous request without a committed response reports found=false.
func (s *Store) PriorUsage(ctx context.Context, runID string, before int64) (harness.Usage, bool, error) {
	var request int64
	if err := s.db.QueryRowContext(ctx, priorRequestSQL, runID, before).Scan(&request); err != nil {
		return harness.Usage{}, false, err
	}
	if before <= 0 {
		return harness.Usage{}, false, nil
	}
	var tokens, reported sql.NullInt64
	err := s.db.QueryRowContext(ctx, priorUsageSQL, runID, request, before).Scan(&tokens, &reported)
	if errors.Is(err, sql.ErrNoRows) {
		return harness.Usage{}, false, nil
	}
	if err != nil {
		return harness.Usage{}, false, err
	}
	usage := harness.Usage{}
	if reported.Valid && reported.Int64 != 0 {
		usage.InputTokens, usage.InputReported = tokens.Int64, true
	}
	return usage, true, nil
}

// MaxAcceptedInput aggregates the largest reported input usage among the run's
// complete, accepted responses before seq, and whether any reported it.
func (s *Store) MaxAcceptedInput(ctx context.Context, runID string, before int64) (int64, bool, error) {
	var maximum, reported int64
	if err := s.db.QueryRowContext(ctx, acceptedInputSQL, runID, before).Scan(&maximum, &reported); err != nil {
		return 0, false, err
	}
	return maximum, reported != 0, nil
}

// ComparableUsage reads the previous request of runID before seq, when it
// was frozen at or after anchor and an attempt of it reported input usage.
func (s *Store) ComparableUsage(ctx context.Context, runID string, before, anchor int64) (harness.ComparableUsage, bool, error) {
	return scanComparable(s.db.QueryRowContext(ctx, comparableUsageSQL, runID, before, anchor))
}

func scanComparable(row *sql.Row) (harness.ComparableUsage, bool, error) {
	var u harness.ComparableUsage
	var bytes sql.NullInt64
	err := row.Scan(&u.Seq, &bytes, &u.Input)
	if errors.Is(err, sql.ErrNoRows) {
		return u, false, nil
	}
	if err != nil {
		return u, false, err
	}
	if !bytes.Valid {
		return u, false, fmt.Errorf("%w: request %d has no context capture", ErrIncompatible, u.Seq)
	}
	u.Bytes = int(bytes.Int64)
	return u, true, nil
}

// PriorRequest reads the sequence of the latest request of runID frozen
// before seq, zero when there is none.
func (s *Store) PriorRequest(ctx context.Context, runID string, before int64) (int64, error) {
	var seq int64
	err := s.db.QueryRowContext(ctx, priorRequestSQL, runID, before).Scan(&seq)
	return seq, err
}
