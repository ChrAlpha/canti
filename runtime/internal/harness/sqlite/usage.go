package sqlite

import (
	"context"
	"database/sql"
	"errors"

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
