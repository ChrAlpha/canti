package sqlite

import (
	"context"
	"fmt"

	"github.com/ChrAlpha/canti/internal/harness"
)

const latestRequestSQL = `SELECT semantics, facts FROM transitions WHERE run_id = ? AND kind = 'request_frozen' ORDER BY step DESC, rev DESC LIMIT 1`

func (s *Store) LatestRequest(ctx context.Context, runID string) (harness.Request, error) {
	var semantics int
	var data string
	if err := s.db.QueryRowContext(ctx, latestRequestSQL, runID).Scan(&semantics, &data); err != nil {
		return harness.Request{}, missing(err, "latest request")
	}
	if err := current(semantics); err != nil {
		return harness.Request{}, err
	}
	var facts harness.Facts
	if err := strict(data, &facts); err != nil {
		return harness.Request{}, err
	}
	if facts.Request == nil {
		return harness.Request{}, fmt.Errorf("%w: frozen request missing", ErrIncompatible)
	}
	return *facts.Request, nil
}
