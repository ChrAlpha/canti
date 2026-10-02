package sqlite

import (
	"context"
	"fmt"

	"github.com/ChrAlpha/canti/internal/harness"
)

const latestRequestSQL = `SELECT semantics, from_state, to_state, facts FROM transitions WHERE run_id = ? AND kind = 'request_frozen' ORDER BY step DESC, rev DESC LIMIT 1`

func (s *Store) LatestRequest(ctx context.Context, runID string) (harness.Request, error) {
	facts, err := decodeFacts(s.db.QueryRowContext(ctx, latestRequestSQL, runID))
	if err != nil {
		return harness.Request{}, err
	}
	if facts.Request == nil {
		return harness.Request{}, fmt.Errorf("%w: frozen request missing", ErrIncompatible)
	}
	return *facts.Request, nil
}
