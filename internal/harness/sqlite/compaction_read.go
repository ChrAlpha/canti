package sqlite

import (
	"context"
	"errors"

	"github.com/ChrAlpha/canti/internal/harness"
)

// CompactionRecord gives diagnostics a stable pagination cursor independent of
// the business journal sequence. Its operation may still be in progress.
type CompactionRecord struct {
	Sequence  int64              `json:"sequence"`
	Operation harness.Compaction `json:"operation"`
}

// Compactions returns a bounded page of immutable plans and their current
// outcomes. A single operation is returned intact; callers choose body display.
func (s *Store) Compactions(ctx context.Context, runID string, after int64, limit int) ([]CompactionRecord, error) {
	if runID == "" || after < 0 || limit < 1 || limit > 100 {
		return nil, errors.New("sqlite: invalid compaction page")
	}
	rows, err := s.db.QueryContext(ctx, "SELECT rowid,data FROM compactions WHERE run_id=? AND rowid>? ORDER BY rowid LIMIT ?", runID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := []CompactionRecord{}
	bytes := 0
	for rows.Next() {
		var record CompactionRecord
		var data string
		if err := rows.Scan(&record.Sequence, &data); err != nil {
			return nil, err
		}
		if bytes+len(data) > ContextEventPageBytes && len(records) > 0 {
			break
		}
		if err := decodeCompaction(data, &record.Operation); err != nil {
			return nil, err
		}
		records = append(records, record)
		bytes += len(data)
	}
	return records, rows.Err()
}

func (s *Store) Compaction(ctx context.Context, runID, id string) (harness.Compaction, error) {
	var c harness.Compaction
	var data string
	err := s.db.QueryRowContext(ctx, "SELECT data FROM compactions WHERE run_id=? AND id=?", runID, id).Scan(&data)
	if err == nil {
		err = decodeCompaction(data, &c)
	}
	return c, missing(err, "compaction")
}
