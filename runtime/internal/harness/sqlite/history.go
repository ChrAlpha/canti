package sqlite

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// HistoryRun is one run of a line as a reopening reader needs it: its record,
// its state at the snapshot end, and its serial position. Line is recorded on
// the run, not derived from journal positions.
type HistoryRun struct {
	Run     harness.Run
	State   core.State
	Ordinal int
	Line    int
}

// HistoryLine fixes the recorded line a page belongs to and the snapshot log
// end that live following starts after.
type HistoryLine struct {
	Start   int
	End     int
	EndPos  int64
	Session string
}

// HistoryCursor is the server-issued page cursor. Pos is the exclusive log
// position; Seq pins each line run's transition stream so a later page seeks
// by (run_id, seq) instead of scanning a position range.
type HistoryCursor struct {
	Pos     int64            `json:"pos"`
	Seq     map[string]int64 `json:"seq,omitempty"`
	Session string           `json:"session,omitempty"`
}

type HistoryData struct {
	Runs    []HistoryRun
	Rows    []LogEntry
	Line    HistoryLine
	Cursor  int64
	Older   HistoryCursor
	Omitted bool
}

const (
	historySessionTTL = 5 * time.Second
	historySessionMax = 8
)

// ErrHistoryExpired reports that a pinned history snapshot is no longer open.
var ErrHistoryExpired = errors.New("sqlite: history snapshot expired")

// ErrHistoryCursor reports a cursor this snapshot did not issue.
var ErrHistoryCursor = errors.New("sqlite: history cursor was not issued by this snapshot")

// ErrHistoryClosed reports a first page after the Store began closing.
var ErrHistoryClosed = errors.New("sqlite: history store is closed")

// ErrHistoryBusy reports that every history slot is held by an in-flight
// first page, so none can be evicted for a new one.
var ErrHistoryBusy = errors.New("sqlite: every history slot is busy")

type historySession struct {
	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
	tx        *sql.Tx
	line      HistoryLine
	ids       []historyRunID
	runs      []HistoryRun
	issued    map[string]bool
	expires   time.Time
	timer     *time.Timer
	pending   int
	firstDone bool
	expired   bool
	draining  bool
}

func (s *Store) historySessionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, s.historyKey[:])
	mac.Write(b[:])
	return hex.EncodeToString(b[:]) + hex.EncodeToString(mac.Sum(nil)[:16]), nil
}

func (s *Store) validHistoryID(id string) bool {
	if len(id) != 64 {
		return false
	}
	raw, err := hex.DecodeString(id[:32])
	if err != nil {
		return false
	}
	want, err := hex.DecodeString(id[32:])
	if err != nil || len(want) != 16 {
		return false
	}
	mac := hmac.New(sha256.New, s.historyKey[:])
	mac.Write(raw)
	return hmac.Equal(want, mac.Sum(nil)[:16])
}

// SetHistoryLifetime changes the snapshot lifetime; tests use short or long
// values while production uses historySessionTTL.
func (s *Store) SetHistoryLifetime(d time.Duration) { s.historyTTL = d }

func historyCursorKey(c HistoryCursor) string {
	data, _ := json.Marshal(c)
	return string(data)
}

const (
	latestLineSQL = `SELECT line FROM runs WHERE n = (SELECT max(n) FROM runs)`
	runLineSQL    = `SELECT line FROM runs WHERE id = ?`
)

const historyRunIDsSQL = `SELECT n, id, line FROM runs WHERE line = ? ORDER BY n DESC LIMIT ?`

const summaryRunSQL = `SELECT id, COALESCE(json_extract(run, '$.InputID'), ''),
	COALESCE(substr(CAST(json_extract(run, '$.Goal') AS BLOB), 1, 8193), ''),
	COALESCE(substr(CAST(json_extract(run, '$.Constraints') AS BLOB), 1, 8193), ''),
	COALESCE(input_pos,0), fresh
	FROM runs WHERE id = ?`

const summaryRunConfigSQL = `SELECT COALESCE(json_extract(run, '$.Config'), '') FROM runs WHERE id = ?`

func scanSummaryRun(row interface{ Scan(...any) error }) (harness.Run, error) {
	var run harness.Run
	if err := row.Scan(&run.ID, &run.InputID, &run.Goal, &run.Constraints, &run.InputPos, &run.Fresh); err != nil {
		return harness.Run{}, err
	}
	return run, nil
}

func (s *Store) summaryConfig(ctx context.Context, tx *sql.Tx, id string) ([]byte, error) {
	var config string
	if err := tx.QueryRowContext(ctx, summaryRunConfigSQL, id).Scan(&config); err != nil {
		return nil, err
	}
	if config == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(config)
}

// A page reads each line run through its (run_id, seq) index, the line's
// inputs through inputs_line, and its withdrawals through
// inputs_line_withdrawn. No query interprets a position as line membership.
var historyTransitionsRunSQL = `SELECT ` + progressColumns + `
	FROM transitions INDEXED BY sqlite_autoindex_transitions_1
	WHERE run_id = ?1 AND pos < ?2
	ORDER BY seq DESC LIMIT ?3`

var historyTransitionsSeqSQL = `SELECT ` + progressColumns + `
	FROM transitions INDEXED BY sqlite_autoindex_transitions_1
	WHERE run_id = ?1 AND seq <= ?2
	ORDER BY seq DESC LIMIT ?3`

var historyInputsLineSQL = `SELECT pos, COALESCE(run_id, ''), authority, delivery, disposition, COALESCE(input_id, ''),
	` + clipped(`text`, ProgressCommandBytes) + `, ` + clipped(`source`, harness.InputKeyBytes) + `, ` + clipped(`subject`, harness.InputKeyBytes) + `, COALESCE(delivered_pos, 0),
	COALESCE(admitted_line, 0), COALESCE(admitted_disposition, ''), COALESCE(line, 0)
	FROM inputs INDEXED BY inputs_line WHERE line = ?1 AND pos < ?2
	ORDER BY pos DESC LIMIT ?3`

const historyStateSQL = `SELECT COALESCE(json_extract(to_state, '$.Phase'), ''),
	COALESCE(json_extract(to_state, '$.Step'), 0),
	COALESCE(json_extract(to_state, '$.Attempt'), 0),
	COALESCE(substr(CAST(json_extract(to_state, '$.Decision.kind') AS BLOB), 1, 257), ''),
	COALESCE(substr(CAST(json_extract(to_state, '$.Decision.payload') AS BLOB), 1, 8193), ''),
	COALESCE(json_extract(to_state, '$.EffectID'), ''),
	COALESCE(json_extract(to_state, '$.Reason'), '')
	FROM transitions INDEXED BY sqlite_autoindex_transitions_1
	WHERE run_id = ?1 ORDER BY seq DESC LIMIT 1`

var historyQueuedLineSQL = `SELECT pos, COALESCE(run_id, ''), authority, delivery, disposition, COALESCE(input_id, ''),
	` + clipped(`text`, ProgressCommandBytes) + `, ` + clipped(`source`, harness.InputKeyBytes) + `, ` + clipped(`subject`, harness.InputKeyBytes) + `, COALESCE(delivered_pos, 0),
	COALESCE(admitted_line, 0), COALESCE(admitted_disposition, ''), COALESCE(line, 0)
	FROM inputs INDEXED BY inputs_queued_line
	WHERE line IS NULL AND disposition = 'queued' AND pos < ?1
	ORDER BY pos DESC LIMIT ?2`

// historyWithdrawalsSQL walks the line's withdrawal positions through
// inputs_line_withdrawn with LIMIT and reads bounded names from the same line.
var historyWithdrawalsSQL = `WITH RECURSIVE page(n, withdrawn_pos) AS (
	SELECT 1, (SELECT max(withdrawn_pos) FROM inputs WHERE line = ?1 AND withdrawn_pos < ?2)
	UNION ALL
	SELECT page.n + 1, (SELECT max(withdrawn_pos) FROM inputs WHERE line = ?1 AND withdrawn_pos < page.withdrawn_pos)
	FROM page WHERE page.withdrawn_pos IS NOT NULL AND page.n < ?3
)
SELECT p.withdrawn_pos, COALESCE((SELECT line FROM inputs WHERE line = ?1 AND withdrawn_pos = p.withdrawn_pos LIMIT 1), 0), COALESCE((
	SELECT group_concat(printf('%d:%s', i.pos, COALESCE(i.input_id, 'pos:' || i.pos)), char(0))
	FROM (SELECT pos, input_id FROM inputs WHERE line = ?1 AND withdrawn_pos = p.withdrawn_pos ORDER BY pos LIMIT ?4) i
), '')
FROM page p WHERE p.withdrawn_pos IS NOT NULL`

type historyRunID struct {
	ordinal int
	id      string
	line    int
}

// History reads one page of a line, newest pages first. The token's Start is
// the recorded line ordinal; a missing token starts a new read at the latest
// line. The snapshot's examined end is fixed on the first page and reused by
// later pages, so run summaries are as of that snapshot and everything after
// the cursor is left to live follow.
func (s *Store) History(ctx context.Context, before HistoryCursor, limit, maxRuns, maxSummaries int, line HistoryLine) (HistoryData, error) {
	if before.Pos < 0 || limit <= 0 || limit > 256 || maxRuns <= 0 || maxSummaries <= 0 {
		return HistoryData{}, fmt.Errorf("sqlite: invalid history page")
	}
	s.histories.mu.Lock()
	if s.historyClosed {
		s.histories.mu.Unlock()
		return HistoryData{}, ErrHistoryClosed
	}
	s.historyOps.Add(1)
	s.histories.mu.Unlock()
	defer s.historyOps.Done()
	if before.Pos == 0 && line.Session == "" {
		if before.Seq != nil || before.Session != "" {
			return HistoryData{}, fmt.Errorf("%w: a first page carries no cursor", ErrHistoryCursor)
		}
		data, session, id, err := s.startHistory(ctx, limit, maxRuns, maxSummaries)
		if err != nil || session == nil {
			return data, err
		}
		if !s.completeFirstPage(id, session) {
			return HistoryData{}, ErrHistoryClosed
		}
		return data, nil
	}
	return s.continueHistory(ctx, before, limit, maxRuns, maxSummaries, line)
}

// reserveHistorySlot takes one connection slot. A slot is released only by
// closeSession after its transaction is rolled back, including evictions,
// expiry, drain, Store.Close, and an abandoned first page. At the cap it
// evicts the oldest published session and waits for its slot to be released.
// If no published session can be evicted, it fails with ErrHistoryBusy.
func (s *Store) reserveHistorySlot() error {
	for {
		s.histories.mu.Lock()
		if s.historyClosed {
			s.histories.mu.Unlock()
			return ErrHistoryClosed
		}
		if s.historySlots < historySessionMax {
			s.historySlots++
			s.beginClose()
			s.histories.mu.Unlock()
			return nil
		}
		var oldestKey string
		var oldest *historySession
		for k, old := range s.histories.entries {
			if old.pending != 0 || !old.firstDone {
				continue
			}
			if oldest == nil || old.expires.Before(oldest.expires) {
				oldestKey, oldest = k, old
			}
		}
		if oldest == nil {
			s.histories.mu.Unlock()
			return ErrHistoryBusy
		}
		delete(s.histories.entries, oldestKey)
		s.histories.mu.Unlock()
		s.closeSession(oldest)
	}
}

func (s *Store) startHistory(ctx context.Context, limit, maxRuns, maxSummaries int) (HistoryData, *historySession, string, error) {
	if err := s.reserveHistorySlot(); err != nil {
		return HistoryData{}, nil, "", err
	}
	line := HistoryLine{}
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		s.closeSession(&historySession{})
		return HistoryData{}, nil, "", err
	}
	session := &historySession{tx: tx}
	published := false
	defer func() {
		if !published {
			s.closeSession(session)
		}
	}()
	var last int64
	if err := s.stmt(ctx, tx, lastPosSQL).QueryRowContext(ctx).Scan(&last); err != nil {
		return HistoryData{}, nil, "", err
	}
	if line.Start == 0 {
		if err := tx.QueryRowContext(ctx, latestLineSQL).Scan(&line.Start); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if s.firstPageClosed() {
					return HistoryData{}, nil, "", ErrHistoryClosed
				}
				return HistoryData{Cursor: last}, nil, "", nil
			}
			return HistoryData{}, nil, "", err
		}
	}
	if line.EndPos == 0 {
		line.EndPos = last + 1
	}
	ids, err := s.historyRunIDs(ctx, tx, line.Start, maxRuns)
	if err != nil {
		return HistoryData{}, nil, "", err
	}
	runs := []HistoryRun{}
	if len(ids) > maxSummaries {
		runs, err = s.historyRuns(ctx, tx, ids[:maxSummaries])
	} else {
		runs, err = s.historyRuns(ctx, tx, ids)
	}
	if err != nil {
		return HistoryData{}, nil, "", err
	}
	data := HistoryData{Line: line, Cursor: line.EndPos - 1, Runs: runs, Omitted: len(ids) > maxSummaries}
	if data.Rows, data.Older, err = s.historyRows(ctx, tx, ids, HistoryCursor{}, line.Start, line.EndPos, limit); err != nil {
		return HistoryData{}, nil, "", err
	}
	if data.Older.Pos == 0 {
		if s.firstPageClosed() {
			return HistoryData{}, nil, "", ErrHistoryClosed
		}
		return data, nil, "", nil
	}
	id, err := s.historySessionID()
	if err != nil {
		return HistoryData{}, nil, "", err
	}
	data.Older.Session = id
	line.Session = id
	data.Line = line
	session.line = line
	session.ids = ids
	session.runs = runs
	session.issued = map[string]bool{historyCursorKey(data.Older): true}
	if !s.putHistory(id, session) {
		return HistoryData{}, nil, "", ErrHistoryClosed
	}
	published = true
	return data, session, id, nil
}

func (s *Store) firstPageClosed() bool {
	s.histories.mu.Lock()
	defer s.histories.mu.Unlock()
	return s.historyClosed
}

// completeFirstPage is the operation's final decision. A published session
// stays ineligible for eviction until this runs, after the page has decided
// its result.
func (s *Store) completeFirstPage(id string, session *historySession) bool {
	s.histories.mu.Lock()
	if s.historyClosed {
		s.histories.mu.Unlock()
		s.closeHistory(id, session)
		return false
	}
	session.pending--
	session.firstDone = true
	session.expires = time.Now().Add(s.historyTTL)
	expires := session.expires
	s.histories.mu.Unlock()
	session.mu.Lock()
	if !session.closed {
		session.timer = time.AfterFunc(time.Until(expires), func() {
			s.expireHistory(id, session)
		})
	}
	session.mu.Unlock()
	return true
}

// admitContinuation refuses a closed, expired, or draining snapshot before
// registering an operation, so a late request does no SQL and cannot prolong
// a session that is already finishing.
func (s *Store) admitContinuation(id string) (*historySession, error) {
	s.histories.mu.Lock()
	if s.historyClosed {
		s.histories.mu.Unlock()
		return nil, ErrHistoryClosed
	}
	session := s.histories.entries[id]
	if session != nil {
		expired := session.expired || time.Now().After(session.expires)
		if expired || session.draining {
			if expired {
				session.expired = true
			}
			closeNow := false
			if session.pending == 0 {
				s.dropHistoryLocked(session)
				closeNow = true
			}
			s.histories.mu.Unlock()
			if closeNow {
				s.closeSession(session)
			}
			return nil, fmt.Errorf("%w: %s", ErrHistoryExpired, id)
		}
		session.pending++
	}
	s.histories.mu.Unlock()
	if session == nil {
		if s.closed() {
			return nil, ErrHistoryClosed
		}
		if s.validHistoryID(id) {
			return nil, fmt.Errorf("%w: %s", ErrHistoryExpired, id)
		}
		return nil, fmt.Errorf("%w: unknown session %s", ErrHistoryCursor, id)
	}
	return session, nil
}

func (s *Store) continueHistory(ctx context.Context, before HistoryCursor, limit, maxRuns, maxSummaries int, line HistoryLine) (HistoryData, error) {
	if before.Session == "" || line.Session == "" || before.Session != line.Session {
		return HistoryData{}, fmt.Errorf("%w: cursor and line must name the same session", ErrHistoryCursor)
	}
	session, err := s.admitContinuation(before.Session)
	if err != nil {
		return HistoryData{}, err
	}
	decided := false
	defer func() {
		if !decided {
			s.releaseHistoryOp(session)
		}
	}()
	var data HistoryData
	var opErr error
	var last bool
	session.mu.Lock()
	switch {
	case session.closed:
		opErr = fmt.Errorf("%w: %s", ErrHistoryExpired, before.Session)
	case !session.issued[historyCursorKey(before)]:
		opErr = fmt.Errorf("%w: %+v", ErrHistoryCursor, before)
	case before.Pos > session.line.EndPos:
		opErr = fmt.Errorf("sqlite: history cursor %d is past the snapshot end %d", before.Pos, session.line.EndPos)
	default:
		pageEnd := min(before.Pos, session.line.EndPos)
		data = HistoryData{Line: session.line, Cursor: pageEnd - 1, Runs: session.runs}
		data.Older = HistoryCursor{}
		if pageEnd > 0 {
			data.Rows, data.Older, opErr = s.historyRows(ctx, session.tx, session.ids, before, session.line.Start, pageEnd, limit)
		}
		if opErr == nil {
			if data.Older.Pos > 0 {
				data.Older.Session = before.Session
				session.issued[historyCursorKey(data.Older)] = true
			}
			last = data.Older.Pos == 0
		}
	}
	session.mu.Unlock()
	closeNow, status := s.finishHistoryOp(session, last)
	decided = true
	if closeNow {
		s.closeSession(session)
	}
	switch status {
	case historyOpClosed:
		return HistoryData{}, ErrHistoryClosed
	case historyOpExpired:
		return HistoryData{}, fmt.Errorf("%w: %s", ErrHistoryExpired, before.Session)
	}
	if opErr != nil {
		return HistoryData{}, opErr
	}
	return data, nil
}

const (
	historyOpOK = iota
	historyOpClosed
	historyOpExpired
)

// finishHistoryOp decides one operation's result under the map lock. Once a
// terminal page has marked the session draining, every nonterminal operation
// returns ErrHistoryExpired, whether or not another operation is still
// pending: its nonempty Older cannot be used, because admission refuses a
// draining session. Terminal operations still return their rows, whose empty
// Older means no more pages. The session closes when pending reaches zero.
// The timer only marks a session expired while an operation is in flight; this
// decision performs the close.
func (s *Store) finishHistoryOp(session *historySession, last bool) (bool, int) {
	s.histories.mu.Lock()
	defer s.histories.mu.Unlock()
	if s.historyClosed {
		return s.releaseHistoryLocked(session), historyOpClosed
	}
	if session.expired || time.Now().After(session.expires) {
		session.expired = true
		return s.releaseHistoryLocked(session), historyOpExpired
	}
	session.pending--
	if last {
		session.draining = true
	}
	if !last && session.draining {
		if session.pending == 0 {
			s.dropHistoryLocked(session)
			return true, historyOpExpired
		}
		return false, historyOpExpired
	}
	if last && session.pending == 0 {
		s.dropHistoryLocked(session)
		return true, historyOpOK
	}
	return false, historyOpOK
}

// releaseHistoryOp releases an operation that failed before deciding a page.
// If it was the last operation on a session that must close, it removes it so
// closeSession can release the slot.
func (s *Store) releaseHistoryOp(session *historySession) {
	s.histories.mu.Lock()
	closeNow := s.releaseHistoryLocked(session)
	s.histories.mu.Unlock()
	if closeNow {
		s.closeSession(session)
	}
}

func (s *Store) releaseHistoryLocked(session *historySession) bool {
	session.pending--
	if session.pending == 0 && (session.expired || s.historyClosed || session.draining) {
		s.dropHistoryLocked(session)
		return true
	}
	return false
}

func (s *Store) dropHistoryLocked(session *historySession) {
	for id, held := range s.histories.entries {
		if held == session {
			delete(s.histories.entries, id)
			return
		}
	}
}

func (s *Store) closed() bool {
	s.histories.mu.Lock()
	defer s.histories.mu.Unlock()
	return s.historyClosed
}

// expireHistory is the timer path. It never rolls back a transaction while an
// operation is in flight; it marks the session expired and lets the last
// operation perform the close.
func (s *Store) expireHistory(id string, session *historySession) {
	s.histories.mu.Lock()
	if s.histories.entries[id] != session {
		s.histories.mu.Unlock()
		return
	}
	if session.pending > 0 {
		session.expired = true
		s.histories.mu.Unlock()
		return
	}
	delete(s.histories.entries, id)
	s.histories.mu.Unlock()
	s.closeSession(session)
}

func (s *Store) putHistory(id string, session *historySession) bool {
	s.histories.mu.Lock()
	if s.historyClosed {
		s.histories.mu.Unlock()
		return false
	}
	session.pending = 1
	s.histories.entries[id] = session
	s.histories.mu.Unlock()
	return true
}

func (s *Store) beginClose()  { s.historyCloses.Add(1) }
func (s *Store) finishClose() { s.historyCloses.Done() }

func (s *Store) closeHistory(id string, session *historySession) {
	s.histories.mu.Lock()
	if s.histories.entries[id] == session {
		delete(s.histories.entries, id)
	}
	s.histories.mu.Unlock()
	s.closeSession(session)
}

func (s *Store) closeSession(session *historySession) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.closeOnce.Do(func() {
		session.closed = true
		if session.timer != nil {
			session.timer.Stop()
		}
		if session.tx != nil {
			_ = session.tx.Rollback()
		}
		s.histories.mu.Lock()
		s.historySlots--
		s.histories.mu.Unlock()
		s.finishClose()
	})
}

func (s *Store) historyRunIDs(ctx context.Context, tx *sql.Tx, line, maxRuns int) ([]historyRunID, error) {
	rows, err := tx.QueryContext(ctx, historyRunIDsSQL, line, maxRuns)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []historyRunID
	for rows.Next() {
		var run historyRunID
		if err := rows.Scan(&run.ordinal, &run.id, &run.line); err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

// historyRuns projects each run with its state at snapshotEnd. A run opened
// after the snapshot has no transition at that end and is shown from its
// initial state, so its later return is rendered by live follow only.
func (s *Store) historyRuns(ctx context.Context, tx *sql.Tx, ids []historyRunID) ([]HistoryRun, error) {
	out := make([]HistoryRun, 0, len(ids))
	for i, id := range ids {
		run, err := scanSummaryRun(tx.QueryRowContext(ctx, summaryRunSQL, id.id))
		if err != nil {
			return nil, err
		}
		if i == 0 {
			run.Config, err = s.summaryConfig(ctx, tx, id.id)
			if err != nil {
				return nil, err
			}
		}
		state, err := s.historyState(ctx, tx, id.id)
		if err != nil {
			return nil, err
		}
		out = append(out, HistoryRun{Run: run, State: state, Ordinal: id.ordinal, Line: id.line})
	}
	return out, nil
}

func (s *Store) historyState(ctx context.Context, tx *sql.Tx, runID string) (core.State, error) {
	var phase, decisionKind, payload, effectID, reason string
	var step, attempt int
	err := tx.QueryRowContext(ctx, historyStateSQL, runID).Scan(&phase, &step, &attempt, &decisionKind, &payload, &effectID, &reason)
	if err == nil {
		return core.State{RunID: runID, Step: step, Attempt: attempt, Phase: core.Phase(phase),
			Decision: core.Decision{Kind: core.Kind(decisionKind), Payload: payload}, EffectID: effectID, Reason: core.StopReason(reason)}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return core.State{}, err
	}
	var runSemantics int
	var initial string
	if err := s.stmt(ctx, tx, initialSQL).QueryRowContext(ctx, runID).Scan(&runSemantics, &initial); err != nil {
		return core.State{}, missing(err, "run")
	}
	var state core.State
	if err := decodeState(runSemantics, initial, &state); err != nil {
		return core.State{}, err
	}
	return state, nil
}

func (s *Store) historyRows(ctx context.Context, tx *sql.Tx, ids []historyRunID, before HistoryCursor, line int, end int64, limit int) ([]LogEntry, HistoryCursor, error) {
	var out []LogEntry
	transitions := make(map[string][]LogEntry, len(ids))
	var frontiers []int64
	add := func(rows []LogEntry) {
		out = append(out, rows...)
		if len(rows) > limit {
			frontiers = append(frontiers, rows[len(rows)-1].Pos)
		}
	}
	for _, id := range ids {
		var rows []LogEntry
		var err error
		if seq, ok := before.Seq[id.id]; ok {
			rows, err = s.logTransitionsQuery(ctx, tx, historyTransitionsSeqSQL, id.id, seq, limit+1)
		} else {
			rows, err = s.logTransitionsQuery(ctx, tx, historyTransitionsRunSQL, id.id, end, limit+1)
		}
		if err != nil {
			return nil, HistoryCursor{}, err
		}
		transitions[id.id] = rows
		add(rows)
	}
	inputs, err := s.logInputsQuery(ctx, tx, historyInputsLineSQL, line, end, limit+1)
	if err != nil {
		return nil, HistoryCursor{}, err
	}
	add(inputs)
	var latestLine int
	if err := tx.QueryRowContext(ctx, latestLineSQL).Scan(&latestLine); err == nil && latestLine == line {
		queued, err := s.logInputsQuery(ctx, tx, historyQueuedLineSQL, end, limit+1)
		if err != nil {
			return nil, HistoryCursor{}, err
		}
		add(queued)
	}
	withdrawals, err := s.historyWithdrawals(ctx, tx, line, end, limit)
	if err != nil {
		return nil, HistoryCursor{}, err
	}
	add(withdrawals)
	cutoff := int64(0)
	for _, frontier := range frontiers {
		if frontier > cutoff {
			cutoff = frontier
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Pos > out[j].Pos })
	var page []LogEntry
	older := HistoryCursor{}
	if cutoff > 0 {
		for _, row := range out {
			if row.Pos >= cutoff {
				page = append(page, row)
			}
		}
	} else {
		page = out
	}
	if len(page) > limit {
		next := page[limit].Pos
		page = page[:limit]
		older.Pos = next + 1
	} else if cutoff > 0 {
		older.Pos = cutoff
	}
	for i, j := 0, len(page)-1; i < j; i, j = i+1, j-1 {
		page[i], page[j] = page[j], page[i]
	}
	if older.Pos > 0 {
		older.Seq = make(map[string]int64, len(ids))
		for _, id := range ids {
			older.Seq[id.id] = transitionSeqBefore(transitions[id.id], older.Pos)
		}
	}
	return page, older, nil
}

func (s *Store) historyWithdrawals(ctx context.Context, tx *sql.Tx, line int, end int64, limit int) ([]LogEntry, error) {
	rows, err := tx.QueryContext(ctx, historyWithdrawalsSQL, line, end, limit+1, withdrawalNames+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LogEntry
	for rows.Next() {
		var pos, line int64
		var joined string
		if err := rows.Scan(&pos, &line, &joined); err != nil {
			return nil, err
		}
		names, cut := withdrawalNamesOf(joined)
		out = append(out, LogEntry{Pos: pos, InputLine: int(line), Withdrawn: names, WithdrawnTotal: len(names), WithdrawnCut: cut})
	}
	return out, rows.Err()
}

func transitionSeqBefore(rows []LogEntry, before int64) int64 {
	for _, row := range rows {
		if row.Pos < before {
			return row.Transition.Seq
		}
	}
	return 0
}

// RunLine returns the recorded line of a run.
func (s *Store) RunLine(ctx context.Context, id string) (int, error) {
	var line int
	if err := s.db.QueryRowContext(ctx, runLineSQL, id).Scan(&line); err != nil {
		return 0, missing(err, "run")
	}
	return line, nil
}

// LatestSummary returns the latest run's clipped identity and state without
// decoding its run record or goal.
func (s *Store) LatestSummary(ctx context.Context) (HistoryRun, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return HistoryRun{}, err
	}
	defer tx.Rollback()
	var id string
	var n, line int
	if err := tx.QueryRowContext(ctx, `SELECT id, n, line FROM runs ORDER BY n DESC LIMIT 1`).Scan(&id, &n, &line); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return HistoryRun{}, harness.ErrNotFound
		}
		return HistoryRun{}, err
	}
	run, err := scanSummaryRun(tx.QueryRowContext(ctx, summaryRunSQL, id))
	if err != nil {
		return HistoryRun{}, err
	}
	run.Config, err = s.summaryConfig(ctx, tx, id)
	if err != nil {
		return HistoryRun{}, err
	}
	state, err := s.historyState(ctx, tx, run.ID)
	if err != nil {
		return HistoryRun{}, err
	}
	if err := tx.Commit(); err != nil {
		return HistoryRun{}, err
	}
	return HistoryRun{Run: run, State: state, Ordinal: n, Line: line}, nil
}

// LatestRun is the latest run with its state and recorded line.
func (s *Store) LatestRun(ctx context.Context) (HistoryRun, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return HistoryRun{}, err
	}
	defer tx.Rollback()
	var last int64
	if err := s.stmt(ctx, tx, lastPosSQL).QueryRowContext(ctx).Scan(&last); err != nil {
		return HistoryRun{}, err
	}
	run, err := s.run(s.stmt(ctx, tx, lastRunSQL).QueryRowContext(ctx))
	if errors.Is(err, harness.ErrNotFound) {
		return HistoryRun{}, harness.ErrNotFound
	}
	if err != nil {
		return HistoryRun{}, err
	}
	var ordinal, line int
	if err := tx.QueryRowContext(ctx, `SELECT n, line FROM runs WHERE id = ?`, run.ID).Scan(&ordinal, &line); err != nil {
		return HistoryRun{}, missing(err, "run")
	}
	state, err := s.historyState(ctx, tx, run.ID)
	if err != nil {
		return HistoryRun{}, err
	}
	if err := tx.Commit(); err != nil {
		return HistoryRun{}, err
	}
	return HistoryRun{Run: run, State: state, Ordinal: ordinal, Line: line}, nil
}
