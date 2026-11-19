package obs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// ErrBusy is returned by Receive when the number of received but not yet
// processed messages has reached the configured capacity. It maps to a
// retryable HTTP 503 response.
var ErrBusy = errors.New("receive capacity reached")

// ErrNotFound is returned by query methods for unknown objects.
var ErrNotFound = errors.New("not found")

const schema = `
CREATE TABLE IF NOT EXISTS messages (
  seq         INTEGER PRIMARY KEY,            -- receive position, gapless from 1
  gateway     TEXT NOT NULL,
  msg_id      TEXT NOT NULL,
  type        TEXT NOT NULL,                  -- 'reading' | 'progress'
  telescope   TEXT NOT NULL DEFAULT '',
  ts          INTEGER NOT NULL DEFAULT 0,     -- reading timestamp (unix s)
  drift       REAL NOT NULL DEFAULT 0,        -- drift amplitude (arcsec)
  progress    INTEGER NOT NULL DEFAULT 0,     -- reported progress (unix s)
  received_at INTEGER NOT NULL,
  processed   INTEGER NOT NULL DEFAULT 0,
  disposition TEXT NOT NULL DEFAULT '',
  UNIQUE (gateway, msg_id)                    -- idempotent retry per gateway
);
CREATE INDEX IF NOT EXISTS idx_messages_processed ON messages(processed, seq);

CREATE TABLE IF NOT EXISTS window_aggregates ( -- current aggregate per window
  telescope    TEXT NOT NULL,
  window_start INTEGER NOT NULL,
  count        INTEGER NOT NULL,
  max_drift    REAL NOT NULL,
  PRIMARY KEY (telescope, window_start)
);

CREATE TABLE IF NOT EXISTS window_versions (   -- published version history
  telescope    TEXT NOT NULL,
  window_start INTEGER NOT NULL,
  version      INTEGER NOT NULL,
  count        INTEGER NOT NULL,
  max_drift    REAL NOT NULL,
  trigger_seq  INTEGER NOT NULL,              -- receive position that triggered it
  PRIMARY KEY (telescope, window_start, version)
);
CREATE INDEX IF NOT EXISTS idx_window_versions_trigger ON window_versions(trigger_seq);

CREATE TABLE IF NOT EXISTS gateway_progress (
  gateway  TEXT PRIMARY KEY,
  progress INTEGER NOT NULL                   -- max reported so far
);

CREATE TABLE IF NOT EXISTS replays (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  prefix_seq INTEGER NOT NULL,                -- log prefix this replay covers
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS replay_versions (  -- replay output, separate from live
  replay_id    INTEGER NOT NULL,
  telescope    TEXT NOT NULL,
  window_start INTEGER NOT NULL,
  version      INTEGER NOT NULL,
  count        INTEGER NOT NULL,
  max_drift    REAL NOT NULL,
  trigger_seq  INTEGER NOT NULL,
  PRIMARY KEY (replay_id, telescope, window_start, version)
);
`

// Store owns the SQLite database and the live processing machine.
//
// A single database connection serializes all reads and writes, which keeps
// every multi-statement operation trivially consistent; multi-statement
// queries additionally run inside read-only transactions so each response
// reflects one log snapshot.
type Store struct {
	db       *sql.DB
	gateways []string
	capacity int64

	mu      sync.Mutex // guards machine
	machine *Machine

	notify chan struct{} // wakes the processor
}

// Open opens (or creates) the database at path and restores the processing
// state, so a restarted process continues exactly where the log says it was.
func Open(ctx context.Context, path string, gateways []string, capacity int64) (*Store, error) {
	if len(gateways) == 0 {
		return nil, errors.New("obs: at least one gateway must be configured")
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("obs: create schema: %w", err)
	}
	s := &Store{db: db, gateways: gateways, capacity: capacity, notify: make(chan struct{}, 1)}
	if err := s.reloadMachine(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// Gateways returns the configured fixed gateway set.
func (s *Store) Gateways() []string { return s.gateways }

// reloadMachine rebuilds the in-memory machine from durable state. Called at
// startup and after a failed processing transaction.
func (s *Store) reloadMachine(ctx context.Context) error {
	m := NewMachine(s.gateways)
	rows, err := s.db.QueryContext(ctx, `SELECT telescope, window_start, count, max_drift FROM window_aggregates`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var k WinKey
		var a Agg
		if err := rows.Scan(&k.Telescope, &k.WindowStart, &a.Count, &a.MaxDrift); err != nil {
			rows.Close()
			return err
		}
		m.Aggs[k] = a
	}
	rows.Close()
	rows, err = s.db.QueryContext(ctx, `SELECT telescope, window_start, MAX(version) FROM window_versions GROUP BY telescope, window_start`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var k WinKey
		var v int
		if err := rows.Scan(&k.Telescope, &k.WindowStart, &v); err != nil {
			rows.Close()
			return err
		}
		m.Vers[k] = v
	}
	rows.Close()
	rows, err = s.db.QueryContext(ctx, `SELECT gateway, progress FROM gateway_progress`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var g string
		var p int64
		if err := rows.Scan(&g, &p); err != nil {
			rows.Close()
			return err
		}
		m.Progs[g] = p
	}
	rows.Close()
	s.mu.Lock()
	s.machine = m
	s.mu.Unlock()
	return nil
}

// Receive persists a message and returns its receive position. Retrying a
// message with the same (gateway, msg_id) returns the original position.
// When the number of unprocessed messages reaches the capacity, new messages
// are rejected with ErrBusy (duplicates are still answered).
func (s *Store) Receive(ctx context.Context, m Message) (seq int64, duplicate bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()

	err = tx.QueryRowContext(ctx,
		`SELECT seq FROM messages WHERE gateway = ? AND msg_id = ?`, m.Gateway, m.MsgID).Scan(&seq)
	switch {
	case err == nil:
		return seq, true, tx.Commit()
	case !errors.Is(err, sql.ErrNoRows):
		return 0, false, err
	}

	var pending int64
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages WHERE processed = 0`).Scan(&pending); err != nil {
		return 0, false, err
	}
	if pending >= s.capacity {
		return 0, false, ErrBusy
	}

	err = tx.QueryRowContext(ctx, `
		INSERT INTO messages (seq, gateway, msg_id, type, telescope, ts, drift, progress, received_at)
		VALUES ((SELECT COALESCE(MAX(seq), 0) + 1 FROM messages), ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING seq`,
		m.Gateway, m.MsgID, m.Type, m.Telescope, m.Ts, m.Drift, m.Progress, time.Now().Unix(),
	).Scan(&seq)
	if err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	select {
	case s.notify <- struct{}{}:
	default:
	}
	return seq, false, nil
}

// RunProcessor processes messages in receive order until ctx is cancelled.
// It is woken by Receive and also polls once a second as a safety net.
func (s *Store) RunProcessor(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.notify:
		case <-ticker.C:
		}
		for {
			done, err := s.processOne(ctx)
			if err != nil {
				log.Printf("obs: processing error: %v (resyncing state)", err)
				if rerr := s.reloadMachine(ctx); rerr != nil {
					log.Printf("obs: resync failed: %v", rerr)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(200 * time.Millisecond):
				}
				break
			}
			if done {
				break
			}
		}
	}
}

// ProcessAll synchronously drains all unprocessed messages and returns how
// many were processed. Used by tests; the background processor uses
// processOne directly.
func (s *Store) ProcessAll(ctx context.Context) (int, error) {
	n := 0
	for {
		done, err := s.processOne(ctx)
		if err != nil {
			return n, err
		}
		if done {
			return n, nil
		}
		n++
	}
}

// processOne processes the oldest unprocessed message in one transaction.
// It returns done=true when no unprocessed message remains.
func (s *Store) processOne(ctx context.Context) (done bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var m Message
	err = tx.QueryRowContext(ctx, `
		SELECT seq, gateway, msg_id, type, telescope, ts, drift, progress
		FROM messages WHERE processed = 0 ORDER BY seq LIMIT 1`,
	).Scan(&m.Seq, &m.Gateway, &m.MsgID, &m.Type, &m.Telescope, &m.Ts, &m.Drift, &m.Progress)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}

	s.mu.Lock()
	delta := s.machine.Process(m)
	s.mu.Unlock()

	for k, a := range delta.Aggs {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO window_aggregates (telescope, window_start, count, max_drift)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (telescope, window_start)
			DO UPDATE SET count = excluded.count, max_drift = excluded.max_drift`,
			k.Telescope, k.WindowStart, a.Count, a.MaxDrift); err != nil {
			return false, err
		}
	}
	for _, v := range delta.Versions {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO window_versions (telescope, window_start, version, count, max_drift, trigger_seq)
			VALUES (?, ?, ?, ?, ?, ?)`,
			v.Telescope, v.WindowStart, v.Version, v.Count, v.MaxDrift, v.TriggerSeq); err != nil {
			return false, err
		}
	}
	for g, p := range delta.Progs {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO gateway_progress (gateway, progress) VALUES (?, ?)
			ON CONFLICT (gateway) DO UPDATE SET progress = excluded.progress`,
			g, p); err != nil {
			return false, err
		}
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE messages SET processed = 1, disposition = ? WHERE seq = ?`,
		delta.Disposition, m.Seq); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return false, nil
}

// --- queries ---------------------------------------------------------------

// ProgressInfo is the processing-progress snapshot.
type ProgressInfo struct {
	Received  int64             `json:"received"`
	Processed int64             `json:"processed"`
	Pending   int64             `json:"pending"`
	Capacity  int64             `json:"capacity"`
	Watermark *int64            `json:"watermark"` // null until every gateway reported
	Gateways  map[string]*int64 `json:"gateways"`  // null per gateway until it reported
}

// Progress returns a consistent snapshot of the processing progress.
func (s *Store) Progress(ctx context.Context) (ProgressInfo, error) {
	info := ProgressInfo{Capacity: s.capacity, Gateways: map[string]*int64{}}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return info, err
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(processed), 0) FROM messages`,
	).Scan(&info.Received, &info.Processed); err != nil {
		return info, err
	}
	info.Pending = info.Received - info.Processed
	progs, err := gatewayProgress(ctx, tx)
	if err != nil {
		return info, err
	}
	for _, g := range s.gateways {
		if p, ok := progs[g]; ok {
			p := p
			info.Gateways[g] = &p
		} else {
			info.Gateways[g] = nil
		}
	}
	if wm, ok := watermarkOf(s.gateways, progs); ok {
		info.Watermark = &wm
	}
	return info, nil
}

func gatewayProgress(ctx context.Context, tx *sql.Tx) (map[string]int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT gateway, progress FROM gateway_progress`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	progs := map[string]int64{}
	for rows.Next() {
		var g string
		var p int64
		if err := rows.Scan(&g, &p); err != nil {
			return nil, err
		}
		progs[g] = p
	}
	return progs, rows.Err()
}

// WindowSummary is the current state of one window.
type WindowSummary struct {
	Telescope   string  `json:"telescope"`
	WindowStart int64   `json:"window_start"`
	WindowEnd   int64   `json:"window_end"`
	Count       int     `json:"count"`
	MaxDrift    float64 `json:"max_drift"`
	Versions    int     `json:"versions"`  // number of published versions
	Finalized   bool    `json:"finalized"` // watermark >= window_end + 60s
}

// WindowDetail is a window plus its full version history.
type WindowDetail struct {
	WindowSummary
	History []VersionRow `json:"history"`
}

// ListWindows returns all windows that have readings, optionally filtered by
// telescope, in chronological order.
func (s *Store) ListWindows(ctx context.Context, telescope string) ([]WindowSummary, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	progs, err := gatewayProgress(ctx, tx)
	if err != nil {
		return nil, err
	}
	wm, wmOK := watermarkOf(s.gateways, progs)

	q := `SELECT a.telescope, a.window_start, a.count, a.max_drift,
		(SELECT COUNT(*) FROM window_versions v
		  WHERE v.telescope = a.telescope AND v.window_start = a.window_start)
		FROM window_aggregates a`
	var args []any
	if telescope != "" {
		q += ` WHERE a.telescope = ?`
		args = append(args, telescope)
	}
	q += ` ORDER BY a.window_start, a.telescope`
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []WindowSummary{}
	for rows.Next() {
		var w WindowSummary
		if err := rows.Scan(&w.Telescope, &w.WindowStart, &w.Count, &w.MaxDrift, &w.Versions); err != nil {
			return nil, err
		}
		w.WindowEnd = w.WindowStart + 60
		w.Finalized = wmOK && wm >= w.WindowStart+120
		out = append(out, w)
	}
	return out, rows.Err()
}

// GetWindow returns one window with its version history.
func (s *Store) GetWindow(ctx context.Context, telescope string, windowStart int64) (WindowDetail, error) {
	var d WindowDetail
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return d, err
	}
	defer tx.Rollback()
	progs, err := gatewayProgress(ctx, tx)
	if err != nil {
		return d, err
	}
	wm, wmOK := watermarkOf(s.gateways, progs)

	err = tx.QueryRowContext(ctx,
		`SELECT count, max_drift FROM window_aggregates WHERE telescope = ? AND window_start = ?`,
		telescope, windowStart).Scan(&d.Count, &d.MaxDrift)
	if errors.Is(err, sql.ErrNoRows) {
		return d, ErrNotFound
	}
	if err != nil {
		return d, err
	}
	d.Telescope = telescope
	d.WindowStart = windowStart
	d.WindowEnd = windowStart + 60
	d.Finalized = wmOK && wm >= windowStart+120
	d.History = []VersionRow{}
	rows, err := tx.QueryContext(ctx, `
		SELECT version, count, max_drift, trigger_seq FROM window_versions
		WHERE telescope = ? AND window_start = ? ORDER BY version`, telescope, windowStart)
	if err != nil {
		return d, err
	}
	defer rows.Close()
	for rows.Next() {
		var v VersionRow
		if err := rows.Scan(&v.Version, &v.Count, &v.MaxDrift, &v.TriggerSeq); err != nil {
			return d, err
		}
		v.Telescope = telescope
		v.WindowStart = windowStart
		d.History = append(d.History, v)
		d.Versions++
	}
	return d, rows.Err()
}

// AllVersions returns every published live version, ordered deterministically.
func (s *Store) AllVersions(ctx context.Context) ([]VersionRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT telescope, window_start, version, count, max_drift, trigger_seq
		FROM window_versions ORDER BY window_start, telescope, version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []VersionRow{}
	for rows.Next() {
		var v VersionRow
		if err := rows.Scan(&v.Telescope, &v.WindowStart, &v.Version, &v.Count, &v.MaxDrift, &v.TriggerSeq); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// MessageDetail is a receive-log entry plus the versions it triggered.
type MessageDetail struct {
	Seq         int64        `json:"seq"`
	Gateway     string       `json:"gateway"`
	MsgID       string       `json:"msg_id"`
	Type        string       `json:"type"`
	Telescope   string       `json:"telescope,omitempty"`
	Ts          int64        `json:"ts,omitempty"`
	Drift       float64      `json:"drift,omitempty"`
	Progress    int64        `json:"progress,omitempty"`
	ReceivedAt  int64        `json:"received_at"`
	Processed   bool         `json:"processed"`
	Disposition string       `json:"disposition,omitempty"`
	Triggered   []VersionRow `json:"triggered_versions,omitempty"`
}

// GetMessage returns one receive-log entry and the versions its processing
// triggered.
func (s *Store) GetMessage(ctx context.Context, seq int64) (MessageDetail, error) {
	var d MessageDetail
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return d, err
	}
	defer tx.Rollback()
	var processed int
	err = tx.QueryRowContext(ctx, `
		SELECT seq, gateway, msg_id, type, telescope, ts, drift, progress, received_at, processed, disposition
		FROM messages WHERE seq = ?`, seq,
	).Scan(&d.Seq, &d.Gateway, &d.MsgID, &d.Type, &d.Telescope, &d.Ts, &d.Drift, &d.Progress,
		&d.ReceivedAt, &processed, &d.Disposition)
	if errors.Is(err, sql.ErrNoRows) {
		return d, ErrNotFound
	}
	if err != nil {
		return d, err
	}
	d.Processed = processed != 0
	d.Triggered = []VersionRow{}
	rows, err := tx.QueryContext(ctx, `
		SELECT telescope, window_start, version, count, max_drift, trigger_seq
		FROM window_versions WHERE trigger_seq = ? ORDER BY window_start, telescope, version`, seq)
	if err != nil {
		return d, err
	}
	defer rows.Close()
	for rows.Next() {
		var v VersionRow
		if err := rows.Scan(&v.Telescope, &v.WindowStart, &v.Version, &v.Count, &v.MaxDrift, &v.TriggerSeq); err != nil {
			return d, err
		}
		d.Triggered = append(d.Triggered, v)
	}
	return d, rows.Err()
}

// --- replay ----------------------------------------------------------------

// ReplaySummary describes one replay run.
type ReplaySummary struct {
	ID           int64 `json:"replay_id"`
	PrefixSeq    int64 `json:"prefix_seq"`
	CreatedAt    int64 `json:"created_at"`
	WindowCount  int   `json:"window_count"`
	VersionCount int   `json:"version_count"`
}

// ReplayDetail is a replay plus its full per-window version history.
type ReplayDetail struct {
	ReplaySummary
	Versions []VersionRow `json:"versions"`
}

// CreateReplay replays the receive-log prefix that is confirmed at call time
// (including messages not yet processed by the live pipeline) through a fresh
// machine, and stores the resulting versions under a new replay id. It never
// touches live tables, so live receiving, processing and querying continue
// unaffected. The same prefix always yields the same versions and triggers.
func (s *Store) CreateReplay(ctx context.Context) (int64, error) {
	// 1. Read a consistent snapshot of the confirmed log prefix.
	rtx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, err
	}
	var prefix int64
	if err := rtx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) FROM messages`).Scan(&prefix); err != nil {
		rtx.Rollback()
		return 0, err
	}
	rows, err := rtx.QueryContext(ctx, `
		SELECT seq, gateway, msg_id, type, telescope, ts, drift, progress
		FROM messages WHERE seq <= ? ORDER BY seq`, prefix)
	if err != nil {
		rtx.Rollback()
		return 0, err
	}
	var msgs []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.Seq, &m.Gateway, &m.MsgID, &m.Type, &m.Telescope, &m.Ts, &m.Drift, &m.Progress); err != nil {
			rows.Close()
			rtx.Rollback()
			return 0, err
		}
		msgs = append(msgs, m)
	}
	rows.Close()
	if err := rtx.Rollback(); err != nil {
		return 0, err
	}

	// 2. Deterministically recompute the window versions from the prefix.
	m := NewMachine(s.gateways)
	var versions []VersionRow
	for _, msg := range msgs {
		d := m.Process(msg)
		versions = append(versions, d.Versions...)
	}

	// 3. Persist the replay result in one short transaction.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx,
		`INSERT INTO replays (prefix_seq, created_at) VALUES (?, ?)`, prefix, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	for _, v := range versions {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO replay_versions (replay_id, telescope, window_start, version, count, max_drift, trigger_seq)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			id, v.Telescope, v.WindowStart, v.Version, v.Count, v.MaxDrift, v.TriggerSeq); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

// GetReplay returns one replay with its versions.
func (s *Store) GetReplay(ctx context.Context, id int64) (ReplayDetail, error) {
	var d ReplayDetail
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return d, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(ctx,
		`SELECT id, prefix_seq, created_at FROM replays WHERE id = ?`, id,
	).Scan(&d.ID, &d.PrefixSeq, &d.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return d, ErrNotFound
	}
	if err != nil {
		return d, err
	}
	d.Versions = []VersionRow{}
	rows, err := tx.QueryContext(ctx, `
		SELECT telescope, window_start, version, count, max_drift, trigger_seq
		FROM replay_versions WHERE replay_id = ? ORDER BY window_start, telescope, version`, id)
	if err != nil {
		return d, err
	}
	defer rows.Close()
	windows := map[WinKey]bool{}
	for rows.Next() {
		var v VersionRow
		if err := rows.Scan(&v.Telescope, &v.WindowStart, &v.Version, &v.Count, &v.MaxDrift, &v.TriggerSeq); err != nil {
			return d, err
		}
		d.Versions = append(d.Versions, v)
		if !windows[WinKey{v.Telescope, v.WindowStart}] {
			windows[WinKey{v.Telescope, v.WindowStart}] = true
			d.WindowCount++
		}
	}
	d.VersionCount = len(d.Versions)
	return d, rows.Err()
}

// ListReplays returns all replays, oldest first.
func (s *Store) ListReplays(ctx context.Context) ([]ReplaySummary, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT r.id, r.prefix_seq, r.created_at,
			(SELECT COUNT(*) FROM replay_versions v WHERE v.replay_id = r.id),
			(SELECT COUNT(DISTINCT telescope || char(31) || window_start)
			   FROM replay_versions v WHERE v.replay_id = r.id)
		FROM replays r ORDER BY r.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ReplaySummary{}
	for rows.Next() {
		var r ReplaySummary
		if err := rows.Scan(&r.ID, &r.PrefixSeq, &r.CreatedAt, &r.VersionCount, &r.WindowCount); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
