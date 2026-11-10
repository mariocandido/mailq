// Package store is the durable queue. Postgres is used as the queue itself:
// workers claim batches with FOR UPDATE SKIP LOCKED and hold a time-limited
// lease, so a crashed worker's messages are picked up again automatically.
package store

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

// Message statuses.
const (
	StatusQueued     = "queued"
	StatusSending    = "sending"
	StatusDeferred   = "deferred"
	StatusDelivered  = "delivered"
	StatusBounced    = "bounced"
	StatusSuppressed = "suppressed"
)

// ErrNotFound is returned when a message does not exist.
var ErrNotFound = errors.New("not found")

// Message is one email to one recipient.
type Message struct {
	ID             string    `json:"id"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	From           string    `json:"from"`
	To             string    `json:"to"`
	Domain         string    `json:"domain"`
	Subject        string    `json:"subject"`
	Body           string    `json:"-"`
	Status         string    `json:"status"`
	Attempts       int       `json:"attempts"`
	NextAttemptAt  time.Time `json:"next_attempt_at"`
	LastError      string    `json:"last_error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// NewMessage is the input to Enqueue.
type NewMessage struct {
	From, To, Domain, Subject, Body string
}

// Attempt records one conversation with a remote server.
type Attempt struct {
	At         time.Time `json:"at"`
	Host       string    `json:"host"`
	Code       int       `json:"code"`
	Enhanced   string    `json:"enhanced,omitempty"`
	Response   string    `json:"response"`
	Class      string    `json:"class"`
	Reason     string    `json:"reason"`
	DurationMS int64     `json:"duration_ms"`
}

// Outcome is what the worker decided after an attempt.
type Outcome struct {
	Status         string
	NextAttemptAt  time.Time
	LastError      string
	Attempt        Attempt
	SuppressReason string // non-empty adds the recipient to the suppression list
}

// Suppression is a recipient that must not be mailed again.
type Suppression struct {
	Recipient string    `json:"recipient"`
	Reason    string    `json:"reason"`
	CreatedAt time.Time `json:"created_at"`
}

// Store wraps the connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects, waiting up to ~30s for the database to come up.
func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for range 30 {
		if lastErr = pool.Ping(ctx); lastErr == nil {
			return &Store{pool: pool}, nil
		}
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	pool.Close()
	return nil, fmt.Errorf("database not reachable: %w", lastErr)
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Pool exposes the pool (used by tests).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Migrate creates the schema if needed.
func (s *Store) Migrate(ctx context.Context) error {
	for _, stmt := range strings.Split(schema, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

const msgCols = `id::text, coalesce(idempotency_key, ''), sender, recipient, domain, subject, body,
	status, attempts, next_attempt_at, coalesce(last_error, ''), created_at, updated_at`

func scanMessage(row pgx.Row) (Message, error) {
	var m Message
	err := row.Scan(&m.ID, &m.IdempotencyKey, &m.From, &m.To, &m.Domain, &m.Subject, &m.Body,
		&m.Status, &m.Attempts, &m.NextAttemptAt, &m.LastError, &m.CreatedAt, &m.UpdatedAt)
	return m, err
}

// Enqueue stores one message per recipient. With a non-empty key, repeating
// the call returns the messages created the first time instead of new ones.
// Suppressed recipients are stored with status "suppressed" so the caller can
// see why they were not sent.
func (s *Store) Enqueue(ctx context.Context, key string, msgs []NewMessage) ([]Message, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var keyArg any
	if key != "" {
		keyArg = key
	}
	out := make([]Message, 0, len(msgs))
	for _, nm := range msgs {
		status, lastErr := StatusQueued, ""
		var reason string
		err := tx.QueryRow(ctx, `SELECT reason FROM suppressions WHERE recipient = $1`, nm.To).Scan(&reason)
		switch {
		case err == nil:
			status, lastErr = StatusSuppressed, "recipient is suppressed: "+reason
		case !errors.Is(err, pgx.ErrNoRows):
			return nil, err
		}

		m, err := scanMessage(tx.QueryRow(ctx, `
			INSERT INTO messages (idempotency_key, sender, recipient, domain, subject, body, status, last_error)
			VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''))
			ON CONFLICT (idempotency_key, recipient) WHERE idempotency_key IS NOT NULL DO NOTHING
			RETURNING `+msgCols,
			keyArg, nm.From, nm.To, nm.Domain, nm.Subject, nm.Body, status, lastErr))
		if errors.Is(err, pgx.ErrNoRows) { // idempotent replay
			m, err = scanMessage(tx.QueryRow(ctx,
				`SELECT `+msgCols+` FROM messages WHERE idempotency_key = $1 AND recipient = $2`, key, nm.To))
		}
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, tx.Commit(ctx)
}

// Claim leases up to limit messages that are due. Messages whose lease
// expired (worker crashed mid-delivery) are reclaimed too.
//
// Messages for the domains in skip are left in the queue: the worker already
// holds enough of them in memory, waiting for the domain's throttle.
func (s *Store) Claim(ctx context.Context, limit int, lease time.Duration, skip []string) ([]Message, error) {
	if skip == nil {
		skip = []string{} // NULL would make `<> ALL` exclude every row
	}
	rows, err := s.pool.Query(ctx, `
		UPDATE messages
		SET status = 'sending', locked_until = now() + make_interval(secs => $2), updated_at = now()
		WHERE id IN (
			SELECT id FROM messages
			WHERE ((status IN ('queued', 'deferred') AND next_attempt_at <= now())
			       OR (status = 'sending' AND locked_until < now()))
			  AND domain <> ALL($3::text[])
			ORDER BY next_attempt_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED)
		RETURNING `+msgCols, limit, lease.Seconds(), skip)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Message, error) { return scanMessage(r) })
}

// Reschedule gives a leased message back without counting an attempt (used
// when the domain is throttled).
func (s *Store) Reschedule(ctx context.Context, id string, at time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE messages
		SET status = CASE WHEN attempts > 0 THEN 'deferred' ELSE 'queued' END,
		    next_attempt_at = $2, locked_until = NULL, updated_at = now()
		WHERE id = $1 AND status = 'sending'`, id, at)
	return err
}

// Complete records an attempt and the resulting state in one transaction.
func (s *Store) Complete(ctx context.Context, m Message, o Outcome) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	a := o.Attempt
	if _, err := tx.Exec(ctx, `
		INSERT INTO attempts (message_id, at, host, code, enhanced, response, class, reason, duration_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		m.ID, a.At, a.Host, a.Code, a.Enhanced, a.Response, a.Class, a.Reason, a.DurationMS); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE messages
		SET status = $2, attempts = attempts + 1, next_attempt_at = $3, last_error = NULLIF($4, ''),
		    locked_until = NULL, updated_at = now()
		WHERE id = $1`, m.ID, o.Status, o.NextAttemptAt, o.LastError); err != nil {
		return err
	}
	if o.SuppressReason != "" {
		if _, err := tx.Exec(ctx, `
			INSERT INTO suppressions (recipient, reason) VALUES ($1, $2)
			ON CONFLICT (recipient) DO NOTHING`, m.To, o.SuppressReason); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// Get returns a message and its attempt history.
func (s *Store) Get(ctx context.Context, id string) (Message, []Attempt, error) {
	m, err := scanMessage(s.pool.QueryRow(ctx, `SELECT `+msgCols+` FROM messages WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, nil, ErrNotFound
	}
	if err != nil {
		return Message{}, nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT at, host, code, enhanced, response, class, reason, duration_ms
		FROM attempts WHERE message_id = $1 ORDER BY at, id`, id)
	if err != nil {
		return Message{}, nil, err
	}
	attempts, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Attempt, error) {
		var a Attempt
		err := r.Scan(&a.At, &a.Host, &a.Code, &a.Enhanced, &a.Response, &a.Class, &a.Reason, &a.DurationMS)
		return a, err
	})
	return m, attempts, err
}

// Suppressions lists suppressed recipients, newest first.
func (s *Store) Suppressions(ctx context.Context, limit int) ([]Suppression, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT recipient, reason, created_at FROM suppressions ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Suppression])
}

// DeleteSuppression removes a recipient from the suppression list.
func (s *Store) DeleteSuppression(ctx context.Context, recipient string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM suppressions WHERE recipient = $1`, recipient)
	return tag.RowsAffected() > 0, err
}

// QueueDepth counts messages per status.
func (s *Store) QueueDepth(ctx context.Context) (map[string]int, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM messages GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}
