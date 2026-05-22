// Package pgstore provides a Postgres-backed implementation of liveconfig.Store.
//
// It uses pgx/v5 for all database operations and Postgres LISTEN/NOTIFY for
// real-time change propagation. A dedicated connection (separate from the pool)
// is held open for LISTEN; it reconnects automatically with exponential backoff
// if the connection drops.
//
// # Usage
//
//	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
//
//	store := pgstore.New(pool)
//	if err := store.Migrate(ctx); err != nil { ... }
//
//	provider, err := liveconfig.New(ctx, myConfig, store,
//	    liveconfig.WithNamespace("myservice"),
//	)
//
// # Tables
//
// Migrate creates two tables and one trigger (names are configurable):
//   - liveconfig_values  — one row per (namespace, key)
//   - liveconfig_audit   — append-only history of every change
//
// The trigger on liveconfig_values fires pg_notify after every INSERT or UPDATE,
// which the listener goroutine picks up and converts to a liveconfig.ChangeEvent.
// DELETE operations issue pg_notify manually inside the transaction.
package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lepek/liveconfig"
)

const (
	defaultTable         = "liveconfig_values"
	defaultAuditTable    = "liveconfig_audit"
	defaultNotifyChannel = "liveconfig_changed"
	initialBackoff       = 2 * time.Second
	defaultMaxBackoff    = 60 * time.Second
)

// identRe restricts table and channel names to safe SQL identifiers. The
// values are interpolated directly into DDL/DML strings because Postgres does
// not allow identifiers as bind parameters, so they MUST be operator-controlled
// and must not come from user input. This regex catches the common mistakes
// (spaces, quotes, semicolons, slashes) and is intentionally narrow.
var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validateIdent panics with a clear message if name fails the identifier check.
// Configuration mistakes here are programmer errors, not runtime input errors,
// so a panic is the appropriate signal.
func validateIdent(kind, name string) {
	if !identRe.MatchString(name) {
		panic(fmt.Sprintf("pgstore: invalid %s name %q (must match %s)", kind, name, identRe.String()))
	}
}

// notifyPayload is the JSON structure emitted by the Postgres trigger and by
// the manual pg_notify call in Delete.
type notifyPayload struct {
	Namespace string    `json:"namespace"`
	Key       string    `json:"key"`
	OldValue  string    `json:"old_value"`
	NewValue  string    `json:"new_value"`
	ChangedBy string    `json:"changed_by"`
	ChangedAt time.Time `json:"changed_at"`
}

// PGStore is a Postgres-backed liveconfig.Store.
type PGStore struct {
	pool          *pgxpool.Pool
	table         string
	auditTable    string
	notifyChannel string
	logger        *slog.Logger
	maxBackoff    time.Duration
}

// Option configures a PGStore.
type Option func(*PGStore)

// WithTable sets the table name for config values.
// Default: "liveconfig_values".
func WithTable(name string) Option {
	return func(s *PGStore) { s.table = name }
}

// WithAuditTable sets the table name for the audit log.
// Default: "liveconfig_audit".
func WithAuditTable(name string) Option {
	return func(s *PGStore) { s.auditTable = name }
}

// WithNotifyChannel sets the Postgres NOTIFY channel name used by LISTEN/NOTIFY.
// All service instances sharing the same database must use the same name.
// Default: "liveconfig_changed".
func WithNotifyChannel(name string) Option {
	return func(s *PGStore) { s.notifyChannel = name }
}

// WithLogger sets the structured logger.
// Default: slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(s *PGStore) { s.logger = l }
}

// WithMaxBackoff sets the maximum wait time between LISTEN reconnect attempts.
// Default: 60s.
func WithMaxBackoff(d time.Duration) Option {
	return func(s *PGStore) { s.maxBackoff = d }
}

// New creates a PGStore backed by pool. The pool is used for all CRUD and audit
// operations. A separate pgx.Conn is opened (from the same config) for LISTEN.
//
// New panics if table, audit table, or notify channel names contain anything
// other than letters, digits, and underscores. These names are interpolated
// directly into SQL because Postgres does not accept identifiers as bind
// parameters; they MUST come from operator-controlled configuration and never
// from user input.
func New(pool *pgxpool.Pool, opts ...Option) *PGStore {
	s := &PGStore{
		pool:          pool,
		table:         defaultTable,
		auditTable:    defaultAuditTable,
		notifyChannel: defaultNotifyChannel,
		logger:        slog.Default(),
		maxBackoff:    defaultMaxBackoff,
	}
	for _, o := range opts {
		o(s)
	}
	if s.logger == nil {
		s.logger = slog.Default()
	}
	if s.maxBackoff <= 0 {
		s.maxBackoff = defaultMaxBackoff
	}
	validateIdent("table", s.table)
	validateIdent("audit table", s.auditTable)
	validateIdent("notify channel", s.notifyChannel)
	return s
}

// Migrate creates the liveconfig tables, index, trigger function, and trigger
// if they do not already exist. Safe to call multiple times.
// Call this once at application startup before using the store.
//
// All DDL runs inside a single transaction so that a failure partway through
// (e.g. trigger creation) leaves no half-built schema behind.
func (s *PGStore) Migrate(ctx context.Context) error {
	ddl := buildDDL(s.table, s.auditTable, s.notifyChannel)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgstore: migrate begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("pgstore: migrate exec: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgstore: migrate commit: %w", err)
	}

	s.logger.Info("pgstore: migration complete",
		slog.String("table", s.table),
		slog.String("audit_table", s.auditTable),
		slog.String("notify_channel", s.notifyChannel),
	)
	return nil
}

// Get returns the current override value for namespace/key.
// Returns ("", false, nil) when no override exists.
func (s *PGStore) Get(ctx context.Context, namespace, key string) (string, bool, error) {
	var value string
	err := s.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT value FROM %s WHERE namespace=$1 AND key=$2`, s.table),
		namespace, key,
	).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("pgstore: get %s/%s: %w", namespace, key, err)
	}
	return value, true, nil
}

// List returns all key-value overrides for namespace.
func (s *PGStore) List(ctx context.Context, namespace string) (map[string]string, error) {
	rows, err := s.pool.Query(ctx,
		fmt.Sprintf(`SELECT key, value FROM %s WHERE namespace=$1`, s.table),
		namespace,
	)
	if err != nil {
		return nil, fmt.Errorf("pgstore: list %s: %w", namespace, err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("pgstore: list scan: %w", err)
		}
		result[k] = v
	}
	return result, rows.Err()
}

// Set upserts the value for namespace/key inside a transaction and records an
// audit entry. The Postgres trigger fires NOTIFY automatically on commit.
func (s *PGStore) Set(ctx context.Context, entry liveconfig.SetEntry) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgstore: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Capture old value for the audit record.
	var oldValue string
	err = tx.QueryRow(ctx,
		fmt.Sprintf(`SELECT value FROM %s WHERE namespace=$1 AND key=$2`, s.table),
		entry.Namespace, entry.Key,
	).Scan(&oldValue)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("pgstore: set read old: %w", err)
	}

	now := time.Now().UTC()

	if _, err = tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (namespace, key, value, changed_by, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (namespace, key) DO UPDATE
			SET value=$3, changed_by=$4, updated_at=$5
	`, s.table), entry.Namespace, entry.Key, entry.Value, entry.ChangedBy, now); err != nil {
		return fmt.Errorf("pgstore: set upsert: %w", err)
	}

	if _, err = tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (namespace, key, old_value, new_value, changed_by, changed_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, s.auditTable), entry.Namespace, entry.Key, oldValue, entry.Value, entry.ChangedBy, now); err != nil {
		return fmt.Errorf("pgstore: set audit: %w", err)
	}

	// The trigger on liveconfig_values fires pg_notify after commit automatically.
	return tx.Commit(ctx)
}

// Delete removes the override for namespace/key inside a transaction, records an
// audit entry attributed to changedBy, and manually emits a NOTIFY (the trigger
// only fires on INSERT/UPDATE). Deleting a non-existent key is a no-op.
func (s *PGStore) Delete(ctx context.Context, namespace, key, changedBy string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgstore: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var oldValue string
	err = tx.QueryRow(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE namespace=$1 AND key=$2 RETURNING value`, s.table),
		namespace, key,
	).Scan(&oldValue)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx) // nothing to delete
	}
	if err != nil {
		return fmt.Errorf("pgstore: delete: %w", err)
	}

	now := time.Now().UTC()

	if _, err = tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (namespace, key, old_value, new_value, changed_by, changed_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, s.auditTable), namespace, key, oldValue, "", changedBy, now); err != nil {
		return fmt.Errorf("pgstore: delete audit: %w", err)
	}

	// Emit NOTIFY manually so listeners react to the deletion.
	payload, _ := json.Marshal(notifyPayload{
		Namespace: namespace,
		Key:       key,
		OldValue:  oldValue,
		NewValue:  "",
		ChangedBy: changedBy,
		ChangedAt: now,
	})
	if _, err = tx.Exec(ctx, fmt.Sprintf(`SELECT pg_notify('%s', $1)`, s.notifyChannel), string(payload)); err != nil {
		return fmt.Errorf("pgstore: delete notify: %w", err)
	}

	return tx.Commit(ctx)
}

// Watch opens a dedicated pgx.Conn from the same config as the pool and issues
// a LISTEN command. It returns a channel that receives a ChangeEvent for every
// notification that belongs to the given namespace. Events for other namespaces
// are filtered client-side. The connection is re-established automatically with
// exponential backoff when it drops.
//
// The channel is closed when ctx is cancelled.
//
// Watch is intended to be called once per Provider. Each call opens a new
// connection; misusing it (e.g. calling it repeatedly without cancelling the
// context) leaks one connection per call.
func (s *PGStore) Watch(ctx context.Context, namespace string) (<-chan liveconfig.ChangeEvent, error) {
	conn, err := pgx.ConnectConfig(ctx, s.pool.Config().ConnConfig)
	if err != nil {
		return nil, fmt.Errorf("pgstore: open listen conn: %w", err)
	}
	if _, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", s.notifyChannel)); err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("pgstore: LISTEN: %w", err)
	}

	ch := make(chan liveconfig.ChangeEvent, 64)
	s.logger.Info("pgstore: LISTEN established",
		slog.String("notify_channel", s.notifyChannel),
		slog.String("namespace", namespace),
	)
	go s.listenLoop(ctx, conn, namespace, ch)
	return ch, nil
}

// listenLoop blocks on WaitForNotification and forwards events that match the
// given namespace to ch. On error it closes the old connection and reconnects
// with exponential backoff.
func (s *PGStore) listenLoop(ctx context.Context, conn *pgx.Conn, namespace string, ch chan liveconfig.ChangeEvent) {
	defer close(ch)
	backoff := initialBackoff

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			conn.Close(context.Background())

			if ctx.Err() != nil {
				return // normal shutdown
			}

			s.logger.Warn("pgstore: LISTEN connection lost, will reconnect",
				slog.String("notify_channel", s.notifyChannel),
				slog.Duration("backoff", backoff),
				slog.String("error", err.Error()),
			)

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, s.maxBackoff)

			conn, err = pgx.ConnectConfig(ctx, s.pool.Config().ConnConfig)
			if err != nil {
				s.logger.Error("pgstore: reconnect failed",
					slog.String("error", err.Error()),
				)
				continue
			}
			if _, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", s.notifyChannel)); err != nil {
				s.logger.Error("pgstore: LISTEN after reconnect failed",
					slog.String("error", err.Error()),
				)
				conn.Close(ctx)
				continue
			}
			backoff = initialBackoff
			s.logger.Info("pgstore: LISTEN reconnected", slog.String("notify_channel", s.notifyChannel))
			continue
		}

		backoff = initialBackoff // reset on successful receive

		var p notifyPayload
		if err := json.Unmarshal([]byte(notification.Payload), &p); err != nil {
			s.logger.Warn("pgstore: failed to parse notification payload",
				slog.String("raw", notification.Payload),
				slog.String("error", err.Error()),
			)
			continue
		}

		// Filter by namespace client-side. The trigger sends NOTIFY for every
		// row in liveconfig_values, regardless of namespace.
		if p.Namespace != namespace {
			continue
		}

		event := liveconfig.ChangeEvent{
			Namespace: p.Namespace,
			Key:       p.Key,
			OldValue:  p.OldValue,
			NewValue:  p.NewValue,
			ChangedBy: p.ChangedBy,
			ChangedAt: p.ChangedAt,
		}

		select {
		case ch <- event:
		case <-ctx.Done():
			conn.Close(context.Background())
			return
		}
	}
}

// History returns the audit trail for namespace/key ordered newest-first.
func (s *PGStore) History(ctx context.Context, namespace, key string, opts liveconfig.HistoryOptions) ([]liveconfig.AuditEntry, error) {
	opts = opts.Normalised()

	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT id, namespace, key, old_value, new_value, changed_by, changed_at
		FROM %s
		WHERE namespace=$1 AND key=$2
		ORDER BY id DESC
		LIMIT $3 OFFSET $4
	`, s.auditTable), namespace, key, opts.Limit, opts.Offset)
	if err != nil {
		return nil, fmt.Errorf("pgstore: history %s/%s: %w", namespace, key, err)
	}
	defer rows.Close()

	var result []liveconfig.AuditEntry
	for rows.Next() {
		var e liveconfig.AuditEntry
		if err := rows.Scan(&e.ID, &e.Namespace, &e.Key, &e.OldValue, &e.NewValue, &e.ChangedBy, &e.ChangedAt); err != nil {
			return nil, fmt.Errorf("pgstore: history scan: %w", err)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

// Close is a no-op. The pgxpool.Pool passed to New is managed by the caller.
// Calling Close more than once is safe.
func (s *PGStore) Close(_ context.Context) error { return nil }
