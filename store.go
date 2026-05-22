package liveconfig

import (
	"context"
	"reflect"
	"time"
)

// ReloadStrategy describes how a running application should react when a
// config field changes in the store.
//
// The strategy is declared on each struct field via the dyn struct tag.
// See docs/ANNOTATIONS.md for the full tag reference.
type ReloadStrategy string

const (
	// ReloadStrategyLive means the new value is applied to the atomic snapshot
	// immediately and takes effect on the very next call to Provider.Get.
	// No action is required from the application.
	// Use this for timeouts, URLs, feature flags, limits, and most scalar settings.
	ReloadStrategyLive ReloadStrategy = "live"

	// ReloadStrategyRecreate means the value is applied to the snapshot AND a
	// ChangeEvent is sent to all channels returned by Provider.Subscribe.
	// The application is responsible for rebuilding any long-lived resource that
	// depends on the field (e.g. an HTTP client with a new timeout, a cron worker
	// with a new schedule expression).
	ReloadStrategyRecreate ReloadStrategy = "recreate-on-change"

	// ReloadStrategyRestart means the value is accepted and persisted so it
	// survives the next restart, but no live reload is performed.
	// The UI or API should surface a warning to the operator that a restart is needed.
	ReloadStrategyRestart ReloadStrategy = "restart-required"
)

// ChangeEvent is emitted by the store (via Watch) and by the Provider (via
// Subscribe) whenever a config key is created, updated, or deleted.
type ChangeEvent struct {
	// Namespace is the store namespace that the key belongs to.
	Namespace string

	// Key is the dot-separated field path that changed (e.g. "jira.default_epic").
	Key string

	// OldValue is the previous raw string value. Empty when the key is new.
	OldValue string

	// NewValue is the updated raw string value. Empty when the key is deleted.
	NewValue string

	// ChangedBy identifies the actor that made the change (e.g. a username).
	ChangedBy string

	// ChangedAt is the time the change was committed to the store.
	ChangedAt time.Time
}

// SetEntry carries all the information needed to persist a config change.
//
// Atomicity caveat: Store.Set operates on a single key. The library does not
// currently expose a way to update multiple keys atomically. If you set two
// related keys back to back (for example "jira.url" and "jira.project"), two
// separate ChangeEvents are emitted and the running application briefly
// observes a snapshot where one is new and the other is still old.
//
// In practice this matters very rarely. For pgstore, both operations land in
// distinct transactions inside the same millisecond. If you do need strict
// atomicity for related keys, the future addition would be
// Store.SetBatch(ctx, []SetEntry) error which a Postgres implementation can
// satisfy with a single transaction and a single combined NOTIFY.
type SetEntry struct {
	// Namespace groups related config keys, typically by service name
	// (e.g. "poseidon", "poseidon-api").
	Namespace string

	// Key is the dot-separated field path (e.g. "jira.default_epic").
	Key string

	// Value is the raw string representation of the new value.
	// The format must match the field's Go type; see docs/ANNOTATIONS.md.
	Value string

	// ChangedBy identifies the actor making the change. Required.
	ChangedBy string
}

// AuditEntry represents one record in the change history of a config key.
type AuditEntry struct {
	// ID is the monotonically increasing identifier of the audit record.
	ID int64

	// Namespace is the store namespace.
	Namespace string

	// Key is the field path that changed.
	Key string

	// OldValue is the value before the change. Empty for the first write.
	OldValue string

	// NewValue is the value after the change. Empty when the key was deleted.
	NewValue string

	// ChangedBy identifies the actor that made the change.
	ChangedBy string

	// ChangedAt is the time the change was committed.
	ChangedAt time.Time
}

// HistoryOptions controls pagination for Store.History.
type HistoryOptions struct {
	// Limit is the maximum number of entries to return. Defaults to 50.
	Limit int

	// Offset is the number of entries to skip for page-based pagination.
	Offset int
}

// defaultHistoryLimit is the value used when HistoryOptions.Limit is <= 0.
const defaultHistoryLimit = 50

// Normalised returns a copy of opts with default values applied:
// Limit becomes 50 if not positive, and Offset is clamped to 0 if negative.
// Store implementations should call this at the top of History to avoid
// duplicating the default logic in each implementation.
func (o HistoryOptions) Normalised() HistoryOptions {
	if o.Limit <= 0 {
		o.Limit = defaultHistoryLimit
	}
	if o.Offset < 0 {
		o.Offset = 0
	}
	return o
}

// Store is the persistence and notification backend for liveconfig.
// All methods must be safe for concurrent use from multiple goroutines.
//
// Responsibilities of an implementation:
//   - Persist values durably across restarts.
//   - Emit a ChangeEvent on the Watch channel after every successful Set or Delete.
//   - Record an AuditEntry for every Set and Delete.
//   - Reconnect automatically if the notification stream is interrupted.
//
// Two implementations are shipped with this module:
//   - pgstore: Postgres-backed with LISTEN/NOTIFY (github.com/lepek/liveconfig/pgstore).
//   - memstore: In-memory for tests (github.com/lepek/liveconfig/memstore).
type Store interface {
	// Get retrieves the raw string value for the given namespace and key.
	// Returns ("", false, nil) when the key has no override in the store.
	// A non-nil error indicates an infrastructure failure.
	Get(ctx context.Context, namespace, key string) (value string, found bool, err error)

	// List returns all key-value overrides for the given namespace.
	// Returns an empty (non-nil) map when no overrides exist.
	List(ctx context.Context, namespace string) (map[string]string, error)

	// Set persists a new value for the given key and records an audit entry.
	// If the key already exists its value is updated; otherwise it is created.
	// Implementations must emit a ChangeEvent via the Watch channel after a
	// successful write.
	Set(ctx context.Context, entry SetEntry) error

	// Delete removes the stored override for the given key, causing the
	// application to fall back to its compiled-in default on the next snapshot
	// refresh. Deleting a non-existent key is a no-op (returns nil).
	// Implementations must emit a ChangeEvent with NewValue="" via Watch.
	//
	// changedBy identifies the actor performing the deletion and is recorded
	// in the audit log. It is required (pass "system" for automated deletions).
	Delete(ctx context.Context, namespace, key, changedBy string) error

	// Watch returns a channel that receives a ChangeEvent for every key in the
	// given namespace that is created, updated, or deleted. Events for other
	// namespaces are not delivered on this channel. The channel is closed when
	// ctx is cancelled or Close is called.
	//
	// Implementations are free to choose their delivery mechanism:
	//
	//   Push (recommended): Postgres LISTEN/NOTIFY, Redis pub/sub, etc.
	//   The implementation must reconnect automatically with exponential backoff
	//   and emit events that arrive after reconnect.
	//
	//   Poll (fallback): For backends without push support (MySQL, SQLite, etc.),
	//   embed a *PollingWatcher and delegate Watch to it. PollingWatcher diffs
	//   successive List calls and emits ChangeEvents for each changed key.
	//   See docs/DEVELOPER_GUIDE.md for a full example.
	//
	// Callers must not close the returned channel.
	Watch(ctx context.Context, namespace string) (<-chan ChangeEvent, error)

	// History returns the audit trail for a specific key, ordered newest-first.
	History(ctx context.Context, namespace, key string, opts HistoryOptions) ([]AuditEntry, error)

	// Close releases all resources held by the store (connections, goroutines).
	// Calling Close more than once must be safe.
	Close(ctx context.Context) error
}

// FieldDescriptor describes a single config field that liveconfig manages.
// Fields tagged dyn:"bootstrap" or dyn:"secret" are excluded from the catalog.
type FieldDescriptor struct {
	// Key is the dot-separated store key derived from json tags
	// (e.g. "jira.default_epic"). This is the key used in Store operations.
	Key string

	// Description is the value of the desc struct tag, if present.
	Description string

	// TypeName is the human-readable Go type (e.g. "string", "int",
	// "time.Duration", "[]string").
	TypeName string

	// ReloadStrategy describes what the application must do after this field changes.
	ReloadStrategy ReloadStrategy

	// fieldIndices is the reflect index path from the root struct to this field.
	// Used internally by Provider to navigate and set fields via reflection.
	fieldIndices []int

	// fieldType is the Go type of this field. Used internally to parse raw
	// string values from the store back into the correct Go type.
	fieldType reflect.Type
}
