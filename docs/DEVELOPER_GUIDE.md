# Developer Guide

This guide explains how the liveconfig library is structured, how to work on it locally, how to run the tests, and how to integrate it into a host application.

---

## Repository layout

```
liveconfig/
  go.mod              Core module (github.com/lepek/liveconfig)
  go.work             Go workspace - links core and pgstore for local dev
  errors.go           Sentinel errors
  store.go            Store interface, ChangeEvent, SetEntry, AuditEntry, HistoryOptions
  catalog.go          Reflection-based struct walker; parseFieldValue; applyOverrides
  options.go          Option funcs for Provider (WithNamespace, WithLogger, ...)
  provider.go         Generic Provider[T]; single eventLoop goroutine
  pollingwatcher.go   Polling-based Watch helper for stores without push notifications
  catalog_test.go     Unit tests for catalog and field value parsing
  provider_test.go    Integration tests for Provider using memstore
  pollingwatcher_test.go  Tests for PollingWatcher
  docs/
    ANNOTATIONS.md    Full struct tag reference
    DEVELOPER_GUIDE.md  This file
  memstore/           Subpackage of the core module
    store.go          In-memory Store implementation (tests and local dev)
    store_test.go     Tests for MemStore
  pgstore/            Separate module (pulls pgx/v5)
    go.mod            module github.com/lepek/liveconfig/pgstore
    migrations.go     DDL builder (tables, index, notify function, trigger)
    store.go          Postgres Store implementation using pgx/v5
    store_test.go     Integration tests using testcontainers-go
```

`memstore` lives inside the core module rather than as a separate module so consumers of `liveconfig` (which always need a test store) do not have to depend on a second module to get one. The core module has zero external runtime dependencies.

`pgstore` is a separate module because it pulls in `pgx/v5` and several Docker-related test dependencies; users of MemStore or a custom Store should not be forced to download them.

---

## Go workspace

The two modules share a `go.work` file so you can develop and test across them without publishing to a package registry.

```bash
cd ~/Work/liveconfig
go build ./...         # core + memstore
go test -race ./...    # core + memstore
go test -race ./pgstore/...  # requires Docker for testcontainers
```

---

## How Provider works

1. **Startup** - `New[T]()` calls `buildCatalog[T]()` via reflection to collect all dynamic fields. It then calls `store.List()` to load existing overrides and builds the first atomic snapshot via `applyOverrides()`. Catalog construction validates `dyn` tag values and leaf field types; any unknown tag or unsupported type causes `New` to return an error immediately.

2. **Single eventLoop goroutine** - One goroutine consumes three sources in a single `select`:
   - store change events (from `store.Watch`)
   - safety-tick refreshes (every `WithRefreshInterval`, default 5 min)
   - manual `Provider.Refresh()` triggers (non-blocking, coalescing)

   Because there is only one goroutine calling `refresh`, refreshes are naturally serialised. A slow safety refresh can never overwrite a newer change-driven snapshot.

3. **Refresh** - Each refresh calls `store.List()` and rebuilds the entire snapshot via `applyOverrides`. A full re-read is used (instead of a single-key patch) to guarantee consistency when multiple changes arrive quickly.

4. **Subscriber fan-out** - After the snapshot is rebuilt, the event loop checks whether the changed key belongs to a field tagged `dyn:"recreate-on-change"`. If so, it sends the `ChangeEvent` to all channels registered via `Provider.Subscribe(ctx)`.

5. **Atomic pointer** - The snapshot is stored in `atomic.Pointer[T]`. `Provider.Get()` calls `Load()` and dereferences once to return a value copy. No lock is held by readers; the copy prevents the shared snapshot from being mutated through a returned pointer.

6. **Stats** - `Provider.Stats()` exposes `LastRefresh`, `RefreshCount`, and `LastError` via atomic counters. Use these in health checks.

---

## How PGStore works

- **Set** opens a transaction, upserts the row in `liveconfig_values`, inserts an audit row in `liveconfig_audit`, and commits. The Postgres trigger on `liveconfig_values` fires `pg_notify` automatically after the commit.
- **Delete** opens a transaction, deletes the row, inserts an audit row attributed to the passed `changedBy`, and manually calls `pg_notify` (because the trigger only fires on INSERT/UPDATE), then commits.
- **Watch** opens a dedicated `pgx.Conn` (not from the pool), issues `LISTEN <channel>`, and starts a goroutine that calls `conn.WaitForNotification()` in a loop. Notifications carry the namespace; events from other namespaces are filtered client-side. On error the connection is reconnected with exponential backoff (up to `WithMaxBackoff`, default 60s).
- **Migrate** is transactional. The trigger name embeds the table name so multiple PGStore instances on different tables in the same database do not stomp on each other's triggers.

---

## Multi-key updates

`Store.Set` operates on a single key. If you call it twice in a row for two related keys, you get two `ChangeEvent`s and the Provider runs two refreshes; for a brief moment the snapshot reflects one new key and one old key.

For most settings (timeouts, feature flags, the Jira epic, etc.) this is invisible: each individual key is independent. If you do have two keys that must change together, the workaround today is:

1. Make the change in the order that fails safe: change the "trailing" key first, then the "leading" one.
2. Or model the pair as a single composite key whose value is a JSON object that you parse in `applyOverrides`.

If a real need for atomic multi-key updates appears, the planned addition is `Store.SetBatch(ctx, []SetEntry) error`. In pgstore this would commit all upserts in a single transaction and emit a single combined NOTIFY; in PollingWatcher the diff would naturally batch multiple keys into one tick.

---

## Adding a new Store implementation

Implement the `liveconfig.Store` interface defined in [store.go](../store.go). The required methods are:

| Method      | Responsibility                                     |
|-------------|----------------------------------------------------|
| `Get`       | Return the raw string value for a key.             |
| `List`      | Return all overrides for a namespace.              |
| `Set`       | Persist a new value and emit a ChangeEvent.        |
| `Delete`    | Remove an override and emit a ChangeEvent attributed to changedBy. |
| `Watch`     | Return a channel of ChangeEvents for the given namespace. |
| `History`   | Return the audit trail, newest-first, paginated.   |
| `Close`     | Release resources.                                 |

See [memstore/store.go](../memstore/store.go) for the simplest reference implementation.

---

## Implementing Watch without a push mechanism (MySQL, SQLite, Redis, ...)

Not every database supports push notifications equivalent to Postgres `LISTEN`/`NOTIFY`. For those backends, use the `PollingWatcher` helper bundled in the core module. It calls `Store.List` on a fixed interval, diffs the result against the previous call, and emits a `ChangeEvent` for every key that was added, changed, or deleted.

The poll interval drives the latency of config changes reaching the application. A 5-second interval is a sensible default for most cases. The `Provider` safety refresh loop (`WithRefreshInterval`) is independent of `PollingWatcher` - you can set it longer (e.g. 1 minute) and rely on `PollingWatcher` for sub-minute reactivity.

### Skeleton MySQL store

Because `PollingWatcher.Watch` has the exact same signature as `Store.Watch`, you can embed `*PollingWatcher` directly and it satisfies the interface with no manual delegation:

```go
package mysqlstore

import (
    "context"
    "database/sql"
    "fmt"
    "log/slog"
    "time"

    "github.com/lepek/liveconfig"
)

type MySQLStore struct {
    *liveconfig.PollingWatcher
    db         *sql.DB
    table      string
    auditTable string
}

func New(db *sql.DB, pollInterval time.Duration) *MySQLStore {
    s := &MySQLStore{
        db:         db,
        table:      "liveconfig_values",
        auditTable: "liveconfig_audit",
    }
    s.PollingWatcher = liveconfig.NewPollingWatcher(s, pollInterval, slog.Default())
    return s
}

func (s *MySQLStore) Get(ctx context.Context, namespace, key string) (string, bool, error) {
    var value string
    err := s.db.QueryRowContext(ctx,
        fmt.Sprintf("SELECT value FROM %s WHERE namespace=? AND `key`=?", s.table),
        namespace, key,
    ).Scan(&value)
    if err == sql.ErrNoRows {
        return "", false, nil
    }
    return value, err == nil, err
}

func (s *MySQLStore) List(ctx context.Context, namespace string) (map[string]string, error) {
    rows, err := s.db.QueryContext(ctx,
        fmt.Sprintf("SELECT `key`, value FROM %s WHERE namespace=?", s.table), namespace)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    result := make(map[string]string)
    for rows.Next() {
        var k, v string
        if err := rows.Scan(&k, &v); err != nil {
            return nil, err
        }
        result[k] = v
    }
    return result, rows.Err()
}

func (s *MySQLStore) Set(ctx context.Context, entry liveconfig.SetEntry) error {
    // upsert + audit insert (implementation omitted for brevity)
    return nil
}

func (s *MySQLStore) Delete(ctx context.Context, namespace, key, changedBy string) error {
    // delete + audit insert (implementation omitted for brevity)
    return nil
}

// No Watch method declared: the embedded *PollingWatcher provides it.

func (s *MySQLStore) History(ctx context.Context, namespace, key string, opts liveconfig.HistoryOptions) ([]liveconfig.AuditEntry, error) {
    // query audit table (implementation omitted for brevity)
    return nil, nil
}

func (s *MySQLStore) Close(_ context.Context) error {
    return s.db.Close()
}
```

### Notes on the polling approach

- **Latency:** changes are visible to the `Provider` within one poll interval, not in milliseconds. For most config changes (feature flags, Jira epics, cron schedules) this is acceptable.
- **`ChangedBy` in ChangeEvent:** `PollingWatcher` sets it to `"poll"` because there is no actor information available at diff time. The real actor is still recorded in your audit table by `Set`/`Delete`.
- **Initial seed:** `PollingWatcher` waits for the first successful `List` to record a baseline silently, then starts diffing from the next tick onwards. If `List` fails on the first try, the watcher keeps retrying on each tick until it succeeds, with no events emitted in the meantime. This prevents a false event storm when the store recovers from a startup failure.
- **Spurious events:** If `List` returns different results across two polls for the same key (e.g. due to a flaky read), a spurious event is emitted. This causes an extra snapshot rebuild in the `Provider`, which is harmless but slightly wasteful.

---

## Running the tests

### Core (no external dependencies)

```bash
cd ~/Work/liveconfig
go test ./...
```

This runs the core tests and the `memstore` tests.

### pgstore (requires Docker)

```bash
cd ~/Work/liveconfig
go test ./pgstore/...
```

The pgstore tests use `testcontainers-go` to spin up a real Postgres 16 container. The container is started and stopped automatically per test. Each test gets its own table/channel triple, derived from `t.Name()`, so tests can run in parallel without colliding. If Docker is not available, the tests are skipped with a clear message.

### All tests with race detector

```bash
go test -race ./...
go test -race ./pgstore/...
```

---

## Adding a new dynamic field

1. Add the field to the config struct with the appropriate `dyn`, `json`, and `desc` tags. See [ANNOTATIONS.md](ANNOTATIONS.md) for the full reference.
2. Choose the right strategy: `live` for most scalars, `recreate-on-change` for long-lived objects, `restart-required` for connection-level settings.
3. If the field uses `recreate-on-change`, add a subscriber in your application:
   ```go
   go func() {
       for range provider.Subscribe(ctx) {
           // rebuild the resource that depends on the changed field
       }
   }()
   ```
4. Run `go test ./...` to verify the catalog picks up the new field. Build will fail at startup, not later, if you typo the `dyn` tag or use an unsupported field type.

---

## Logging

liveconfig uses the standard `log/slog` package. All log lines include:

- `namespace` - the store namespace (service name).
- `key` - the config key, when applicable.
- `error` - the error string for warnings and errors.

To silence liveconfig logs, pass a no-op handler:

```go
liveconfig.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
```

---

## Key design decisions

**Why a full re-read on each change event, not a single-key patch?**
A full `store.List()` is cheap (single SQL query) and eliminates edge cases where rapid successive changes arrive out of order or a change event is dropped. Consistency is more important than the marginal saving of a targeted update.

**Why `atomic.Pointer[T]` instead of a `sync.RWMutex`?**
`atomic.Pointer.Load()` is lock-free and safe for hot paths. A `RWMutex` would require every read to acquire a lock, adding latency and contention in high-throughput services.

**Why does `Get` return a value, not a pointer?**
Returning a value prevents callers from mutating the shared snapshot through the returned pointer. For typical config structs the copy cost is negligible. If the struct contains very large slices/maps that should not be copied, read individual fields instead of holding the whole snapshot.

**Why a single eventLoop goroutine?**
Two separate goroutines (one for change events, one for safety ticks) could call `refresh` concurrently. A slow `List` from one of them could finish later than a newer one and install a stale snapshot. Funnelling all three trigger sources (change events, safety ticks, manual Refresh) through one `select` removes the race without needing any locks around `refresh`.

**Why a dedicated connection for LISTEN?**
The `pgxpool` connection pool does not support `LISTEN`/`NOTIFY` because connections are returned to the pool between uses, losing the listener registration. A dedicated, long-lived `pgx.Conn` is the correct approach.

**Why is the base value a value type (not a pointer)?**
Each call to `applyOverrides` copies the base struct and returns a new value. This means the base is never mutated, and `Get()` always returns a value derived from a stable snapshot.
