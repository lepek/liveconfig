# liveconfig

liveconfig adds hot-reload support to any Go config struct. You keep your existing config loading (go-conf, envconfig, viper, plain `os.Getenv`, etc.) and wrap it with a `Provider` that applies database-backed overrides on top, then keeps them current in real time via Postgres `LISTEN`/`NOTIFY`.

Changes made through a management API or UI reach every running instance of your service in seconds - no restart or redeployment required.

---

## Key features

- **Generic** - works with any Go struct. No code generation needed.
- **Lock-free reads** - `Provider.Get()` uses `atomic.Pointer`, safe for hot paths.
- **Real-time** - changes propagate via Postgres `LISTEN`/`NOTIFY`, or through a polling helper for backends that don't support push (MySQL, SQLite, etc.).
- **Audit log** - every change is recorded with who made it and what the previous value was.
- **Granular reload strategies** - per-field control over whether a change is invisible, requires a resource rebuild, or requires a restart.
- **Safety net** - a periodic full re-read catches any changes missed during a connection drop.
- **Two stores** - `pgstore` for production, `memstore` for tests and local dev.

---

## Modules

| Module path                                          | Description                         |
|------------------------------------------------------|-------------------------------------|
| `github.com/lepek/liveconfig`                | Core: Provider, Store interface, PollingWatcher, in-memory MemStore (under `memstore`) |
| `github.com/lepek/liveconfig/pgstore`        | Postgres store (production)         |

`memstore` is a sub-package of the core module, so the core has zero external dependencies. The `pgstore` module pulls in `pgx/v5` and is therefore packaged separately.

---

## Installation

Requires **Go 1.25+**.

```bash
go get github.com/lepek/liveconfig@v0.1.0

# only if you need the Postgres-backed store:
go get github.com/lepek/liveconfig/pgstore@v0.1.0
```

The core module has zero external dependencies. Because `testcontainers` is only a test dependency of `pgstore`, projects that import `pgstore` pull in just `pgx/v5` and its dependencies.

---

## Quick start

### 1. Tag your config struct

```go
import "time"

type Config struct {
    // bootstrap: loaded once at startup, never overridden by the store.
    DBHost string `json:"db_host" dyn:"bootstrap"`

    // live: applied to the atomic snapshot immediately.
    RequestTimeout  time.Duration `json:"request_timeout"   dyn:"live" desc:"Outbound HTTP timeout"`
    JiraDefaultEpic string        `json:"jira_default_epic" dyn:"live" desc:"Default Jira epic (changes quarterly)"`
    MaxRetries      int           `json:"max_retries"       dyn:"live" desc:"HTTP retry limit"`

    // recreate-on-change: snapshot updated AND a ChangeEvent is sent to Subscribe channels.
    CronSchedule string `json:"cron_schedule" dyn:"recreate-on-change" desc:"Patch automation schedule"`

    // restart-required: persisted for next restart, no live reload.
    ListenPort int `json:"listen_port" dyn:"restart-required" desc:"HTTP listen port"`

    // secret: excluded from the catalog and never stored.
    JiraAPIKey string `json:"jira_api_key" dyn:"secret"`
}
```

See [docs/ANNOTATIONS.md](docs/ANNOTATIONS.md) for the full tag reference.

### 2. Load base config at startup

Load your config however you currently do it - nothing changes here.

```go
base := loadFromEnvOrFlags() // your existing code
```

### 3. Create a store and migrate

```go
import (
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/lepek/liveconfig/pgstore"
)

pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
// handle err

store := pgstore.New(pool,
    pgstore.WithTable("liveconfig_values"),         // optional, this is the default
    pgstore.WithAuditTable("liveconfig_audit"),     // optional, this is the default
    pgstore.WithNotifyChannel("liveconfig_changed"), // optional, this is the default
)
if err := store.Migrate(ctx); err != nil { ... }
```

### 4. Create a Provider

```go
import "github.com/lepek/liveconfig"

provider, err := liveconfig.New(ctx, base, store,
    liveconfig.WithNamespace("myservice"),
    liveconfig.WithLogger(logger),
    liveconfig.WithRefreshInterval(5 * time.Minute),
)
if err != nil { ... }
defer provider.Close()
```

### 5. Read config

```go
cfg := provider.Get()  // lock-free, safe to call on every request
doSomething(cfg.RequestTimeout)
```

`Get` returns a value copy of the snapshot. For typical config structs (a few dozen fields, no large slices/maps) the copy cost is in the tens of nanoseconds and irrelevant. If your config struct embeds very large slices/maps, prefer reading individual fields rather than holding the whole snapshot.

Call `Get` again for every read where you want the latest value: a value returned by an earlier `Get` is a point-in-time copy and will not auto-refresh.

### 6. React to recreate-on-change fields

```go
subCtx, cancelSub := context.WithCancel(ctx)
defer cancelSub()
go func() {
    for range provider.Subscribe(subCtx) {
        restartCronWorker(provider.Get().CronSchedule)
    }
}()
```

`Subscribe(ctx)` returns a channel that is closed when ctx is cancelled. Tie the context to the lifetime of the worker that consumes events; the Provider cleans up the channel when ctx ends.

### 7. Force a refresh (ops endpoint)

```go
http.HandleFunc("/admin/refresh", func(w http.ResponseWriter, r *http.Request) {
    provider.Refresh()
    w.WriteHeader(http.StatusAccepted)
})
```

`Refresh()` is fire-and-forget and coalesces with any already-pending refresh.

### 8. Health and observability

```go
stats := provider.Stats()
// stats.LastRefresh, stats.RefreshCount, stats.LastError
```

---

## Updating config values

Use your management API, or insert directly:

```sql
INSERT INTO liveconfig_values (namespace, key, value, changed_by, updated_at)
VALUES ('myservice', 'jira_default_epic', 'FY26Q3-EPIC', 'alice', NOW())
ON CONFLICT (namespace, key) DO UPDATE
    SET value = EXCLUDED.value,
        changed_by = EXCLUDED.changed_by,
        updated_at = EXCLUDED.updated_at;
```

The running application picks up the new value within milliseconds via NOTIFY.

### Multi-key updates

`Store.Set` operates on one key at a time. Two related keys updated back-to-back produce two `ChangeEvent`s and a brief snapshot state where one is new and the other is still old. For most use cases this is invisible. If you need atomic multi-key updates, see the discussion in [docs/DEVELOPER_GUIDE.md](docs/DEVELOPER_GUIDE.md#multi-key-updates).

---

## Testing

Use the `memstore` package to avoid needing a real database in tests:

```go
import (
    "github.com/lepek/liveconfig"
    "github.com/lepek/liveconfig/memstore"
)

store := memstore.New()
provider, _ := liveconfig.New(ctx, myConfig, store,
    liveconfig.WithNamespace("test"),
)

_ = store.Set(ctx, liveconfig.SetEntry{
    Namespace: "test",
    Key:       "request_timeout",
    Value:     "10s",
    ChangedBy: "test",
})

// provider.Get().RequestTimeout is now 10s
```

---

## Provider options

| Option                               | Default          | Description                                            |
|--------------------------------------|------------------|--------------------------------------------------------|
| `WithNamespace(string)`              | `"default"`      | Store namespace, typically the service name.           |
| `WithLogger(*slog.Logger)`           | `slog.Default()` | Structured logger for info/warning/error messages.     |
| `WithRefreshInterval(time.Duration)` | `5m`             | How often to perform a full re-read as a safety net.   |
| `WithSubscriberBuffer(int)`          | `16`             | Buffer size of channels returned by Subscribe.         |

Passing nil/zero values to any option falls back to the default.

---

## PGStore options

| Option                             | Default              | Description                                          |
|------------------------------------|----------------------|------------------------------------------------------|
| `WithTable(string)`                | `"liveconfig_values"`  | Table name for config values.                        |
| `WithAuditTable(string)`           | `"liveconfig_audit"`   | Table name for the audit log.                        |
| `WithNotifyChannel(string)`        | `"liveconfig_changed"` | Postgres NOTIFY channel name used for LISTEN/NOTIFY. |
| `WithLogger(*slog.Logger)`         | `slog.Default()`     | Structured logger.                                   |
| `WithMaxBackoff(time.Duration)`    | `60s`                | Maximum wait between LISTEN reconnect attempts.      |

Table and channel names must match `^[a-zA-Z_][a-zA-Z0-9_]*$`. `New` panics on invalid names because these identifiers are interpolated directly into SQL (Postgres does not accept identifiers as bind parameters).

---

## Catalog API

`Provider.Catalog()` returns metadata for every managed field. Use it to build a config management API:

```go
for _, field := range provider.Catalog() {
    fmt.Printf("key=%-30s type=%-15s strategy=%s\n",
        field.Key, field.TypeName, field.ReloadStrategy)
}
```

---

## Architecture overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Application                                                    │
│                                                                 │
│  provider.Get()       atomic.Pointer[T].Load()    *Config       │
│                                                                 │
│  provider.Subscribe() chan ChangeEvent  rebuild resource        │
│  provider.Refresh()   coalesced trigger                         │
│  provider.Stats()     LastRefresh / RefreshCount / LastError    │
└──────────────────────────┬──────────────────────────────────────┘
                           │  one eventLoop goroutine reads:
                           │   - store change events
                           │   - safety ticks
                           │   - Refresh() triggers
┌──────────────────────────▼──────────────────────────────────────┐
│  Postgres (pgstore)                                             │
│                                                                 │
│  liveconfig_values  ─┐                                            │
│  liveconfig_audit    │  NOTIFY  LISTEN goroutine  ChangeEvent     │
│  trigger + fn     ─┘                                            │
└─────────────────────────────────────────────────────────────────┘
```

For details on the internal design and how to implement a custom Store see [docs/DEVELOPER_GUIDE.md](docs/DEVELOPER_GUIDE.md).
