# liveconfig Struct Tag Reference

liveconfig discovers which fields it manages through struct tags. Two tags are used:

| Tag    | Purpose                                               |
|--------|-------------------------------------------------------|
| `dyn`  | Classifies the field and sets its reload strategy.    |
| `json` | Provides the store key name (snake_case recommended). |
| `desc` | Human-readable description surfaced in the catalog.   |

The `json` tag is the same tag you already use for JSON serialization. liveconfig re-uses it to derive the store key, so there is no duplication.

---

## The `dyn` tag

### `dyn:"live"`

The value is applied to the atomic snapshot immediately. Every call to `Provider.Get()` after the change returns the new value. No further action is required from the application.

**When to use:** Timeouts, URLs, feature flags, rate limits, log levels, Jira epics, pagination sizes - anything where the new value takes effect on the next request or invocation.

```go
type Config struct {
    JiraDefaultEpic string        `json:"jira_default_epic" dyn:"live" desc:"Jira epic for patch tickets"`
    RequestTimeout  time.Duration `json:"request_timeout"   dyn:"live" desc:"Outbound HTTP timeout"`
    MaxRetries      int           `json:"max_retries"       dyn:"live" desc:"HTTP retry limit"`
}
```

---

### `dyn:"recreate-on-change"`

Like `live`, the snapshot is updated atomically. In addition, a `ChangeEvent` is sent to every channel registered via `Provider.Subscribe()`. The application must listen on that channel and rebuild the resource that depends on the field.

**When to use:** Long-lived objects that cannot simply re-read config on the next call - cron workers, connection pools, HTTP clients that are constructed once, SMTP relays, queue consumers.

```go
type Config struct {
    CronSchedule string `json:"cron_schedule" dyn:"recreate-on-change" desc:"Patch automation cron expression"`
    WorkerCount  int    `json:"worker_count"  dyn:"recreate-on-change" desc:"Number of background workers"`
}
```

```go
subCtx, cancelSub := context.WithCancel(ctx)
defer cancelSub()
go func() {
    for range provider.Subscribe(subCtx) {
        restartCronWorker(provider.Get().CronSchedule)
    }
}()
```

---

### `dyn:"restart-required"`

The value is persisted in the store so it survives the next restart and takes effect after a redeployment. No live reload is performed - the current process continues to use the old value.

**When to use:** Database DSN, TLS certificate paths, port numbers, feature flags that require a full initialization path to apply.

The UI should display a warning badge next to fields with this strategy.

```go
type Config struct {
    DatabaseDSN string `json:"database_dsn" dyn:"restart-required" desc:"Primary database connection string"`
    ListenPort  int    `json:"listen_port"  dyn:"restart-required" desc:"HTTP listen port"`
}
```

---

### `dyn:"bootstrap"`

The field is loaded once at startup from environment variables or command-line flags and is **never** overridden by the store. liveconfig excludes bootstrap fields from the catalog and silently ignores any store value for them.

**When to use:** Secrets injected via environment variables, service identity (e.g. pod name, region), and anything that cannot change without a full restart and re-initialization.

```go
type Config struct {
    DBPassword string `json:"db_password" dyn:"bootstrap"` // injected from K8s secret
    PodName    string `json:"pod_name"    dyn:"bootstrap"` // downward API
}
```

---

### `dyn:"secret"`

The field is completely excluded from the catalog. liveconfig will never read from or write to the store for this field. The field remains invisible to the management API and the UI.

**When to use:** Fields that contain sensitive values that must never be persisted in the database (API keys, OAuth tokens, private keys). Use `dyn:"bootstrap"` when you want the field to be excluded from live updates but it is not a secret. Use `dyn:"secret"` when the value must also never appear in the audit log or change history.

```go
type Config struct {
    JiraAPIKey string `json:"jira_api_key" dyn:"secret"`
}
```

---

### No `dyn` tag

Fields without a `dyn` tag are ignored by liveconfig entirely. They are not added to the catalog and receive no store overrides. The base value set at startup is used permanently.

### Unrecognised `dyn` value

A typo in the tag (e.g. `dyn:"life"` instead of `dyn:"live"`) causes `liveconfig.New` to fail at startup with a clear error pointing at the offending field. The five accepted values are: `live`, `recreate-on-change`, `restart-required`, `bootstrap`, `secret`. This fail-fast behaviour prevents a misconfigured field from silently being excluded from the catalog.

---

## The `desc` tag

The `desc` tag provides a human-readable description of the field. It is surfaced in:

- `Provider.Catalog()` - for building a config management API or UI.
- Documentation generation tools.

```go
type Config struct {
    Timeout time.Duration `json:"timeout" dyn:"live" desc:"HTTP client timeout for outbound Jira requests"`
}
```

Descriptions are optional. Fields without a `desc` tag have an empty `Description` in `FieldDescriptor`.

---

## Nested structs

liveconfig traverses nested structs recursively. The store key is built by joining the `json` tag names of each level with `.`.

```go
type Config struct {
    Jira JiraConfig `json:"jira"`
}

type JiraConfig struct {
    DefaultEpic string `json:"default_epic" dyn:"live" desc:"Default Jira epic (changes quarterly)"`
    ProjectKey  string `json:"project_key"  dyn:"live" desc:"Jira project key"`
    APIKey      string `json:"api_key"      dyn:"secret"`
}
```

This produces store keys `jira.default_epic` and `jira.project_key`. The `api_key` field is excluded because it is tagged `dyn:"secret"`.

If the parent struct field is tagged `dyn:"secret"`, the entire subtree is excluded.

---

## Supported field types

| Go type           | Example store value       |
|-------------------|---------------------------|
| `string`          | `my-epic-KEY`             |
| `bool`            | `true` / `false` / `1` / `0` |
| `int`, `int64`    | `42`                      |
| `uint`, `uint64`  | `100`                     |
| `float64`         | `3.14`                    |
| `time.Duration`   | `30s` / `5m` / `1h30m`   |
| `[]string`        | `a,b,c` or `["a","b","c"]` |

Any other type (`time.Time`, `map`, struct slices, complex numbers, etc.) on a field tagged with a dynamic strategy will cause `liveconfig.New` to fail at startup with a clear error. This avoids surprises at the first store update where parsing would fail at runtime.
