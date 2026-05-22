// Package liveconfig provides a generic, hot-reload config provider for any Go
// struct. It sits on top of whatever mechanism you already use to load config at
// startup (go-conf, viper, envconfig, plain os.Getenv, etc.) and adds:
//
//   - A database-backed override store so operators can change non-secret config
//     values without restarting the application.
//   - An atomic snapshot updated in real time via the store's change notifications
//     (e.g. Postgres LISTEN/NOTIFY).
//   - A subscriber channel for fields that require the application to rebuild a
//     resource when the value changes (recreate-on-change strategy).
//   - A periodic safety re-read that catches changes missed during connection drops.
//
// # Quick start
//
//	// 1. Tag your config struct fields.
//	type Config struct {
//	    DBHost  string        `json:"db_host"  dyn:"bootstrap"`
//	    Timeout time.Duration `json:"timeout"  dyn:"live"               desc:"HTTP client timeout"`
//	    Epic    string        `json:"jira_epic" dyn:"live"              desc:"Default Jira epic (changes quarterly)"`
//	    Cron    string        `json:"cron"     dyn:"recreate-on-change" desc:"Patch trigger schedule"`
//	}
//
//	// 2. Load base config however you like.
//	base := loadFromEnv()
//
//	// 3. Wrap it in a Provider backed by a store.
//	store := pgstore.New(pool)
//	if err := store.Migrate(ctx); err != nil { ... }
//
//	p, err := liveconfig.New(ctx, base, store,
//	    liveconfig.WithNamespace("myservice"),
//	)
//
//	// 4. Read config. Always lock-free.
//	cfg := p.Get()
//	doSomething(cfg.Timeout)
//
//	// 5. React to recreate-on-change fields.
//	subCtx, cancelSub := context.WithCancel(ctx)
//	defer cancelSub()
//	go func() {
//	    for range p.Subscribe(subCtx) {
//	        rebuildCronWorker(p.Get().Cron)
//	    }
//	}()
//
// # Struct tags
//
// See docs/ANNOTATIONS.md for the complete tag reference.
package liveconfig

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Provider holds a live atomic snapshot of a config struct T.
//
// The snapshot is rebuilt atomically whenever the backing store emits a change
// event, so all callers of Get always see the latest confirmed values without
// holding a lock.
//
// Provider is safe for concurrent use. Get is lock-free and suitable for hot paths.
type Provider[T any] struct {
	current atomic.Pointer[T]
	base    T

	catalog   []FieldDescriptor
	descByKey map[string]*FieldDescriptor // O(1) lookup by store key

	store    Store
	ns       string
	logger   *slog.Logger
	interval time.Duration
	bufSize  int

	// refreshTrigger is a buffered channel of size 1 used to coalesce
	// refresh requests. Senders use a non-blocking send; if a request is
	// already queued, additional requests collapse into the existing one.
	refreshTrigger chan struct{}

	// subsMu guards subs.
	subsMu sync.Mutex
	subs   []chan ChangeEvent

	// Stats counters. Read atomically by Stats().
	refreshCount atomic.Uint64
	lastRefresh  atomic.Int64 // unix nano of last successful refresh
	lastErrText  atomic.Pointer[string]

	cancel   context.CancelFunc
	wg       sync.WaitGroup
	closeMu  sync.Once
	closeErr error
}

// Stats is a snapshot of the Provider's internal counters, returned by
// Provider.Stats. The values are intended for monitoring and debugging.
//
// All fields are point-in-time copies; they do not change after the
// Stats value has been returned.
type Stats struct {
	// LastRefresh is the time of the last successful snapshot rebuild.
	// Zero value if no successful refresh has happened yet.
	LastRefresh time.Time

	// RefreshCount is the total number of successful refreshes since New.
	RefreshCount uint64

	// LastError is the most recent refresh error message, or empty if the
	// last refresh succeeded. Stored as a string (not an error) to keep
	// the value immutable and safe for concurrent reads.
	LastError string
}

// New creates a Provider[T] backed by the given store.
//
// base is the config struct already populated by the caller. Provider applies
// any overrides found in the store on top of base immediately, then keeps the
// snapshot current as the store emits change events.
//
// New blocks until the initial override load from the store completes, then
// starts a single background goroutine that consumes store change events,
// safety-tick refreshes, and manual Refresh() requests in one select.
//
// New returns an error if:
//   - T is not a struct (pointer-to-struct is rejected).
//   - Reflection-based catalog construction fails (e.g. typo in dyn tag,
//     unsupported leaf type).
//   - The initial override load from the store fails.
//   - store.Watch cannot be established.
func New[T any](ctx context.Context, base T, store Store, opts ...Option) (*Provider[T], error) {
	cfg := defaultProviderOptions()
	for _, o := range opts {
		o(&cfg)
	}

	// Defensive normalisation: caller may have passed nil/zero through options.
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	if cfg.refreshInterval <= 0 {
		cfg.refreshInterval = defaultRefreshInterval
	}
	if cfg.subscriberBuffer <= 0 {
		cfg.subscriberBuffer = defaultSubscriberBuf
	}
	if cfg.namespace == "" {
		cfg.namespace = defaultNamespace
	}

	catalog, err := buildCatalog[T]()
	if err != nil {
		return nil, fmt.Errorf("liveconfig: build catalog: %w", err)
	}

	descByKey := make(map[string]*FieldDescriptor, len(catalog))
	for i := range catalog {
		descByKey[catalog[i].Key] = &catalog[i]
	}

	p := &Provider[T]{
		base:           base,
		catalog:        catalog,
		descByKey:      descByKey,
		store:          store,
		ns:             cfg.namespace,
		logger:         cfg.logger,
		interval:       cfg.refreshInterval,
		bufSize:        cfg.subscriberBuffer,
		refreshTrigger: make(chan struct{}, 1),
	}

	// Apply existing store overrides to build the initial snapshot.
	if err := p.refresh(ctx); err != nil {
		return nil, fmt.Errorf("liveconfig: initial refresh: %w", err)
	}

	eventCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	watchCh, err := store.Watch(eventCtx, p.ns)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("liveconfig: store.Watch: %w", err)
	}

	p.wg.Add(1)
	go p.eventLoop(eventCtx, watchCh)

	p.logger.Info("liveconfig: provider started",
		slog.String("namespace", p.ns),
		slog.Int("managed_fields", len(catalog)),
		slog.Duration("refresh_interval", p.interval),
	)
	return p, nil
}

// Get returns a copy of the current config snapshot.
//
// The snapshot is replaced atomically on each change; Get always returns the
// latest confirmed values. Because it returns a value (not a pointer), callers
// cannot accidentally mutate the shared snapshot, and each caller gets its own
// independent copy.
//
// For most config structs the copy cost is negligible. Get is lock-free and
// safe to call on hot paths.
//
// Callers must call Get for every read where they want the latest value; the
// returned struct is a point-in-time copy and is not auto-updated.
func (p *Provider[T]) Get() T {
	return *p.current.Load()
}

// Refresh requests a snapshot rebuild from the store. It is a fire-and-forget
// non-blocking request: if a refresh is already pending it is coalesced into
// the existing one. Use this from ops endpoints that want to force an
// immediate re-read without waiting for the periodic safety refresh.
func (p *Provider[T]) Refresh() {
	p.requestRefresh()
}

// Subscribe returns a channel that receives a ChangeEvent every time a field
// tagged dyn:"recreate-on-change" is updated in the store. Use Subscribe to
// rebuild long-lived resources such as HTTP clients, cron workers, or
// connection pools that cannot simply re-read the config struct on the next
// invocation.
//
// The returned channel is closed when ctx is cancelled or when the Provider
// is closed, whichever comes first. Pass a context tied to the lifetime of
// the worker that owns the subscription so the channel is cleaned up when the
// worker shuts down.
//
// # Overflow behaviour
//
// The channel is buffered (size set by WithSubscriberBuffer, default 16). If
// the buffer is full when an event arrives, the event is dropped and a
// warning is logged. This means a slow subscriber can miss events.
//
// In practice this is acceptable because:
//   - The atomic snapshot is always current; missing an event only means the
//     subscriber's "wake up, rebuild" call did not happen for that specific
//     change. The next event (or the next periodic refresh) will trigger the
//     rebuild.
//   - The subscriber callback should be small ("rebuild worker X then return").
//     Heavy work belongs in a separate goroutine fed by the channel.
//
// # Deferred features
//
// These options are intentionally NOT implemented in v1 and are listed here so
// we know what to add later if a use case appears:
//
//   - WithOverflowStrategy(Drop|Block|Latest):
//     Drop is the current behaviour. Block would make the Provider's event
//     loop wait on a full channel, applying back-pressure to the store
//     event stream (risk: a single slow subscriber halts all reload work
//     for the Provider). Latest would replace a queued event with the
//     incoming one, ensuring the most recent event always wins
//     (risk: callers lose visibility into intermediate changes).
//
//   - Per-key subscriptions:
//     Today a subscriber gets every recreate-on-change event in the
//     namespace. A future Subscribe(ctx, keys...) could filter to a
//     specific subset.
//
//   - Synchronous wait for snapshot application:
//     A future SubscribeWithBarrier could guarantee that the snapshot
//     visible via Get already reflects the event being delivered.
//
// To add WithOverflowStrategy later: add an enum + Option, store it on
// Provider, and replace the default-case send in notifySubscribers with a
// strategy-driven send.
func (p *Provider[T]) Subscribe(ctx context.Context) <-chan ChangeEvent {
	ch := make(chan ChangeEvent, p.bufSize)

	p.subsMu.Lock()
	p.subs = append(p.subs, ch)
	p.subsMu.Unlock()

	go func() {
		<-ctx.Done()
		p.subsMu.Lock()
		defer p.subsMu.Unlock()
		for i, c := range p.subs {
			if c == ch {
				p.subs = append(p.subs[:i], p.subs[i+1:]...)
				close(ch)
				return
			}
		}
	}()

	return ch
}

// Catalog returns metadata for every field that liveconfig manages in T.
// Fields tagged dyn:"bootstrap" or dyn:"secret" are excluded.
//
// Use the catalog to build a config management API or UI: iterate the
// descriptors to get key names, types, descriptions, and reload strategies,
// then pair each key with its current store value via store.List.
func (p *Provider[T]) Catalog() []FieldDescriptor {
	return slices.Clone(p.catalog)
}

// Stats returns a point-in-time snapshot of the Provider's internal counters.
// Useful for liveness checks and observability dashboards.
func (p *Provider[T]) Stats() Stats {
	var lastErr string
	if ptr := p.lastErrText.Load(); ptr != nil {
		lastErr = *ptr
	}
	var last time.Time
	if ns := p.lastRefresh.Load(); ns != 0 {
		last = time.Unix(0, ns)
	}
	return Stats{
		LastRefresh:  last,
		RefreshCount: p.refreshCount.Load(),
		LastError:    lastErr,
	}
}

// Close stops the event loop, closes subscriber channels, and releases store
// resources. Calling Close more than once is safe.
func (p *Provider[T]) Close() error {
	p.closeMu.Do(func() {
		p.cancel()
		p.wg.Wait()

		p.subsMu.Lock()
		for _, ch := range p.subs {
			close(ch)
		}
		p.subs = nil
		p.subsMu.Unlock()

		p.closeErr = p.store.Close(context.Background())
		p.logger.Info("liveconfig: provider stopped", slog.String("namespace", p.ns))
	})
	return p.closeErr
}

// requestRefresh signals the event loop to perform a refresh. Non-blocking:
// if a refresh is already queued, this call is a no-op (coalescing).
func (p *Provider[T]) requestRefresh() {
	select {
	case p.refreshTrigger <- struct{}{}:
	default:
		// already pending - no-op
	}
}

// refresh loads all current overrides from the store and atomically replaces
// the snapshot. Errors from individual field parses are logged as warnings;
// the affected field retains its previous base value.
//
// refresh must only be called from the event loop goroutine (or before the
// loop starts, from New). The single-goroutine ownership of refresh removes
// any need for locking and guarantees there is no race between concurrent
// refreshes installing snapshots out of order.
func (p *Provider[T]) refresh(ctx context.Context) error {
	overrides, err := p.store.List(ctx, p.ns)
	if err != nil {
		errText := err.Error()
		p.lastErrText.Store(&errText)
		return fmt.Errorf("store.List: %w", err)
	}

	snapshot := applyOverrides(p.base, overrides, p.catalog, func(key, raw string, err error) {
		p.logger.Warn("liveconfig: could not apply override, keeping base value",
			slog.String("namespace", p.ns),
			slog.String("key", key),
			slog.String("raw_value", raw),
			slog.String("error", err.Error()),
		)
	})
	p.current.Store(&snapshot)
	p.refreshCount.Add(1)
	p.lastRefresh.Store(time.Now().UnixNano())
	empty := ""
	p.lastErrText.Store(&empty)

	p.logger.Debug("liveconfig: snapshot refreshed",
		slog.String("namespace", p.ns),
		slog.Int("overrides", len(overrides)),
	)
	return nil
}

// eventLoop is the only goroutine that calls refresh. It reads:
//   - store ChangeEvents (to refresh + notify recreate subscribers),
//   - periodic safety ticks (to catch missed changes),
//   - manual Refresh() requests (to allow ops-driven re-read).
//
// Because all three sources funnel into a single select, refreshes are
// naturally serialised. There is no chance of a slow safety refresh
// overwriting a newer change-driven snapshot.
func (p *Provider[T]) eventLoop(ctx context.Context, watchCh <-chan ChangeEvent) {
	defer p.wg.Done()
	safetyTicker := time.NewTicker(p.interval)
	defer safetyTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case event, ok := <-watchCh:
			if !ok {
				p.logger.Warn("liveconfig: watch channel closed",
					slog.String("namespace", p.ns),
				)
				return
			}
			p.handleChangeEvent(ctx, event)

		case <-safetyTicker.C:
			if err := p.refresh(ctx); err != nil {
				p.logger.Warn("liveconfig: periodic refresh failed",
					slog.String("namespace", p.ns),
					slog.String("error", err.Error()),
				)
			}

		case <-p.refreshTrigger:
			if err := p.refresh(ctx); err != nil {
				p.logger.Warn("liveconfig: manual refresh failed",
					slog.String("namespace", p.ns),
					slog.String("error", err.Error()),
				)
			}
		}
	}
}

// handleChangeEvent refreshes the snapshot and, if the changed key belongs to
// a recreate-on-change field, fans the event out to all subscribers.
//
// Because handleChangeEvent runs in the single event-loop goroutine, the
// refresh has fully installed the new snapshot before subscribers are
// notified. Subscribers calling p.Get() in their handler will therefore see
// the post-change snapshot.
func (p *Provider[T]) handleChangeEvent(ctx context.Context, event ChangeEvent) {
	p.logger.Info("liveconfig: config change received",
		slog.String("namespace", event.Namespace),
		slog.String("key", event.Key),
		slog.String("old_value", event.OldValue),
		slog.String("new_value", event.NewValue),
		slog.String("changed_by", event.ChangedBy),
	)

	if err := p.refresh(ctx); err != nil {
		p.logger.Warn("liveconfig: refresh after change event failed",
			slog.String("namespace", p.ns),
			slog.String("key", event.Key),
			slog.String("error", err.Error()),
		)
		return
	}

	if d, ok := p.descByKey[event.Key]; ok && d.ReloadStrategy == ReloadStrategyRecreate {
		p.notifySubscribers(event)
	}
}

// notifySubscribers fans a ChangeEvent out to all subscriber channels.
// Events are dropped (with a warning) when a subscriber's buffer is full;
// see Subscribe doc for the rationale and future-work notes.
func (p *Provider[T]) notifySubscribers(event ChangeEvent) {
	p.subsMu.Lock()
	defer p.subsMu.Unlock()
	for _, ch := range p.subs {
		select {
		case ch <- event:
		default:
			p.logger.Warn("liveconfig: subscriber channel full, dropping event",
				slog.String("key", event.Key),
			)
		}
	}
}
