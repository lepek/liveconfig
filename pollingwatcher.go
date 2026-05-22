package liveconfig

import (
	"context"
	"log/slog"
	"time"
)

// PollingWatcher implements the Watch half of the Store interface for backends
// that do not have a native push/subscribe mechanism (MySQL, SQLite, Redis,
// Consul, etcd, etc.).
//
// It calls Store.List on a fixed interval, diffs the result against the
// previous snapshot, and emits a ChangeEvent for every key that was added,
// changed, or deleted. The ChangeEvent.ChangedBy field is set to "poll" because
// there is no actor information available at poll time; the real actor is
// recorded in the audit log by the Store.Set/Delete implementation.
//
// # Initial seed
//
// PollingWatcher records the first successful List call as its baseline and
// does not emit any events for that initial state. If the first List fails
// (e.g. the database is not yet reachable), the watcher keeps trying on each
// tick until it succeeds; until then, no events are emitted. This prevents
// a false event storm when the store recovers, which would otherwise look
// like every existing key was just created.
//
// # Usage inside a custom Store
//
// Because PollingWatcher.Watch matches the Store.Watch signature exactly, you
// can embed *PollingWatcher in a custom Store and the embedded method
// satisfies the interface directly:
//
//	type MySQLStore struct {
//	    *liveconfig.PollingWatcher
//	    // ... other fields
//	}
//
//	func New(db *sql.DB) *MySQLStore {
//	    s := &MySQLStore{db: db}
//	    s.PollingWatcher = liveconfig.NewPollingWatcher(s, 5*time.Second, slog.Default())
//	    return s
//	}
//
// See docs/DEVELOPER_GUIDE.md for a complete example.
type PollingWatcher struct {
	lister   lister
	interval time.Duration
	logger   *slog.Logger
}

// lister is the subset of Store used by PollingWatcher.
// It is an unexported interface so that the dependency is clear without
// exposing an additional public type.
type lister interface {
	List(ctx context.Context, namespace string) (map[string]string, error)
}

// NewPollingWatcher creates a PollingWatcher that calls lister.List every
// interval for comparison. Pass the Store itself as the lister.
func NewPollingWatcher(l lister, interval time.Duration, logger *slog.Logger) *PollingWatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &PollingWatcher{
		lister:   l,
		interval: interval,
		logger:   logger,
	}
}

// Watch starts a goroutine that polls lister.List for the given namespace on
// every tick and emits a ChangeEvent for each key that changed since the last
// tick. The returned channel is closed when ctx is cancelled.
//
// Watch never returns a non-nil error today; the error return is kept on the
// signature so it remains compatible with Store.Watch and so future
// implementations (e.g. a synchronous seed mode) can use it.
//
// Multiple callers may call Watch independently; each gets its own channel.
func (pw *PollingWatcher) Watch(ctx context.Context, namespace string) (<-chan ChangeEvent, error) {
	ch := make(chan ChangeEvent, 64)
	go pw.pollLoop(ctx, namespace, ch)
	return ch, nil
}

func (pw *PollingWatcher) pollLoop(ctx context.Context, namespace string, ch chan ChangeEvent) {
	defer close(ch)

	ticker := time.NewTicker(pw.interval)
	defer ticker.Stop()

	var (
		prev    map[string]string
		seeded  bool
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			curr, err := pw.lister.List(ctx, namespace)
			if err != nil {
				pw.logger.Warn("pollingwatcher: list failed",
					slog.String("namespace", namespace),
					slog.String("error", err.Error()),
				)
				continue
			}

			// On the first successful list, record the baseline silently.
			// Without this guard, every key already in the store would be
			// emitted as a "new" event the first time the watcher runs.
			if !seeded {
				prev = curr
				seeded = true
				pw.logger.Debug("pollingwatcher: initial seed recorded",
					slog.String("namespace", namespace),
					slog.Int("keys", len(curr)),
				)
				continue
			}

			now := time.Now()

			// Detect added and updated keys.
			for k, newVal := range curr {
				oldVal, existed := prev[k]
				if !existed || oldVal != newVal {
					select {
					case ch <- ChangeEvent{
						Namespace: namespace,
						Key:       k,
						OldValue:  oldVal,
						NewValue:  newVal,
						ChangedBy: "poll",
						ChangedAt: now,
					}:
					case <-ctx.Done():
						return
					}
				}
			}

			// Detect deleted keys.
			for k, oldVal := range prev {
				if _, still := curr[k]; !still {
					select {
					case ch <- ChangeEvent{
						Namespace: namespace,
						Key:       k,
						OldValue:  oldVal,
						NewValue:  "",
						ChangedBy: "poll",
						ChangedAt: now,
					}:
					case <-ctx.Done():
						return
					}
				}
			}

			prev = curr
		}
	}
}
