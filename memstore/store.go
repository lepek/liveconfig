// Package memstore provides an in-memory implementation of liveconfig.Store
// intended for use in tests and local development.
//
// MemStore is safe for concurrent use. Changes made via Set and Delete are
// immediately reflected in Get and List, and emitted as ChangeEvents to all
// active Watch channels.
//
// It does not persist data across process restarts. For production use, see
// the pgstore package.
package memstore

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lepek/liveconfig"
)

type storeEntry struct {
	value     string
	changedBy string
	updatedAt time.Time
}

type auditRecord struct {
	id        int64
	namespace string
	key       string
	oldValue  string
	newValue  string
	changedBy string
	changedAt time.Time
}

// watcher pairs a Watch subscriber's channel with the namespace it is
// scoped to. Events are only delivered to watchers whose namespace matches
// the ChangeEvent.
type watcher struct {
	namespace string
	ch        chan liveconfig.ChangeEvent
}

// MemStore is an in-memory liveconfig.Store.
type MemStore struct {
	mu    sync.RWMutex
	data  map[string]map[string]storeEntry // namespace -> key -> entry
	audit []auditRecord
	seq   atomic.Int64

	watchMu  sync.Mutex
	watchers []watcher
}

// New creates an empty MemStore.
func New() *MemStore {
	return &MemStore{
		data: make(map[string]map[string]storeEntry),
	}
}

// Get returns the current value for namespace/key.
// Returns ("", false, nil) when the key has no override.
func (s *MemStore) Get(_ context.Context, namespace, key string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns, ok := s.data[namespace]
	if !ok {
		return "", false, nil
	}
	e, ok := ns[key]
	if !ok {
		return "", false, nil
	}
	return e.value, true, nil
}

// List returns all key-value overrides for the given namespace.
func (s *MemStore) List(_ context.Context, namespace string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns, ok := s.data[namespace]
	if !ok {
		return map[string]string{}, nil
	}
	out := make(map[string]string, len(ns))
	for k, e := range ns {
		out[k] = e.value
	}
	return out, nil
}

// Set creates or updates an override and records an audit entry.
// A ChangeEvent is broadcast to all active Watch channels after the write.
func (s *MemStore) Set(_ context.Context, entry liveconfig.SetEntry) error {
	now := time.Now()

	s.mu.Lock()
	ns, ok := s.data[entry.Namespace]
	if !ok {
		ns = make(map[string]storeEntry)
		s.data[entry.Namespace] = ns
	}

	oldValue := ""
	if existing, ok := ns[entry.Key]; ok {
		oldValue = existing.value
	}

	ns[entry.Key] = storeEntry{
		value:     entry.Value,
		changedBy: entry.ChangedBy,
		updatedAt: now,
	}

	id := s.seq.Add(1)
	s.audit = append(s.audit, auditRecord{
		id:        id,
		namespace: entry.Namespace,
		key:       entry.Key,
		oldValue:  oldValue,
		newValue:  entry.Value,
		changedBy: entry.ChangedBy,
		changedAt: now,
	})
	s.mu.Unlock()

	s.broadcast(liveconfig.ChangeEvent{
		Namespace: entry.Namespace,
		Key:       entry.Key,
		OldValue:  oldValue,
		NewValue:  entry.Value,
		ChangedBy: entry.ChangedBy,
		ChangedAt: now,
	})
	return nil
}

// Delete removes an override and records an audit entry attributed to changedBy.
// A ChangeEvent with NewValue="" is broadcast to active Watch channels in the
// matching namespace. Deleting a non-existent key is a no-op.
func (s *MemStore) Delete(_ context.Context, namespace, key, changedBy string) error {
	now := time.Now()

	s.mu.Lock()
	ns, ok := s.data[namespace]
	if !ok {
		s.mu.Unlock()
		return nil
	}
	existing, ok := ns[key]
	if !ok {
		s.mu.Unlock()
		return nil
	}

	oldValue := existing.value
	delete(ns, key)

	id := s.seq.Add(1)
	s.audit = append(s.audit, auditRecord{
		id:        id,
		namespace: namespace,
		key:       key,
		oldValue:  oldValue,
		newValue:  "",
		changedBy: changedBy,
		changedAt: now,
	})
	s.mu.Unlock()

	s.broadcast(liveconfig.ChangeEvent{
		Namespace: namespace,
		Key:       key,
		OldValue:  oldValue,
		NewValue:  "",
		ChangedBy: changedBy,
		ChangedAt: now,
	})
	return nil
}

// Watch returns a channel that receives a ChangeEvent for every Set or Delete
// in the given namespace. Events for other namespaces are filtered out.
// The channel is closed when ctx is cancelled.
func (s *MemStore) Watch(ctx context.Context, namespace string) (<-chan liveconfig.ChangeEvent, error) {
	ch := make(chan liveconfig.ChangeEvent, 64)
	w := watcher{namespace: namespace, ch: ch}

	s.watchMu.Lock()
	s.watchers = append(s.watchers, w)
	s.watchMu.Unlock()

	go func() {
		<-ctx.Done()
		s.watchMu.Lock()
		defer s.watchMu.Unlock()
		for i, existing := range s.watchers {
			if existing.ch == ch {
				s.watchers = append(s.watchers[:i], s.watchers[i+1:]...)
				close(ch)
				return
			}
		}
	}()

	return ch, nil
}

// History returns the audit trail for a key, ordered newest-first.
func (s *MemStore) History(_ context.Context, namespace, key string, opts liveconfig.HistoryOptions) ([]liveconfig.AuditEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matches []liveconfig.AuditEntry
	for i := len(s.audit) - 1; i >= 0; i-- {
		r := s.audit[i]
		if r.namespace == namespace && r.key == key {
			matches = append(matches, liveconfig.AuditEntry{
				ID:        r.id,
				Namespace: r.namespace,
				Key:       r.key,
				OldValue:  r.oldValue,
				NewValue:  r.newValue,
				ChangedBy: r.changedBy,
				ChangedAt: r.changedAt,
			})
		}
	}

	opts = opts.Normalised()
	if opts.Offset >= len(matches) {
		return nil, nil
	}
	matches = matches[opts.Offset:]
	if len(matches) > opts.Limit {
		matches = matches[:opts.Limit]
	}
	return matches, nil
}

// Close is a no-op for MemStore. It is present to satisfy the liveconfig.Store interface.
func (s *MemStore) Close(_ context.Context) error { return nil }

// broadcast sends event to every watcher whose namespace matches.
// Events are dropped (not queued) if a channel's buffer is full.
func (s *MemStore) broadcast(event liveconfig.ChangeEvent) {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	for _, w := range s.watchers {
		if w.namespace != event.Namespace {
			continue
		}
		select {
		case w.ch <- event:
		default:
		}
	}
}
