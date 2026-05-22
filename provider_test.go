package liveconfig_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/lepek/liveconfig"
	"github.com/lepek/liveconfig/memstore"
)

type svcCfg struct {
	Name    string        `json:"name"    dyn:"live"               desc:"Service name"`
	Timeout time.Duration `json:"timeout" dyn:"live"               desc:"Request timeout"`
	Cron    string        `json:"cron"    dyn:"recreate-on-change" desc:"Schedule cron expr"`
	Port    int           `json:"port"    dyn:"bootstrap"`
}

func newProvider(t *testing.T, base svcCfg, store liveconfig.Store) *liveconfig.Provider[svcCfg] {
	t.Helper()
	p, err := liveconfig.New(context.Background(), base, store,
		liveconfig.WithNamespace("test"),
		liveconfig.WithLogger(slog.Default()),
		liveconfig.WithRefreshInterval(24*time.Hour), // disable periodic refresh in tests
	)
	if err != nil {
		t.Fatalf("liveconfig.New: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func TestProvider_GetReturnsBaseOnNoOverrides(t *testing.T) {
	base := svcCfg{Name: "myservice", Timeout: 5 * time.Second, Port: 8080}
	p := newProvider(t, base, memstore.New())

	cfg := p.Get()
	if cfg.Name != "myservice" {
		t.Errorf("Name: got %q, want %q", cfg.Name, "myservice")
	}
	if cfg.Timeout != 5*time.Second {
		t.Errorf("Timeout: got %v", cfg.Timeout)
	}
	// Bootstrap field must come through unchanged.
	if cfg.Port != 8080 {
		t.Errorf("Port: got %d, want 8080", cfg.Port)
	}
}

func TestProvider_GetReflectsStoreOverride(t *testing.T) {
	store := memstore.New()
	base := svcCfg{Name: "original", Timeout: 5 * time.Second}

	// Pre-seed the store before creating the provider so the initial refresh picks it up.
	_ = store.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "test",
		Key:       "name",
		Value:     "overridden",
		ChangedBy: "test",
	})

	p := newProvider(t, base, store)

	cfg := p.Get()
	if cfg.Name != "overridden" {
		t.Errorf("Name: got %q, want %q", cfg.Name, "overridden")
	}
}

func TestProvider_GetUpdatesAfterSetOnStore(t *testing.T) {
	store := memstore.New()
	base := svcCfg{Name: "original"}
	p := newProvider(t, base, store)

	if p.Get().Name != "original" {
		t.Fatal("precondition: Name should be 'original'")
	}

	err := store.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "test",
		Key:       "name",
		Value:     "live-updated",
		ChangedBy: "test",
	})
	if err != nil {
		t.Fatalf("store.Set: %v", err)
	}

	// The watch goroutine is async; give it a moment.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.Get().Name == "live-updated" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("Name never updated to 'live-updated'; got %q", p.Get().Name)
}

func TestProvider_DeleteRevertsToBase(t *testing.T) {
	store := memstore.New()
	base := svcCfg{Name: "base"}

	_ = store.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "test",
		Key:       "name",
		Value:     "overridden",
		ChangedBy: "test",
	})

	p := newProvider(t, base, store)
	if p.Get().Name != "overridden" {
		t.Fatal("precondition: override should be applied")
	}

	_ = store.Delete(context.Background(), "test", "name", "test")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.Get().Name == "base" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("Name did not revert to base; got %q", p.Get().Name)
}

func TestProvider_BootstrapFieldNotOverridden(t *testing.T) {
	store := memstore.New()
	base := svcCfg{Port: 9090}

	// Attempt to override a bootstrap field via the store (bypass validation).
	_ = store.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "test",
		Key:       "port",
		Value:     "1234",
		ChangedBy: "test",
	})

	p := newProvider(t, base, store)

	// Bootstrap fields are excluded from the catalog so overrides are silently ignored.
	if p.Get().Port != 9090 {
		t.Errorf("Port: bootstrap field was overridden; got %d", p.Get().Port)
	}
}

func TestProvider_SubscribeReceivesRecreateEvents(t *testing.T) {
	store := memstore.New()
	base := svcCfg{Cron: "0 * * * *"}
	p := newProvider(t, base, store)

	subCtx, cancelSub := context.WithCancel(context.Background())
	defer cancelSub()
	sub := p.Subscribe(subCtx)

	var wg sync.WaitGroup
	var received liveconfig.ChangeEvent
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case ev := <-sub:
			received = ev
		case <-time.After(2 * time.Second):
			t.Error("timeout waiting for subscriber event")
		}
	}()

	_ = store.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "test",
		Key:       "cron",
		Value:     "30 * * * *",
		ChangedBy: "alice",
	})
	wg.Wait()

	if received.Key != "cron" {
		t.Errorf("Key: got %q, want %q", received.Key, "cron")
	}
	if received.NewValue != "30 * * * *" {
		t.Errorf("NewValue: got %q", received.NewValue)
	}
	if received.ChangedBy != "alice" {
		t.Errorf("ChangedBy: got %q", received.ChangedBy)
	}
}

func TestProvider_SubscribeDoesNotReceiveLiveEvents(t *testing.T) {
	store := memstore.New()
	base := svcCfg{Name: "original"}
	p := newProvider(t, base, store)

	subCtx, cancelSub := context.WithCancel(context.Background())
	defer cancelSub()
	sub := p.Subscribe(subCtx)

	_ = store.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "test",
		Key:       "name", // tagged dyn:"live", not recreate-on-change
		Value:     "updated",
		ChangedBy: "test",
	})

	// Wait for the event loop to process the event.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if p.Get().Name == "updated" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case ev := <-sub:
		t.Errorf("subscriber should not receive live event; got key=%q", ev.Key)
	default:
		// expected: no event
	}
}

func TestProvider_SubscribeChannelClosedOnContextCancel(t *testing.T) {
	store := memstore.New()
	p := newProvider(t, svcCfg{}, store)

	subCtx, cancelSub := context.WithCancel(context.Background())
	sub := p.Subscribe(subCtx)
	cancelSub()

	select {
	case _, open := <-sub:
		if open {
			t.Error("subscriber channel should be closed after ctx cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber channel not closed after ctx cancel")
	}
}

func TestProvider_Refresh(t *testing.T) {
	store := memstore.New()
	base := svcCfg{Name: "original"}
	p := newProvider(t, base, store)

	statsBefore := p.Stats()

	// Force-write to the store the "wrong" way to bypass the event channel
	// (we want Refresh to be what brings the new value into the snapshot).
	// memstore actually does emit, but the test simply asserts Refresh
	// bumps the count and is non-blocking.
	p.Refresh()

	// Allow the event loop to process.
	time.Sleep(50 * time.Millisecond)

	statsAfter := p.Stats()
	if statsAfter.RefreshCount <= statsBefore.RefreshCount {
		t.Errorf("RefreshCount should increase after Refresh(); before=%d after=%d",
			statsBefore.RefreshCount, statsAfter.RefreshCount)
	}
}

func TestProvider_StatsSuccessTracking(t *testing.T) {
	store := memstore.New()
	p := newProvider(t, svcCfg{}, store)

	stats := p.Stats()
	if stats.RefreshCount == 0 {
		t.Errorf("RefreshCount should be at least 1 (initial load); got %d", stats.RefreshCount)
	}
	if stats.LastRefresh.IsZero() {
		t.Errorf("LastRefresh should be set after initial load")
	}
	if stats.LastError != "" {
		t.Errorf("LastError should be empty after successful refresh; got %q", stats.LastError)
	}
}

func TestProvider_Catalog(t *testing.T) {
	p := newProvider(t, svcCfg{}, memstore.New())
	catalog := p.Catalog()

	byKey := make(map[string]liveconfig.FieldDescriptor)
	for _, d := range catalog {
		byKey[d.Key] = d
	}

	if _, ok := byKey["name"]; !ok {
		t.Error("catalog missing 'name'")
	}
	if _, ok := byKey["port"]; ok {
		t.Error("catalog should not include bootstrap field 'port'")
	}
}

// TestProvider_GetReturnsCopy verifies that Get returns an independent value
// copy: mutating the returned struct has no effect on subsequent Get calls.
func TestProvider_GetReturnsCopy(t *testing.T) {
	store := memstore.New()
	base := svcCfg{Name: "original"}
	p := newProvider(t, base, store)

	first := p.Get()
	// Mutate the local copy.
	first.Name = "mutated-locally"

	// A subsequent Get must still return the unchanged snapshot.
	if p.Get().Name != "original" {
		t.Errorf("shared snapshot was affected by local mutation; Name=%q", p.Get().Name)
	}
}

// TestProvider_GetSnapshotIndependentAfterChange verifies that a value obtained
// before a store change is not affected after the snapshot is updated.
func TestProvider_GetSnapshotIndependentAfterChange(t *testing.T) {
	store := memstore.New()
	base := svcCfg{Name: "original"}
	p := newProvider(t, base, store)

	before := p.Get()

	_ = store.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "test",
		Key:       "name",
		Value:     "updated",
		ChangedBy: "test",
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.Get().Name == "updated" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The value captured before the change is a copy and must be unaffected.
	if before.Name != "original" {
		t.Errorf("pre-change value was affected by snapshot update; Name=%q", before.Name)
	}
}
