package liveconfig_test

import (
	"context"
	"testing"
	"time"

	"github.com/lepek/liveconfig"
	"github.com/lepek/liveconfig/memstore"
)

func TestPollingWatcher_DetectsAdd(t *testing.T) {
	store := memstore.New()
	pw := liveconfig.NewPollingWatcher(store, 50*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := pw.Watch(ctx, "ns")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Wait for the watcher to record its (empty) baseline.
	time.Sleep(80 * time.Millisecond)

	_ = store.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "ns", Key: "k", Value: "v1", ChangedBy: "u",
	})

	select {
	case ev := <-ch:
		if ev.Key != "k" || ev.NewValue != "v1" || ev.OldValue != "" {
			t.Errorf("unexpected event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for add event")
	}
}

func TestPollingWatcher_DetectsUpdate(t *testing.T) {
	store := memstore.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = store.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "ns", Key: "k", Value: "v1", ChangedBy: "u",
	})

	pw := liveconfig.NewPollingWatcher(store, 50*time.Millisecond, nil)
	ch, _ := pw.Watch(ctx, "ns")

	// Wait for the initial seed poll to complete.
	time.Sleep(80 * time.Millisecond)

	_ = store.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "ns", Key: "k", Value: "v2", ChangedBy: "u",
	})

	select {
	case ev := <-ch:
		if ev.Key != "k" || ev.NewValue != "v2" || ev.OldValue != "v1" {
			t.Errorf("unexpected event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for update event")
	}
}

func TestPollingWatcher_DetectsDelete(t *testing.T) {
	store := memstore.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = store.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "ns", Key: "k", Value: "v1", ChangedBy: "u",
	})

	pw := liveconfig.NewPollingWatcher(store, 50*time.Millisecond, nil)
	ch, _ := pw.Watch(ctx, "ns")

	// Wait for the initial seed poll to complete.
	time.Sleep(80 * time.Millisecond)

	_ = store.Delete(context.Background(), "ns", "k", "u")

	select {
	case ev := <-ch:
		if ev.Key != "k" || ev.NewValue != "" || ev.OldValue != "v1" {
			t.Errorf("unexpected event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for delete event")
	}
}

func TestPollingWatcher_NoSpuriousEventsWhenUnchanged(t *testing.T) {
	store := memstore.New()
	_ = store.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "ns", Key: "k", Value: "v1", ChangedBy: "u",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pw := liveconfig.NewPollingWatcher(store, 50*time.Millisecond, nil)
	ch, _ := pw.Watch(ctx, "ns")

	// Let it poll several times without any changes.
	time.Sleep(200 * time.Millisecond)

	select {
	case ev := <-ch:
		t.Errorf("received spurious event: %+v", ev)
	default:
		// expected: no events
	}
}

// TestPollingWatcher_DoesNotEmitForInitialState verifies that keys present in
// the store before Watch was called are treated as the baseline and do not
// produce events on the first successful poll. Without this guard a watcher
// starting against a populated store would emit a "new" event for every key.
func TestPollingWatcher_DoesNotEmitForInitialState(t *testing.T) {
	store := memstore.New()
	for _, key := range []string{"a", "b", "c"} {
		_ = store.Set(context.Background(), liveconfig.SetEntry{
			Namespace: "ns", Key: key, Value: "v", ChangedBy: "u",
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pw := liveconfig.NewPollingWatcher(store, 50*time.Millisecond, nil)
	ch, _ := pw.Watch(ctx, "ns")

	// Several poll ticks elapse with no new changes after the seed.
	select {
	case ev := <-ch:
		t.Errorf("baseline keys should not emit events, got %+v", ev)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestPollingWatcher_ClosedOnContextCancel(t *testing.T) {
	store := memstore.New()
	ctx, cancel := context.WithCancel(context.Background())

	pw := liveconfig.NewPollingWatcher(store, 50*time.Millisecond, nil)
	ch, _ := pw.Watch(ctx, "ns")

	cancel()

	select {
	case _, open := <-ch:
		if open {
			t.Error("channel should be closed after context cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after context cancel")
	}
}
