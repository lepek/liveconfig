package memstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/lepek/liveconfig"
	"github.com/lepek/liveconfig/memstore"
)

func TestMemStore_GetMiss(t *testing.T) {
	s := memstore.New()
	val, found, err := s.Get(context.Background(), "ns", "key")
	if err != nil {
		t.Fatal(err)
	}
	if found || val != "" {
		t.Errorf("expected miss, got found=%v val=%q", found, val)
	}
}

func TestMemStore_SetAndGet(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()

	err := s.Set(ctx, liveconfig.SetEntry{
		Namespace: "svc",
		Key:       "timeout",
		Value:     "30s",
		ChangedBy: "alice",
	})
	if err != nil {
		t.Fatal(err)
	}

	val, found, err := s.Get(ctx, "svc", "timeout")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected key to be found")
	}
	if val != "30s" {
		t.Errorf("got %q, want 30s", val)
	}
}

func TestMemStore_SetOverwrite(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()

	_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "ns", Key: "k", Value: "v1", ChangedBy: "u"})
	_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "ns", Key: "k", Value: "v2", ChangedBy: "u"})

	val, _, _ := s.Get(ctx, "ns", "k")
	if val != "v2" {
		t.Errorf("got %q, want v2", val)
	}
}

func TestMemStore_List(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()

	_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "svc", Key: "a", Value: "1", ChangedBy: "u"})
	_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "svc", Key: "b", Value: "2", ChangedBy: "u"})
	_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "other", Key: "c", Value: "3", ChangedBy: "u"})

	m, err := s.List(ctx, "svc")
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 2 {
		t.Fatalf("want 2 entries, got %d", len(m))
	}
	if m["a"] != "1" || m["b"] != "2" {
		t.Errorf("unexpected map: %v", m)
	}
}

func TestMemStore_ListEmpty(t *testing.T) {
	s := memstore.New()
	m, err := s.List(context.Background(), "nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if m == nil {
		t.Error("List should return non-nil map for missing namespace")
	}
}

func TestMemStore_Delete(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()

	_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "ns", Key: "k", Value: "v", ChangedBy: "u"})
	_ = s.Delete(ctx, "ns", "k", "alice")

	_, found, _ := s.Get(ctx, "ns", "k")
	if found {
		t.Error("key should not exist after delete")
	}
}

func TestMemStore_DeleteRecordsChangedBy(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()

	_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "ns", Key: "k", Value: "v", ChangedBy: "alice"})
	_ = s.Delete(ctx, "ns", "k", "bob")

	entries, _ := s.History(ctx, "ns", "k", liveconfig.HistoryOptions{Limit: 10})
	if len(entries) < 2 {
		t.Fatalf("want at least 2 audit entries, got %d", len(entries))
	}
	// Newest first: delete then set.
	if entries[0].ChangedBy != "bob" {
		t.Errorf("delete audit ChangedBy: got %q, want bob", entries[0].ChangedBy)
	}
	if entries[0].NewValue != "" {
		t.Errorf("delete audit NewValue: got %q, want empty", entries[0].NewValue)
	}
}

func TestMemStore_DeleteNonExistent(t *testing.T) {
	s := memstore.New()
	if err := s.Delete(context.Background(), "ns", "missing", "u"); err != nil {
		t.Errorf("delete of missing key should return nil, got %v", err)
	}
}

func TestMemStore_Watch(t *testing.T) {
	s := memstore.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := s.Watch(ctx, "ns")
	if err != nil {
		t.Fatal(err)
	}

	_ = s.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "ns",
		Key:       "k",
		Value:     "v",
		ChangedBy: "bob",
	})

	select {
	case ev := <-ch:
		if ev.Key != "k" || ev.NewValue != "v" || ev.ChangedBy != "bob" {
			t.Errorf("unexpected event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Watch event")
	}
}

func TestMemStore_WatchFiltersByNamespace(t *testing.T) {
	s := memstore.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, _ := s.Watch(ctx, "ns-a")

	_ = s.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "ns-b",
		Key:       "k",
		Value:     "v",
		ChangedBy: "u",
	})

	select {
	case ev := <-ch:
		t.Errorf("should not receive event from other namespace: %+v", ev)
	case <-time.After(150 * time.Millisecond):
		// expected: no event for ns-a
	}
}

func TestMemStore_WatchClosedOnContextCancel(t *testing.T) {
	s := memstore.New()
	ctx, cancel := context.WithCancel(context.Background())

	ch, _ := s.Watch(ctx, "ns")
	cancel()

	select {
	case _, open := <-ch:
		if open {
			t.Error("channel should be closed after context cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("channel was not closed after context cancel")
	}
}

func TestMemStore_WatchDeleteEmitsEvent(t *testing.T) {
	s := memstore.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = s.Set(context.Background(), liveconfig.SetEntry{Namespace: "ns", Key: "k", Value: "v", ChangedBy: "u"})

	ch, _ := s.Watch(ctx, "ns")
	_ = s.Delete(context.Background(), "ns", "k", "carol")

	select {
	case ev := <-ch:
		if ev.NewValue != "" {
			t.Errorf("delete event NewValue should be empty, got %q", ev.NewValue)
		}
		if ev.OldValue != "v" {
			t.Errorf("delete event OldValue: got %q, want %q", ev.OldValue, "v")
		}
		if ev.ChangedBy != "carol" {
			t.Errorf("delete event ChangedBy: got %q, want carol", ev.ChangedBy)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for delete event")
	}
}

func TestMemStore_History(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()

	_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "ns", Key: "k", Value: "v1", ChangedBy: "u1"})
	_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "ns", Key: "k", Value: "v2", ChangedBy: "u2"})

	entries, err := s.History(ctx, "ns", "k", liveconfig.HistoryOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 entries, got %d", len(entries))
	}
	// Newest first.
	if entries[0].NewValue != "v2" {
		t.Errorf("entries[0].NewValue: got %q, want v2", entries[0].NewValue)
	}
	if entries[1].NewValue != "v1" {
		t.Errorf("entries[1].NewValue: got %q, want v1", entries[1].NewValue)
	}
}

func TestMemStore_HistoryPagination(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "ns", Key: "k", Value: "v", ChangedBy: "u"})
	}

	page1, _ := s.History(ctx, "ns", "k", liveconfig.HistoryOptions{Limit: 2, Offset: 0})
	page2, _ := s.History(ctx, "ns", "k", liveconfig.HistoryOptions{Limit: 2, Offset: 2})

	if len(page1) != 2 {
		t.Errorf("page1: want 2, got %d", len(page1))
	}
	if len(page2) != 2 {
		t.Errorf("page2: want 2, got %d", len(page2))
	}
}

func TestMemStore_Close(t *testing.T) {
	s := memstore.New()
	if err := s.Close(context.Background()); err != nil {
		t.Errorf("Close returned error: %v", err)
	}
}
