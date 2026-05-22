package pgstore_test

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lepek/liveconfig"
	"github.com/lepek/liveconfig/pgstore"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// startPostgres starts a throwaway Postgres container and returns the DSN.
// The container is stopped when the test ends.
func startPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("liveconfig_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Skipf("could not start postgres container (Docker required): %v", err)
	}
	t.Cleanup(func() { ctr.Terminate(ctx) }) //nolint:errcheck

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return dsn
}

func newPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// safeIdent converts a test name into a short, unique, SQL-safe identifier
// that can be embedded into table and channel names without overflowing the
// 63-character limit even when the migration appends prefixes like
// "liveconfig_notify_fn_". A short hash of the test name keeps each test
// isolated without exposing the long name.
func safeIdent(t *testing.T) string {
	t.Helper()
	sum := sha1.Sum([]byte(t.Name()))
	return hex.EncodeToString(sum[:6]) // 12-char hex
}

func newStore(t *testing.T) (*pgstore.PGStore, *pgxpool.Pool) {
	t.Helper()
	dsn := startPostgres(t)
	pool := newPool(t, dsn)

	suffix := safeIdent(t)
	s := pgstore.New(pool,
		pgstore.WithTable(fmt.Sprintf("liveconfig_values_%s", suffix)),
		pgstore.WithAuditTable(fmt.Sprintf("liveconfig_audit_%s", suffix)),
		pgstore.WithNotifyChannel(fmt.Sprintf("liveconfig_ch_%s", suffix)),
	)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s, pool
}

func TestPGStore_SetAndGet(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	err := s.Set(ctx, liveconfig.SetEntry{
		Namespace: "svc",
		Key:       "timeout",
		Value:     "30s",
		ChangedBy: "alice",
	})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	val, found, err := s.Get(ctx, "svc", "timeout")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected key to be found")
	}
	if val != "30s" {
		t.Errorf("got %q, want 30s", val)
	}
}

func TestPGStore_GetMiss(t *testing.T) {
	s, _ := newStore(t)
	val, found, err := s.Get(context.Background(), "ns", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if found || val != "" {
		t.Errorf("expected miss, got found=%v val=%q", found, val)
	}
}

func TestPGStore_SetUpsert(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "ns", Key: "k", Value: "v1", ChangedBy: "u"})
	_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "ns", Key: "k", Value: "v2", ChangedBy: "u"})

	val, _, _ := s.Get(ctx, "ns", "k")
	if val != "v2" {
		t.Errorf("got %q, want v2", val)
	}
}

func TestPGStore_List(t *testing.T) {
	s, _ := newStore(t)
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

func TestPGStore_Delete(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "ns", Key: "k", Value: "v", ChangedBy: "u"})
	_ = s.Delete(ctx, "ns", "k", "alice")

	_, found, _ := s.Get(ctx, "ns", "k")
	if found {
		t.Error("key should not exist after delete")
	}
}

func TestPGStore_DeleteRecordsChangedBy(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	_ = s.Set(ctx, liveconfig.SetEntry{Namespace: "ns", Key: "k", Value: "v", ChangedBy: "alice"})
	_ = s.Delete(ctx, "ns", "k", "bob")

	entries, err := s.History(ctx, "ns", "k", liveconfig.HistoryOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 {
		t.Fatalf("want at least 2 audit entries, got %d", len(entries))
	}
	if entries[0].ChangedBy != "bob" {
		t.Errorf("most recent (delete) audit ChangedBy: got %q, want bob", entries[0].ChangedBy)
	}
}

func TestPGStore_DeleteNonExistent(t *testing.T) {
	s, _ := newStore(t)
	if err := s.Delete(context.Background(), "ns", "missing", "u"); err != nil {
		t.Errorf("delete of missing key should not error: %v", err)
	}
}

func TestPGStore_History(t *testing.T) {
	s, _ := newStore(t)
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
	if entries[0].NewValue != "v2" {
		t.Errorf("entries[0].NewValue: got %q", entries[0].NewValue)
	}
}

func TestPGStore_WatchReceivesSetEvent(t *testing.T) {
	s, _ := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := s.Watch(ctx, "ns")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Give the LISTEN goroutine a moment to register.
	time.Sleep(100 * time.Millisecond)

	_ = s.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "ns",
		Key:       "k",
		Value:     "hello",
		ChangedBy: "bob",
	})

	select {
	case ev := <-ch:
		if ev.Key != "k" || ev.NewValue != "hello" || ev.ChangedBy != "bob" {
			t.Errorf("unexpected event: %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for Watch event")
	}
}

func TestPGStore_WatchReceivesDeleteEvent(t *testing.T) {
	s, _ := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = s.Set(context.Background(), liveconfig.SetEntry{Namespace: "ns", Key: "k", Value: "v", ChangedBy: "u"})

	ch, err := s.Watch(ctx, "ns")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	_ = s.Delete(context.Background(), "ns", "k", "carol")

	select {
	case ev := <-ch:
		if ev.NewValue != "" {
			t.Errorf("delete event NewValue: got %q, want empty", ev.NewValue)
		}
		if ev.OldValue != "v" {
			t.Errorf("delete event OldValue: got %q, want v", ev.OldValue)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for delete Watch event")
	}
}

func TestPGStore_WatchFiltersByNamespace(t *testing.T) {
	s, _ := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := s.Watch(ctx, "ns-a")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	_ = s.Set(context.Background(), liveconfig.SetEntry{
		Namespace: "ns-b",
		Key:       "k",
		Value:     "v",
		ChangedBy: "u",
	})

	select {
	case ev := <-ch:
		t.Errorf("watcher for ns-a should not receive ns-b event: %+v", ev)
	case <-time.After(500 * time.Millisecond):
	}
}

func TestPGStore_Close(t *testing.T) {
	s, _ := newStore(t)
	if err := s.Close(context.Background()); err != nil {
		t.Errorf("Close returned error: %v", err)
	}
	// Second close must be safe.
	if err := s.Close(context.Background()); err != nil {
		t.Errorf("second Close returned error: %v", err)
	}
}
