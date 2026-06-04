package cache

import (
	"testing"
	"time"
)

func TestUpsertIfNewer(t *testing.T) {
	store := NewStore()
	key := "/bounded/demo"

	store.UpsertIfNewer(Entry{
		Key:         key,
		Value:       []byte("new"),
		ModRevision: 10,
		FetchedAt:   time.Unix(20, 0),
	})
	store.UpsertIfNewer(Entry{
		Key:         key,
		Value:       []byte("old"),
		ModRevision: 9,
		FetchedAt:   time.Unix(30, 0),
	})

	entry, ok := store.Get(key)
	if !ok {
		t.Fatalf("expected entry to exist")
	}
	if got := string(entry.Value); got != "new" {
		t.Fatalf("expected newest value to remain, got %q", got)
	}
}

func TestIsFresh(t *testing.T) {
	store := NewStore()
	entry := Entry{FetchedAt: time.Unix(100, 0)}

	if !store.IsFresh(entry, 2*time.Second, time.Unix(101, 0)) {
		t.Fatalf("expected fresh entry")
	}
	if store.IsFresh(entry, 2*time.Second, time.Unix(103, 0)) {
		t.Fatalf("expected stale entry")
	}
}
