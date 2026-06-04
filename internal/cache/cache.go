package cache

import (
	"sync"
	"time"
)

type Entry struct {
	Key            string
	Value          []byte
	Lease          int64
	CreateRevision int64
	ModRevision    int64
	Version        int64
	FetchedAt      time.Time
}

type Store struct {
	mu    sync.RWMutex
	items map[string]Entry
}

func NewStore() *Store {
	return &Store{items: map[string]Entry{}}
}

func (s *Store) UpsertIfNewer(entry Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.items[entry.Key]
	if !ok || isNewer(entry, current) {
		s.items[entry.Key] = clone(entry)
	}
}

func (s *Store) Get(key string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.items[key]
	return clone(entry), ok
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
}

func (s *Store) IsFresh(entry Entry, maxStaleness time.Duration, now time.Time) bool {
	return now.Sub(entry.FetchedAt) <= maxStaleness
}

func clone(e Entry) Entry {
	out := e
	out.Value = append([]byte(nil), e.Value...)
	return out
}

func isNewer(candidate, current Entry) bool {
	if candidate.ModRevision != current.ModRevision {
		return candidate.ModRevision > current.ModRevision
	}
	return candidate.FetchedAt.After(current.FetchedAt)
}
