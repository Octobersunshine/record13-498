package logstore

import (
	"sort"
	"sync"

	"traceid-demo/logx"
)

type MemoryStore struct {
	mu     sync.RWMutex
	entries []logx.Entry
	index   map[string][]int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entries: make([]logx.Entry, 0),
		index:   make(map[string][]int),
	}
}

func (s *MemoryStore) Append(entry logx.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := len(s.entries)
	s.entries = append(s.entries, entry)
	s.index[entry.TraceID] = append(s.index[entry.TraceID], idx)
}

func (s *MemoryStore) QueryByTraceID(traceID string) []logx.Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	indices, ok := s.index[traceID]
	if !ok {
		return nil
	}

	results := make([]logx.Entry, 0, len(indices))
	for _, idx := range indices {
		results = append(results, s.entries[idx])
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp < results[j].Timestamp
	})

	return results
}

func (s *MemoryStore) AllTraceIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]string, 0, len(s.index))
	for id := range s.index {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s *MemoryStore) Stats() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := make(map[string]int, len(s.index))
	for id, indices := range s.index {
		stats[id] = len(indices)
	}
	return stats
}
