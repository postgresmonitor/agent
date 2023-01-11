package util

import "sync"

// shared by multiple components
type Stats struct {
	data map[string]int
	mu   sync.Mutex
}

func (s *Stats) Increment(key string) {
	s.IncrementBy(key, 1)
}

func (s *Stats) IncrementBy(key string, value int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.data == nil {
		s.data = make(map[string]int)
	}

	// this is safe because the default value is 0 and we'll only have count stats
	s.data[key] += value
}

func (s *Stats) ToMap() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.data == nil {
		s.data = make(map[string]int)
	}

	return s.data
}

func (s *Stats) CopyAndReset() *Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	copiedData := make(map[string]int)
	for key, value := range s.data {
		copiedData[key] = value
	}

	copy := &Stats{
		data: copiedData,
	}

	s.data = nil

	return copy
}
