package wal

import (
	"iter"
	"log/slog"
	"sync"
)

type Store struct {
	mu  sync.RWMutex
	wal wal
}

func (s *Store) Init(dir string, lo *slog.Logger) error {
	err := s.wal.Init(dir, lo)
	if err != nil {
		return err
	}
	first := s.wal.firstIndex()
	s.wal.deleteBefore(first - 1)
	return nil
}

func (s *Store) Record(data []byte) error {
	s.mu.Lock()
	err := s.wal.Record(data)
	s.mu.Unlock()
	return err
}

func (s *Store) Range(start uint64, maxSize uint64) iter.Seq2[uint64, []byte] {
	return func(yield func(uint64, []byte) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()

		lo := s.wal.firstIndex()
		lo = max(lo, start)
		hi := s.wal.LastIndex()
		for k, v := range s.wal.AllEntries(lo, hi, maxSize) {
			if !yield(k, v) {
				return
			}
		}
	}
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.current.sync()
}
