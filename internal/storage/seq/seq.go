package seq

import (
	"encoding/binary"
	"os"
	"sync"
)

// Seq allocates monotonically increasing sequences. Used to assign record ids.
type Seq struct {
	mu    sync.Mutex
	f     *os.File
	value uint64
	buf   [8]byte
}

func (s *Seq) Init(name string) error {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	s.f = f
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	if stat.Size() == 8 {
		_, err := f.Read(s.buf[:])
		if err != nil {
			f.Close()
			return err
		}
		s.value = binary.BigEndian.Uint64(s.buf[:])
	}
	if s.value == 0 {
		// counting always starts from 1
		s.value++
	}
	return nil
}

func (s *Seq) Close() error {
	return s.f.Close()
}

func (s *Seq) Allocate(size uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.value += size
	binary.BigEndian.PutUint64(s.buf[:], s.value)
	_, err := s.f.WriteAt(s.buf[:], 0)
	if err != nil {
		return 0, err
	}
	s.f.Sync()
	return s.value, nil
}
