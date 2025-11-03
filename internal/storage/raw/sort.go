package raw

import (
	"slices"
	"sort"

	"github.com/gernest/roaring"
)

type Sorted struct {
	ID    []uint64
	Value []int64
}

var _ sort.Interface = (*Sorted)(nil)

func (s *Sorted) Len() int {
	return len(s.ID)
}

func (s *Sorted) Less(i, j int) bool {
	return s.Value[i] < s.Value[j]
}

func (s *Sorted) Swap(i, j int) {
	s.ID[i], s.ID[j] = s.ID[j], s.ID[i]
	s.Value[i], s.Value[j] = s.Value[j], s.Value[i]
}

func (s *Sorted) Reset(size int) {
	s.ID = slices.Grow(s.ID[:0], size)
	s.Value = slices.Grow(s.Value[:0], size)
}

func (b *BSI) Sorted(filter *roaring.Bitmap, result *Sorted) {
	m := make(map[uint64]uint64)
	exists := b.exists
	if filter != nil {
		exists = exists.Intersect(filter)
	}

	mergeBits(exists, 0, m)
	mergeBits(b.sign.Intersect(exists), 1<<63, m)
	for i := range b.data {
		mergeBits(b.data[i].Intersect(exists), 1<<i, m)
	}
	result.Reset(len(m))
	for k, val := range m {
		result.ID = append(result.ID, k)
		result.Value = append(result.Value,
			(2*(int64(val)>>63)+1)*int64(val&^(1<<63)),
		)
	}
	sort.Sort(result)
}
