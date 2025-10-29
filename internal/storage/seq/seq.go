package seq

import (
	"iter"

	"github.com/gernest/roaring/shardwidth"
)

// Sequence is a range of consecutive uint64. The upper bound Hi is exclusive.
type Sequence struct {
	Lo, Hi uint64
}

func (s *Sequence) Next() (o Sequence) {
	if s.IsEmpty() {
		return Sequence{}
	}
	o = *s
	shard := s.Lo / shardwidth.ShardWidth
	offset := (shard + 1) * shardwidth.ShardWidth
	if s.Hi < offset {
		*s = Sequence{}
		return
	}
	s.Lo = offset
	o.Hi = offset
	return
}

// IsEmpty return true if seq is not set.
func (s Sequence) IsEmpty() bool {
	return s == Sequence{}
}

func RangeShardSequence(se Sequence) iter.Seq2[int, Sequence] {
	var i int
	return func(yield func(int, Sequence) bool) {
		for nxt := se.Next(); !nxt.IsEmpty(); nxt = se.Next() {
			if !yield(i, nxt) {
				return
			}
			i += int(nxt.Hi - nxt.Lo)
		}
	}
}
