package seq

import (
	"fmt"
	"iter"

	"github.com/gernest/roaring/shardwidth"
)

// Sequence is a range of consecutive uint64. The upper bound Hi is exclusive.
type Sequence struct {
	Lo, Hi uint64
}

func (s Sequence) FullShard() bool {
	return s.Lo/shardwidth.ShardWidth != s.Hi/shardwidth.ShardWidth
}

func (s *Sequence) Next() (o Sequence) {
	o = *s
	if s.IsEmpty() {
		return Sequence{}
	}
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

func (s Sequence) String() string {
	return fmt.Sprintf("%v...%v", s.Lo, s.Hi)
}

// IsEmpty return true if seq is not set.
func (s Sequence) IsEmpty() bool {
	return s == Sequence{}
}

// Range generates sequences, yielding position and value. Position starts at 0.
func (s *Sequence) Range() iter.Seq2[int, uint64] {
	return func(yield func(int, uint64) bool) {
		var i int
		for x := s.Lo; x < s.Hi; x++ {
			if !yield(i, x) {
				/// reset lo position
				s.Lo = x
				return
			}
			i++
		}
		*s = Sequence{}
	}
}

// RangeShardSequence iterates over sequences yielding shard occurrence with the
// starting position. The position stats from 0 which points to the first se.Lo value.
//
// Used for iterating in shard batches without allocation.
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
