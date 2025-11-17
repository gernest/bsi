package tsid

import (
	"encoding/binary"
	"iter"
)

type ID []byte

func (id ID) Range() iter.Seq2[uint64, uint64] {
	return func(yield func(uint64, uint64) bool) {
		b := id
		for len(b) > 0 {
			col, n := binary.Uvarint(b)
			b = b[n:]
			val, n := binary.Uvarint(b)
			b = b[n:]
			if !yield(col, val) {
				return
			}
		}
	}
}

func (id *ID) Append(col, value uint64) {
	*id = binary.AppendUvarint(*id, col)
	*id = binary.AppendUvarint(*id, value)
}
