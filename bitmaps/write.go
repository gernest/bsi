package bitmaps

import (
	"math/bits"

	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
)

const (
	// Row ids used for boolean fields.
	falseRowID = uint64(0)
	trueRowID  = uint64(1)

	falseRowOffset = 0 * shardwidth.ShardWidth // fragment row 0
	trueRowOffset  = 1 * shardwidth.ShardWidth // fragment row 1
)

// Mutex (id, value[0]), (id, value[1])  .... as MUTEX into ra.
func Mutex(ra *roaring.Bitmap, id uint64, value ...uint64) {
	for i := range value {
		if value[i] == 0 {
			continue
		}
		ra.DirectAdd(
			value[i]*shardwidth.ShardWidth +
				(id % shardwidth.ShardWidth),
		)
	}
}

// Exist sets existing bit for id in ra. Encodes EXISTENCE bitmaps.
func Exist(ra *roaring.Bitmap, id uint64) {
	ra.DirectAdd(id % shardwidth.ShardWidth)
}

// BSI encodes (id, signedValue) as bit sliced index into ra.
func BSI(ra *roaring.Bitmap, id uint64, signedValue int64) {
	fragmentColumn := id % shardwidth.ShardWidth
	ra.DirectAdd(fragmentColumn)
	negative := signedValue < 0
	var value uint64
	if negative {
		ra.DirectAdd(shardwidth.ShardWidth + fragmentColumn) // set sign bit
		value = uint64(signedValue * -1)
	} else {
		value = uint64(signedValue)
	}
	lz := bits.LeadingZeros64(value)
	row := uint64(2)
	for mask := uint64(0x1); mask <= 1<<(64-lz) && mask != 0; mask <<= 1 {
		if value&mask > 0 {
			ra.DirectAdd(row*shardwidth.ShardWidth + fragmentColumn)
		}
		row++
	}
}

func Boolean(m *roaring.Bitmap, id uint64, value bool) {
	fragmentColumn := id % shardwidth.ShardWidth
	if value {
		m.DirectAdd(trueRowOffset + fragmentColumn)
	} else {
		m.DirectAdd(falseRowOffset + fragmentColumn)
	}
}
