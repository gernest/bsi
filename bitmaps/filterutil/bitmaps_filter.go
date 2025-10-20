package filterutil

import (
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
)

const rowWidth = 16
const keyMask = rowWidth - 1

// BitmapBitmapFilter builds a list of positions in the bitmap which
// match those in a provided bitmap. It is shard-agnostic; no matter what
// offsets the input bitmap's containers have, it matches them against
// corresponding keys.
type BitmapBitmapFilter struct {
	containers  [rowWidth]*roaring.Container
	nextOffsets [rowWidth]uint64
	callback    func(col, row uint64) error
}

// NewBitmapBitmapFilter creates a filter which can report all the positions
// within a bitmap which are set, and which have positions corresponding to
// the specified columns. It calls the provided callback function on
// each value it finds, terminating early if that returns an error.
//
// The input filter is assumed to represent one "row" of a shard's data,
// which is to say, a range of up to rowWidth consecutive containers starting
// at some multiple of rowWidth. We coerce that to the 0..rowWidth range
// because offset-within-row is what we care about.
func NewBitmapBitmapFilter(filter *roaring.Bitmap, callback func(col, row uint64) error) *BitmapBitmapFilter {
	b := new(BitmapBitmapFilter)
	b.Reset(filter, callback)
	return b
}

// Reset clears b if filter is nil. Will initialize containers and offsets if filter is not nil,
func (b *BitmapBitmapFilter) Reset(filter *roaring.Bitmap, cb func(col, row uint64) error) {
	b.callback = cb
	if filter == nil {
		clear(b.containers[:])
		clear(b.nextOffsets[:])
		return
	}
	iter, _ := filter.Containers.Iterator(0)
	last := uint64(0)
	count := 0
	for iter.Next() {
		k, v := iter.Value()
		// Coerce container key into the 0-rowWidth range we'll be
		// using to compare against containers within each row.
		k = k & keyMask
		b.containers[k] = v
		last = k
		count++
	}
	// if there's only one container, we need to populate everything with
	// its position.
	if count == 1 {
		for i := range b.containers {
			b.nextOffsets[i] = last
		}
	} else {
		// Point each container at the offset of the next valid container.
		// With sparse bitmaps this will potentially make skipping faster.
		for i := range b.containers {
			if b.containers[i] != nil {
				for int(last) != i {
					b.nextOffsets[last] = uint64(i)
					last = (last + 1) % rowWidth
				}
			}
		}
	}
}

// ConsiderKey implements roaring.BitmapFilter.
func (b *BitmapBitmapFilter) ConsiderKey(key roaring.FilterKey, n int32) roaring.FilterResult {
	pos := key & keyMask
	if b.containers[pos] == nil || n == 0 {
		return key.RejectUntilOffset(b.nextOffsets[pos])
	}
	return key.NeedData()
}

// ConsiderData implements roaring.BitmapFilter.
func (b *BitmapBitmapFilter) ConsiderData(key roaring.FilterKey, data *roaring.Container) roaring.FilterResult {
	pos := key & keyMask

	filter := b.containers[pos]
	if filter == nil {
		return key.RejectUntilOffset(b.nextOffsets[pos])
	}
	hi0 := highBits(shardwidth.ShardWidth * key.Row())

	base := uint64(key) - hi0
	var lastErr error
	matched := false
	roaring.IntersectionCallback(data, filter, func(v uint16) {
		matched = true
		row := key.Row()
		col := base<<16 | uint64(v)
		err := b.callback(col, row)
		if err != nil {
			lastErr = err
		}
	})
	if lastErr != nil {
		return key.Fail(lastErr)
	}
	if !matched {
		return key.RejectUntilOffset(b.nextOffsets[pos])
	}
	return key.MatchOneUntilOffset(b.nextOffsets[pos])
}

func highBits(v uint64) uint64 { return v >> 16 }
