package filterutil

import (
	"github.com/gernest/roaring"
)

// BitmapBitmapRowFilter is like BitmapBitmapFilter but collects all matching rows into
// a single bitmap.
type BitmapBitmapRowFilter struct {
	containers  [rowWidth]*roaring.Container
	nextOffsets [rowWidth]uint64
	ra          *roaring.Bitmap
}

// Reset resets b with containers from filter.
func (b *BitmapBitmapRowFilter) Reset(filter *roaring.Bitmap) {
	b.ra = roaring.NewBitmap()
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
func (b *BitmapBitmapRowFilter) ConsiderKey(key roaring.FilterKey, n int32) roaring.FilterResult {
	pos := key & keyMask
	if b.containers[pos] == nil || n == 0 {
		return key.RejectUntilOffset(b.nextOffsets[pos])
	}
	return key.NeedData()
}

// Rows return all rows matched by the filter.
func (b *BitmapBitmapRowFilter) Rows() *roaring.Bitmap {
	return b.ra
}

// ConsiderData implements roaring.BitmapFilter.
func (b *BitmapBitmapRowFilter) ConsiderData(key roaring.FilterKey, data *roaring.Container) roaring.FilterResult {
	pos := key & keyMask
	filter := b.containers[pos]
	if filter == nil {
		return key.RejectUntilOffset(b.nextOffsets[pos])
	}

	matched := roaring.IntersectionAny(data, filter)

	if !matched {
		return key.RejectUntilOffset(b.nextOffsets[pos])
	}
	b.ra.DirectAdd(key.Row())
	return key.MatchOneUntilOffset(b.nextOffsets[pos])
}
