package raw

import (
	"math/bits"
	"slices"
	"sync"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/roaring"
)

// BSI contains encoded BSI (column, value) tuple,
type BSI struct {
	mu     sync.Mutex
	exists *roaring.Bitmap
	sign   *roaring.Bitmap
	data   []*roaring.Bitmap
}

// From converts rbf BSI data into b for the given filter columns.
func (b *BSI) From(tx bitmaps.OffsetRanger, shard uint64, bitDepth uint8, filter *roaring.Bitmap) error {
	exists, err := bitmaps.Row(tx, shard, 0)
	if err != nil {
		return err
	}
	if filter != nil {
		exists = exists.Intersect(filter)
	}

	sign, err := bitmaps.Row(tx, shard, 1)
	if err != nil {
		return err
	}
	sign = sign.Intersect(exists)

	if len(b.data) < int(bitDepth) {
		b.data = slices.Grow(b.data, int(bitDepth))[:bitDepth]
	}

	for i := range uint64(bitDepth) {
		bits, err := bitmaps.Row(tx, shard, 2+i)
		if err != nil {
			return err
		}
		bits = bits.Intersect(exists).Clone()

		if b.data[i] != nil {
			b.data[i].UnionInPlace(bits)
			continue
		}
		b.data[i] = bits
	}
	b.exists.UnionInPlace(exists.Clone())
	b.sign.UnionInPlace(sign.Clone())
	return nil
}

// Union performs b or o and writes results in b.
func (b *BSI) Union(o *BSI) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.data) < len(o.data) {
		b.data = slices.Grow(b.data, len(o.data))
	}

	b.exists.UnionInPlace(o.exists)
	b.sign.UnionInPlace(o.sign)

	for i := range b.data {
		if b.data[i] == nil {
			b.data[i] = o.data[i].Clone()
			continue
		}
		b.data[i].UnionInPlace(o.data[i])
	}
}

// Any returns true if there is any values in b.
func (b *BSI) Any() bool {
	return b.exists.Any()
}

// Init setups b.
func (b *BSI) Init() {
	b.exists = roaring.NewBitmap()
	b.sign = roaring.NewBitmap()
}

// Reset clears b for reuse.
func (b *BSI) Reset() *BSI {
	b.exists.Containers.Reset()
	b.sign.Containers.Reset()
	clear(b.data)
	b.data = b.data[:0]
	return b
}

// Optimize run optimize all bitmaps.
func (b *BSI) Optimize() {
	b.exists.Optimize()
	b.sign.Optimize()
	for i := range b.data {
		b.data[i].Optimize()
	}
}

// GetValue reads value encoded at column.
func (b *BSI) GetValue(column uint64) (val uint64, exists bool) {
	if !b.exists.Contains(column) {
		return
	}
	if b.sign.Contains(column) {
		val |= 1 << 63
	}
	for i := range b.data {
		if b.data[i].Contains(column) {
			val |= (1 << i)
		}
	}
	val = uint64((2*(int64(val)>>63) + 1) * int64(val&^(1<<63)))
	return val, true
}

// AsMap returns map of id:value matching filter.
func (b *BSI) AsMap(filters *roaring.Bitmap) (result map[uint64]uint64) {
	result = make(map[uint64]uint64)
	exists := b.exists
	if filters != nil {
		exists = exists.Intersect(filters)
	}
	mergeBits(exists, 0, result)
	mergeBits(b.sign.Intersect(exists), 1<<63, result)
	for i := range b.data {
		mergeBits(b.data[i].Intersect(exists), 1<<i, result)
	}
	for k, val := range result {
		result[k] = uint64((2*(int64(val)>>63) + 1) * int64(val&^(1<<63)))
	}
	return
}

// EQ returns all column ids with predicate value.
func (b *BSI) EQ(predicate int64) *roaring.Bitmap {
	exists := b.exists
	if !exists.Any() {
		return roaring.NewBitmap()
	}
	unsignedPredicate := absInt64(predicate)
	if bits.Len64(unsignedPredicate) > len(b.data) {
		// Predicate is out of range.
		return roaring.NewBitmap()
	}
	if predicate < 0 {
		exists = exists.Intersect(b.sign) // only negatives
	} else {
		exists = exists.Difference(b.sign) // only positives
	}
	for i := range b.data {
		row := b.data[i]
		bit := (unsignedPredicate >> uint(i)) & 1

		if bit == 1 {
			exists = exists.Intersect(row)
		} else {
			exists = exists.Difference(row)
		}
	}
	return exists
}

// Transpose returns a bitmap of all values matching given filters.
func (b *BSI) Transpose(filters *roaring.Bitmap) (ra *roaring.Bitmap) {
	result := make(map[uint64]uint64)
	exists := b.exists
	if filters != nil {
		exists.Intersect(filters)
	}

	mergeBits(exists, 0, result)
	mergeBits(b.sign.Intersect(exists), 1<<63, result)
	for i := range b.data {
		mergeBits(b.data[i].Intersect(exists), 1<<i, result)
	}
	ra = roaring.NewBitmap()
	for _, val := range result {
		ra.DirectAdd(uint64((2*(int64(val)>>63) + 1) * int64(val&^(1<<63))))
	}
	return
}

func mergeBits(ra *roaring.Bitmap, mask uint64, out map[uint64]uint64) {
	itr := ra.Iterator()
	itr.Seek(0)

	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		out[v] |= mask
	}
}

func absInt64(v int64) uint64 {
	switch {
	case v > 0:
		return uint64(v)
	case v == -9223372036854775808:
		return 9223372036854775808
	default:
		return uint64(-v)
	}
}
