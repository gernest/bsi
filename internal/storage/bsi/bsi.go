// Package bsi implements api for working with raw sample values read from rbf storage.
// We store sample Timestamp and Value as BSI encoded values, to simplify processing
// and improve memory footprint, we pass around these values in raw format and allow
// retrieving of individual values on demand.
package bsi

import (
	"math/bits"
	"slices"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/internal/bitmaps"
)

// BSI contains encoded BSI (column, value) tuple,
type BSI struct {
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

func (b *BSI) Any() bool {
	return b.exists.Any()
}

func (b *BSI) Init() {
	b.exists = roaring.NewBitmap()
	b.sign = roaring.NewBitmap()
}

func (b *BSI) Reset() {
	b.exists.Containers.Reset()
	b.sign.Containers.Reset()
	clear(b.data)
	b.data = b.data[:0]
}

// Optimize run optimize all bitmaps.
func (b *BSI) Optimize() {
	b.exists.Optimize()
	b.sign.Optimize()
	for i := range b.data {
		b.data[i].Optimize()
	}
}

// Union merges other bsi into b.
func (b *BSI) Union(other ...*BSI) {
	for _, o := range other {
		b.exists.UnionInPlace(o.exists)
		b.sign.UnionInPlace(o.sign)
		if len(b.data) < len(o.data) {
			b.data = slices.Grow(b.data, len(o.data))[:len(o.data)]
		}

		for i := range o.data {
			if b.data[i] == nil {
				b.data[i] = o.data[i]
				continue
			}
			b.data[i].UnionInPlace(o.data[i])
		}
	}
	b.Optimize()
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

// GetColumns returns al columns with given predicate.

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

func (b *BSI) GT(predicate int64, allowEquality bool) *roaring.Bitmap {
	if predicate == -1 && !allowEquality {
		predicate, allowEquality = 0, true
	}
	// Create predicate without sign bit.
	unsignedPredicate := absInt64(predicate)
	exists := b.exists
	switch {
	case predicate == 0 && !allowEquality:
		// Match all positive numbers except zero.
		exists = b.neq(0)

		fallthrough
	case predicate == 0 && allowEquality:
		// Match all positive numbers.
		return exists.Difference(b.sign)
	case predicate >= 0:
		// Match all positive numbers greater than the predicate.
		return b.ugt(exists.Difference(b.sign), unsignedPredicate, allowEquality)
	default:
		// Match all positives and greater negatives.
		neg := b.ult(exists.Intersect(b.sign), unsignedPredicate, allowEquality)

		pos := exists.Difference(b.sign)
		return pos.Union(neg)
	}
}

func (b *BSI) neq(predicate int64) *roaring.Bitmap {
	return b.EQ(predicate).Difference(b.exists)
}

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

func (b *BSI) ugt(filter *roaring.Bitmap, predicate uint64, allowEquality bool) *roaring.Bitmap {
	bitDepth := uint64(len(b.data))
prep:
	switch {
	case predicate == 0 && allowEquality:
		// This query matches all possible values.
		return filter
	case predicate == 0 && !allowEquality:
		// This query matches everything that is not 0.
		matches := roaring.NewBitmap()
		for i := uint64(0); i < bitDepth; i++ {
			row := b.data[i]
			matches = matches.Union(filter.Intersect(row))
		}
		return matches
	case !allowEquality && uint64(bits.Len64(predicate)) > bitDepth:
		// The predicate is bigger than the BSI width, so nothing can be bigger.
		return roaring.NewBitmap()
	case allowEquality:
		predicate--
		allowEquality = false
		goto prep
	}

	// Compare intermediate bits.
	matched := roaring.NewBitmap()
	remaining := filter
	predicate |= (^uint64(0)) << bitDepth
	for i := int(bitDepth - 1); i >= 0 && predicate < ^uint64(0) && remaining.Any(); i-- {
		row := b.data[i]
		ones := remaining.Intersect(row)
		switch (predicate >> uint(i)) & 1 {
		case 1:
			// Discard everything with a zero bit here.
			remaining = ones
		case 0:
			// Match everything with a one bit here.
			matched = matched.Union(ones)
			predicate |= 1 << uint(i)
		}
	}

	return matched
}

func (b *BSI) ult(filter *roaring.Bitmap, predicate uint64, allowEquality bool) *roaring.Bitmap {
	bitDepth := uint64(len(b.data))
	switch {
	case uint64(bits.Len64(predicate)) > bitDepth:
		fallthrough
	case predicate == (1<<bitDepth)-1 && allowEquality:
		// This query matches all possible values.
		return filter
	case predicate == (1<<bitDepth)-1 && !allowEquality:
		// This query matches everything that is not (1<<bitDepth)-1.
		matches := roaring.NewBitmap()
		for i := uint64(0); i < bitDepth; i++ {
			matches = matches.Union(filter.Difference(b.data[i]))
		}
		return matches
	case allowEquality:
		predicate++
	}

	// Compare intermediate bits.
	matched := roaring.NewBitmap()
	remaining := filter
	for i := int(bitDepth - 1); i >= 0 && predicate > 0 && remaining.Any(); i-- {
		row := b.data[i]
		zeroes := remaining.Difference(row)
		switch (predicate >> uint(i)) & 1 {
		case 1:
			// Match everything with a zero bit here.
			matched = matched.Union(zeroes)
			predicate &^= 1 << uint(i)
		case 0:
			// Discard everything with a one bit here.
			remaining = zeroes
		}
	}

	return matched
}

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
