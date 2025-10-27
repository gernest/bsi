// Package bsi implements api for working with raw sample values read from rbf storage.
// We store sample Timestamp and Value as BSI encoded values, to simplify processing
// and improve memory footprint, we pass around these values in raw format and allow
// retrieving of individual values on demand.
package bsi

import (
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

// New extracts bsi values from tx.
func New(tx bitmaps.OffsetRanger, shard uint64, bitDepth uint8, filter *roaring.Bitmap) (*BSI, error) {
	exists, err := bitmaps.Row(tx, shard, 0)
	if err != nil {
		return nil, err
	}
	if filter != nil {
		exists = exists.Intersect(filter)
	}
	o := &BSI{exists: exists.Clone(), data: make([]*roaring.Bitmap, 0, bitDepth)}
	if !exists.Any() {
		return o, nil
	}
	sign, err := bitmaps.Row(tx, shard, 1)
	if err != nil {
		return nil, err
	}
	if filter != nil {
		sign = sign.Intersect(filter)
	}
	o.sign = sign.Clone()

	for i := range uint64(bitDepth) {
		bits, err := bitmaps.Row(tx, shard, 2+i)
		if err != nil {
			return nil, err
		}
		bits = bits.Intersect(exists)
		o.data = append(o.data, bits.Clone())
	}
	o.Optimize()
	return o, nil
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
func (b *BSI) GetValue(column uint64) (value int64, exists bool) {
	if !b.exists.Contains(column) {
		return
	}
	var val uint64
	if b.sign.Contains(column) {
		val |= 1 << 63
	}
	for i := range b.data {
		if b.data[i].Contains(column) {
			val |= (1 << i)
		}
	}
	val = uint64((2*(int64(val)>>63) + 1) * int64(val&^(1<<63)))
	return int64(val), true
}
