// Copyright (c) Geofrey Ernest
// SPDX-License-Identifier: AGPL-3.0-only

package bitmaps

import (
	"io"
	"sync"

	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
)

// Cursor is an interface for traversing roaring bitmap containers.
type Cursor interface {
	First() error
	Close()
	Next() error
	Value() (uint64, *roaring.Container)
	Max() (uint64, error)
}

// Extractor reads values from roaring bitmaps.
type Extractor struct {
	filter [1 << shardVsContainerExponent]*roaring.Container
	data   map[uint64]uint64
}

var extractorPool = &sync.Pool{New: func() any {
	return &Extractor{data: make(map[uint64]uint64)}
}}

// NewExtractor initialize [Extractor] from pool.
func NewExtractor() *Extractor {
	return extractorPool.Get().(*Extractor)
}

// Release returns e to the pool.
func (b *Extractor) Release() {
	extractorPool.Put(b)
}

// Mutex returns a MUTEX encoded bitmap ra for all columns matching column ids set in match.
// Returned result is the same size and placement as match.Slice(). If no rows are found in a column then
// a nil []uint64{} is set.
//
// This is only used during testing. We only use MUTEX columns for search, we never read them. Reading high cardinality
// MUTEX fields is prohibitively expensive, it requires traversing whole bitmap tree searching for matching
// column ids.
func (b *Extractor) Mutex(ra Cursor, shard uint64, match *roaring.Bitmap) (result [][]uint64, err error) {
	result = make([][]uint64, match.Count())
	positions := buildPositions(match)

	filterIterator, _ := match.Containers.Iterator(0)
	for filterIterator.Next() {
		k, c := filterIterator.Value()
		if c.N() == 0 {
			continue
		}
		b.filter[k%(1<<shardVsContainerExponent)] = c
	}

	prevRow := ^uint64(0)
	seenThisRow := false
	off := highBits(shardwidth.ShardWidth * shard)
	for err = ra.First(); err == nil; err = ra.Next() {
		k, c := ra.Value()
		row := k >> shardVsContainerExponent
		if row == prevRow {
			if seenThisRow {
				continue
			}
		} else {
			seenThisRow = false
			prevRow = row
		}
		hi0 := highBits(shardwidth.ShardWidth * row)
		base := off + k - hi0
		roaring.IntersectionCallback(c, b.filter[k%(1<<shardVsContainerExponent)], func(u uint16) {
			if !seenThisRow {
				seenThisRow = true
			}
			id := base<<16 | uint64(u)
			idx := positions[id]
			result[idx] = append(result[idx], row)
		})
	}
	clear(b.filter[:])
	if err == io.EOF {
		err = nil
	}
	return
}

// MutexRa like mutex but we don't care about position. Returns all rows as a single bitmap.
func (b *Extractor) MutexRa(ra Cursor, match *roaring.Bitmap) (result *roaring.Bitmap, err error) {
	result = roaring.NewBitmap()

	filterIterator, _ := match.Containers.Iterator(0)
	for filterIterator.Next() {
		k, c := filterIterator.Value()
		if c.N() == 0 {
			continue
		}
		b.filter[k%(1<<shardVsContainerExponent)] = c
	}

	prevRow := ^uint64(0)
	seenThisRow := false
	for err = ra.First(); err == nil; err = ra.Next() {
		k, c := ra.Value()
		row := k >> shardVsContainerExponent
		if row == prevRow {
			if seenThisRow {
				continue
			}
		} else {
			seenThisRow = false
			prevRow = row
		}
		if roaring.IntersectionAny(c, b.filter[k%(1<<shardVsContainerExponent)]) {
			result.Add(row)
		}

	}
	clear(b.filter[:])
	if err == io.EOF {
		err = nil
	}
	return
}

// Enum reads very low cardinality enum fields. Ideally used to read stored protobuf enums defined in
// OpenTelemetry spec. Internally interpreted as MUTEX field with only one value per column id.
func (b *Extractor) Enum(ra Cursor, shard uint64, match *roaring.Bitmap) (result []uint64, err error) {
	result = make([]uint64, match.Count())
	positions := buildPositions(match)

	filterIterator, _ := match.Containers.Iterator(0)
	for filterIterator.Next() {
		k, c := filterIterator.Value()
		if c.N() == 0 {
			continue
		}
		b.filter[k%(1<<shardVsContainerExponent)] = c
	}

	prevRow := ^uint64(0)
	seenThisRow := false
	off := highBits(shardwidth.ShardWidth * shard)
	for err = ra.First(); err == nil; err = ra.Next() {
		k, c := ra.Value()
		row := k >> shardVsContainerExponent
		if row == prevRow {
			if seenThisRow {
				continue
			}
		} else {
			seenThisRow = false
			prevRow = row
		}
		hi0 := highBits(shardwidth.ShardWidth * row)
		base := off + k - hi0
		roaring.IntersectionCallback(c, b.filter[k%(1<<shardVsContainerExponent)], func(u uint16) {
			if !seenThisRow {
				seenThisRow = true
			}
			id := base<<16 | uint64(u)
			idx := positions[id]
			result[idx] = row
		})
	}
	if err == io.EOF {
		err = nil
	}
	clear(b.filter[:])
	return
}

func highBits(v uint64) uint64 { return v >> 16 }

// BSI reads stored int64. We return uint64 because we store variety of integers in BSI fields. It is
// up to the caller to cast to the desired type.
// The returned slice has the same size and placement as filter.Slice().
func (b *Extractor) BSI(r OffsetRanger, bitDepth uint8, shard uint64, filter *roaring.Bitmap) ([]uint64, error) {
	result := make([]uint64, filter.Count())

	exists, err := Row(r, shard, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	exists = exists.Intersect(filter)
	if !exists.Any() {
		return result, nil
	}

	mergeBits(exists, 0, b.data)

	sign, err := Row(r, shard, bsiSignBit)
	if err != nil {
		return nil, err
	}
	mergeBits(sign, 1<<63, b.data)

	for i := range uint64(bitDepth) {
		bits, err := Row(r, shard, bsiOffsetBit+i)
		if err != nil {
			return nil, err
		}
		bits = bits.Intersect(exists)
		mergeBits(bits, 1<<i, b.data)
	}

	itr := filter.Iterator()
	itr.Seek(0)
	var i int
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		val, ok := b.data[v]
		if ok {
			result[i] = uint64((2*(int64(val)>>63) + 1) * int64(val&^(1<<63)))
		}
		i++
	}
	clear(b.data)
	return result, nil
}

func mergeBits(ra *roaring.Bitmap, mask uint64, out map[uint64]uint64) {
	itr := ra.Iterator()
	itr.Seek(0)
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		out[v] |= mask
	}
}

func buildPositions(match *roaring.Bitmap) (m map[uint64]int) {
	m = make(map[uint64]int)
	itr := match.Iterator()
	itr.Seek(0)
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		m[v] = len(m)
	}
	return
}
