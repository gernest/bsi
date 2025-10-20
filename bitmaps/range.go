// Copyright (c) Geofrey Ernest
// SPDX-License-Identifier: AGPL-3.0-only

package bitmaps

import (
	"math/bits"

	"github.com/gernest/roaring"
)

const (
	bsiExistsBit = 0
	bsiSignBit   = 1
	bsiOffsetBit = 2
)

// OP defines binary operation on a int64 value.
type OP byte

// Supported operands.
const (
	EQ  OP = 1 + iota // ==
	NEQ               // !=
	LT                // <
	LTE               // <=
	GT                // >
	GTE               // >=

	BETWEEN // ><  (this is like predicate <= x <= end)
)

// OffsetRanger is an interface for reading [roaring.Bitmap] with keys constraints.
type OffsetRanger interface {
	// OffsetRange returns a *roaring.Bitmap containing the portion of the Bitmap for the fragment
	// which is specified by a combination of (offset, [start, end)).
	//
	// start  - The value at which to start reading. This must be the zero value
	//          of a container; i.e. [0, 65536, ...]
	// end    - The value at which to end reading. This must be the zero value
	//          of a container; i.e. [0, 65536, ...]
	// offset - The number of positions to shift the resulting bitmap. This must
	//          be the zero value of a container; i.e. [0, 65536, ...]
	//
	// For example, if (index, field, view, shard) represents the following bitmap:
	// [1, 2, 3, 65536, 65539]
	//
	// then the following results are achieved based on (offset, start, end):
	// (0, 0, 131072)          => [1, 2, 3, 65536, 65539]
	// (0, 65536, 131072)      => [0, 3]
	// (65536, 65536, 131072)  => [65536, 65539]
	// (262144, 65536, 131072) => [262144, 262147]
	//
	OffsetRange(offset, start, end uint64) (*roaring.Bitmap, error)
}

// RoaringRange wraps [roaring.Bitmap] to implement [OffsetRanger] interface.
type RoaringRange struct {
	ra *roaring.Bitmap
}

var _ OffsetRanger = (*RoaringRange)(nil)

// NewRoaringRange initialize [RoaringRange].
func NewRoaringRange(ra *roaring.Bitmap) *RoaringRange {
	return &RoaringRange{ra: ra}
}

// OffsetRange implements [OffsetRange].
func (r *RoaringRange) OffsetRange(offset, start, end uint64) (*roaring.Bitmap, error) {
	return r.ra.OffsetRange(offset, start, end), nil
}

// Range search roaring bitmap tx for predicate matching op. bitDepth determines how deep in the bitmap tree
// we search for relevant bits.
// end is only use when OP == BETWEEN  and it is upper exclusive, i.e `predicate <= VALUE < end“.
// USed on BSI fields.
func Range(tx OffsetRanger, op OP, shard, bitDepth uint64, predicate, end int64) (*roaring.Bitmap, error) {
	switch op {
	case EQ:
		return rangeEQ(tx, shard, bitDepth, predicate)
	case NEQ:
		return rangeNEQ(tx, shard, bitDepth, predicate)
	case LT, LTE:
		return rangeLT(tx, shard, bitDepth, predicate, op == LTE)
	case GT, GTE:
		return rangeGT(tx, shard, bitDepth, predicate, op == GTE)
	case BETWEEN:
		return rangeBetween(tx, shard, bitDepth, predicate, end)
	default:
		return roaring.NewBitmap(), nil
	}
}

func rangeEQ(tx OffsetRanger, shard, bitDepth uint64, predicate int64) (*roaring.Bitmap, error) {
	b, err := Row(tx, shard, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	if !b.Any() {
		return b, nil
	}
	unsignedPredicate := absInt64(predicate)
	if uint64(bits.Len64(unsignedPredicate)) > bitDepth {
		// Predicate is out of range.
		return roaring.NewBitmap(), nil
	}
	r, err := Row(tx, shard, bsiSignBit)
	if err != nil {
		return nil, err
	}
	if predicate < 0 {
		b = b.Intersect(r) // only negatives
	} else {
		b = b.Difference(r) // only positives
	}
	for i := int(bitDepth - 1); i >= 0; i-- {
		row, err := Row(tx, shard, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}

		bit := (unsignedPredicate >> uint(i)) & 1

		if bit == 1 {
			b = b.Intersect(row)
		} else {
			b = b.Difference(row)
		}
	}
	return b, nil
}

func rangeNEQ(tx OffsetRanger, shard, bitDepth uint64, predicate int64) (*roaring.Bitmap, error) {
	// Start with set of columns with values set.
	b, err := Row(tx, shard, bsiExistsBit)
	if err != nil {
		return nil, err
	}

	// Get the equal bitmap.
	eq, err := rangeEQ(tx, shard, bitDepth, predicate)
	if err != nil {
		return nil, err
	}
	// Not-null minus the equal bitmap.
	b = b.Difference(eq)
	return b, nil
}

func rangeLT(tx OffsetRanger, shard, bitDepth uint64, predicate int64, allowEquality bool) (*roaring.Bitmap, error) {
	if predicate == 1 && !allowEquality {
		predicate, allowEquality = 0, true
	}

	// Start with set of columns with values set.
	b, err := Row(tx, shard, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	// Get the sign bit row.
	sign, err := Row(tx, shard, bsiSignBit)
	if err != nil {
		return nil, err
	}
	// Create predicate without sign bit.
	unsignedPredicate := absInt64(predicate)

	switch {
	case predicate == 0 && !allowEquality:
		// Match all negative integers.
		return b.Intersect(sign), nil
	case predicate == 0 && allowEquality:
		// Match all integers that are either negative or 0.
		zeroes, err := rangeEQ(tx, shard, bitDepth, 0)
		if err != nil {
			return nil, err
		}
		return b.Intersect(sign).Union(zeroes), nil
	case predicate < 0:
		// Match all every negative number beyond the predicate.
		return rangeGTUnsigned(tx, b.Intersect(sign), shard, bitDepth, unsignedPredicate, allowEquality)
	default:
		// Match positive numbers less than the predicate, and all negatives.
		pos, err := rangeLTUnsigned(tx, b.Difference(sign), shard, bitDepth, unsignedPredicate, allowEquality)
		if err != nil {
			return nil, err
		}
		neg := b.Intersect(sign)
		return pos.Union(neg), nil
	}
}

func rangeLTUnsigned(tx OffsetRanger, filter *roaring.Bitmap, shard, bitDepth, predicate uint64, allowEquality bool) (*roaring.Bitmap, error) {
	switch {
	case uint64(bits.Len64(predicate)) > bitDepth:
		fallthrough
	case predicate == (1<<bitDepth)-1 && allowEquality:
		// This query matches all possible values.
		return filter, nil
	case predicate == (1<<bitDepth)-1 && !allowEquality:
		// This query matches everything that is not (1<<bitDepth)-1.
		matches := roaring.NewBitmap()
		for i := uint64(0); i < bitDepth; i++ {
			row, err := Row(tx, shard, bsiOffsetBit+i)
			if err != nil {
				return nil, err
			}
			matches = matches.Union(filter.Difference(row))
		}
		return matches, nil
	case allowEquality:
		predicate++
	}

	// Compare intermediate bits.
	matched := roaring.NewBitmap()
	remaining := filter
	for i := int(bitDepth - 1); i >= 0 && predicate > 0 && remaining.Any(); i-- {
		row, err := Row(tx, shard, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
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

	return matched, nil
}

func rangeGT(tx OffsetRanger, shard, bitDepth uint64, predicate int64, allowEquality bool) (*roaring.Bitmap, error) {
	if predicate == -1 && !allowEquality {
		predicate, allowEquality = 0, true
	}

	b, err := Row(tx, shard, bsiExistsBit)
	if err != nil {
		return nil, err
	}

	// Create predicate without sign bit.
	unsignedPredicate := absInt64(predicate)

	sign, err := Row(tx, shard, bsiSignBit)
	if err != nil {
		return nil, err
	}
	switch {
	case predicate == 0 && !allowEquality:
		// Match all positive numbers except zero.
		nonzero, err := rangeNEQ(tx, shard, bitDepth, 0)
		if err != nil {
			return nil, err
		}
		b = nonzero
		fallthrough
	case predicate == 0 && allowEquality:
		// Match all positive numbers.
		return b.Difference(sign), nil
	case predicate >= 0:
		// Match all positive numbers greater than the predicate.
		return rangeGTUnsigned(tx, b.Difference(sign), shard, bitDepth, unsignedPredicate, allowEquality)
	default:
		// Match all positives and greater negatives.
		neg, err := rangeLTUnsigned(tx, b.Intersect(sign), shard, bitDepth, unsignedPredicate, allowEquality)
		if err != nil {
			return nil, err
		}
		pos := b.Difference(sign)
		return pos.Union(neg), nil
	}
}

func rangeGTUnsigned(tx OffsetRanger, filter *roaring.Bitmap, shard, bitDepth, predicate uint64, allowEquality bool) (*roaring.Bitmap, error) {
prep:
	switch {
	case predicate == 0 && allowEquality:
		// This query matches all possible values.
		return filter, nil
	case predicate == 0 && !allowEquality:
		// This query matches everything that is not 0.
		matches := roaring.NewBitmap()
		for i := uint64(0); i < bitDepth; i++ {
			row, err := Row(tx, shard, bsiOffsetBit+i)
			if err != nil {
				return nil, err
			}
			matches = matches.Union(filter.Intersect(row))
		}
		return matches, nil
	case !allowEquality && uint64(bits.Len64(predicate)) > bitDepth:
		// The predicate is bigger than the BSI width, so nothing can be bigger.
		return roaring.NewBitmap(), nil
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
		row, err := Row(tx, shard, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
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

	return matched, nil
}

func rangeBetween(tx OffsetRanger, shard, bitDepth uint64, predicateMin, predicateMax int64) (*roaring.Bitmap, error) {
	b, err := Row(tx, shard, bsiExistsBit)
	if err != nil {
		return nil, err
	}

	// Convert predicates to unsigned values.
	unsignedPredicateMin, unsignedPredicateMax := absInt64(predicateMin), absInt64(predicateMax)

	switch {
	case predicateMin == predicateMax:
		return rangeEQ(tx, shard, bitDepth, predicateMin)
	case predicateMin >= 0:
		// Handle positive-only values.
		r, err := Row(tx, shard, bsiSignBit)
		if err != nil {
			return nil, err
		}
		return rangeBetweenUnsigned(tx, b.Difference(r), shard, bitDepth, unsignedPredicateMin, unsignedPredicateMax)
	case predicateMax < 0:
		// Handle negative-only values. Swap unsigned min/max predicates.
		r, err := Row(tx, shard, bsiSignBit)
		if err != nil {
			return nil, err
		}
		return rangeBetweenUnsigned(tx, b.Intersect(r), shard, bitDepth, unsignedPredicateMax, unsignedPredicateMin)
	default:
		// If predicate crosses positive/negative boundary then handle separately and union.
		r0, err := Row(tx, shard, bsiSignBit)
		if err != nil {
			return nil, err
		}
		pos, err := rangeLTUnsigned(tx, b.Difference(r0), shard, bitDepth, unsignedPredicateMax, true)
		if err != nil {
			return nil, err
		}
		r1, err := Row(tx, shard, bsiSignBit)
		if err != nil {
			return nil, err
		}
		neg, err := rangeLTUnsigned(tx, b.Intersect(r1), shard, bitDepth, unsignedPredicateMin, true)
		if err != nil {
			return nil, err
		}
		return pos.Union(neg), nil
	}
}

func rangeBetweenUnsigned(tx OffsetRanger, filter *roaring.Bitmap, shard, bitDepth, predicateMin, predicateMax uint64) (*roaring.Bitmap, error) {
	switch {
	case predicateMax > (1<<bitDepth)-1:
		// The upper bound cannot be violated.
		return rangeGTUnsigned(tx, filter, shard, bitDepth, predicateMin, true)
	case predicateMin == 0:
		// The lower bound cannot be violated.
		return rangeLTUnsigned(tx, filter, shard, bitDepth, predicateMax, true)
	}

	// Compare any upper bits which are equal.
	diffLen := bits.Len64(predicateMax ^ predicateMin)
	remaining := filter
	for i := int(bitDepth - 1); i >= diffLen; i-- {
		row, err := Row(tx, shard, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
		switch (predicateMin >> uint(i)) & 1 {
		case 1:
			remaining = remaining.Intersect(row)
		case 0:
			remaining = remaining.Difference(row)
		}
	}

	// Clear the bits we just compared.
	equalMask := (^uint64(0)) << diffLen
	predicateMin &^= equalMask
	predicateMax &^= equalMask

	remaining, err := rangeGTUnsigned(tx, remaining, shard, uint64(diffLen), predicateMin, true)
	if err != nil {
		return nil, err
	}
	return rangeLTUnsigned(tx, remaining, shard, uint64(diffLen), predicateMax, true)
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
