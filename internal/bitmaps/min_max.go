package bitmaps

import "github.com/gernest/roaring"

// Min returns the min of a given bsiGroup as well as the number of columns involved.
// A bitmap can be passed in to optionally filter the computed columns.
func Min(tx OffsetRanger, filter *roaring.Bitmap, shard uint64, bitDepth uint64) (min int64, count uint64, err error) {
	consider, err := Row(tx, shard, bsiExistsBit)
	if err != nil {
		return min, count, err
	} else if filter != nil {
		consider = consider.Intersect(filter)
	}

	// If there are no columns to consider, return early.
	if consider.Count() == 0 {
		return 0, 0, nil
	}

	// If we have negative values, we should find the highest unsigned value
	// from that set, then negate it, and return it. For example, if values
	// (-1, -2) exist, they are stored unsigned (1,2) with a negative sign bit
	// set. We take the highest of that set (2) and negate it and return it.
	if row, err := Row(tx, shard, bsiSignBit); err != nil {
		return min, count, err
	} else if row = row.Intersect(consider); row.Any() {
		min, count, err := maxUnsigned(tx, row, shard, bitDepth)
		return -min, count, err
	}

	// Otherwise find lowest positive number.
	return minUnsigned(tx, consider, shard, bitDepth)
}

// minUnsigned the lowest value without considering the sign bit. Filter is required.
func minUnsigned(tx OffsetRanger, filter *roaring.Bitmap, shard uint64, bitDepth uint64) (min int64, count uint64, err error) {
	count = filter.Count()
	for i := int(bitDepth - 1); i >= 0; i-- {
		row, err := Row(tx, shard, uint64(bsiOffsetBit+i))
		if err != nil {
			return min, count, err
		}
		row = filter.Difference(row)
		count = row.Count()
		if count > 0 {
			filter = row
		} else {
			min += (1 << uint(i))
			if i == 0 {
				count = filter.Count()
			}
		}
	}
	return min, count, nil
}

// Max returns the max of a given bsiGroup as well as the number of columns involved.
// A bitmap can be passed in to optionally filter the computed columns.
func Max(tx OffsetRanger, filter *roaring.Bitmap, shard uint64, bitDepth uint64) (max int64, count uint64, err error) {
	consider, err := Row(tx, shard, bsiExistsBit)
	if err != nil {
		return max, count, err
	} else if filter != nil {
		consider = consider.Intersect(filter)
	}

	// If there are no columns to consider, return early.
	if !consider.Any() {
		return 0, 0, nil
	}

	// Find lowest negative number w/o sign and negate, if no positives are available.
	row, err := Row(tx, shard, bsiSignBit)
	if err != nil {
		return max, count, err
	}
	pos := consider.Difference(row)
	if !pos.Any() {
		max, count, err = minUnsigned(tx, consider, shard, bitDepth)
		return -max, count, err
	}

	// Otherwise find highest positive number.
	return maxUnsigned(tx, pos, shard, bitDepth)
}

// maxUnsigned the highest value without considering the sign bit. Filter is required.
func maxUnsigned(tx OffsetRanger, filter *roaring.Bitmap, shard uint64, bitDepth uint64) (max int64, count uint64, err error) {
	count = filter.Count()
	for i := int(bitDepth - 1); i >= 0; i-- {
		row, err := Row(tx, shard, uint64(bsiOffsetBit+i))
		if err != nil {
			return max, count, err
		}
		row = row.Intersect(filter)

		count = row.Count()
		if count > 0 {
			max += (1 << uint(i))
			filter = row
		} else if i == 0 {
			count = filter.Count()
		}
	}
	return max, count, nil
}
