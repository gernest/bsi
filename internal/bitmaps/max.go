package bitmaps

import "github.com/gernest/roaring"

// MaxUnsigned the highest value without considering the sign bit. Filter is required.
func MaxUnsigned(tx OffsetRanger, shard uint64, bitDepth uint8, filter *roaring.Bitmap) (max uint64, err error) {
	count := filter.Count()
	for i := int(bitDepth - 1); i >= 0; i-- {
		row, err := Row(tx, shard, uint64(bsiOffsetBit+i))
		if err != nil {
			return max, err
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
	return max, nil
}
