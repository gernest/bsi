package bitmaps

import (
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
)

// Row reads all column ids for rowID returning them as [roaring.Bitmap].
func Row(ra OffsetRanger, shard, rowID uint64) (*roaring.Bitmap, error) {
	return ra.OffsetRange(shardwidth.ShardWidth*shard,
		shardwidth.ShardWidth*rowID,
		shardwidth.ShardWidth*(rowID+1))
}

func Existence(tx OffsetRanger, shard uint64) (*roaring.Bitmap, error) {
	return Row(tx, shard, bsiExistsBit)
}

func Transpose(r OffsetRanger, bitDepth uint8, shard uint64, filter *roaring.Bitmap, cb func(int64)) error {
	exists, err := Row(r, shard, bsiExistsBit)
	if err != nil {
		return err
	}
	exists = exists.Intersect(filter)
	if !exists.Any() {
		return nil
	}
	data := make(map[uint64]uint64)

	mergeBits(exists, 0, data)

	sign, err := Row(r, shard, bsiSignBit)
	if err != nil {
		return err
	}
	mergeBits(sign, 1<<63, data)

	for i := range uint64(bitDepth) {
		bits, err := Row(r, shard, bsiOffsetBit+i)
		if err != nil {
			return err
		}
		bits = bits.Intersect(exists)
		mergeBits(bits, 1<<i, data)
	}

	itr := filter.Iterator()
	itr.Seek(0)
	var i int
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		val, ok := data[v]
		if ok {
			cb((2*(int64(val)>>63) + 1) * int64(val&^(1<<63)))
		}
		i++
	}
	return nil
}

func mergeBits(ra *roaring.Bitmap, mask uint64, out map[uint64]uint64) {
	itr := ra.Iterator()
	itr.Seek(0)
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		out[v] |= mask
	}
}
