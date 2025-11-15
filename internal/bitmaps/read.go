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
