package storage

import (
	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/raw"
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
)

func readRaw(tx *rbf.Tx, root uint32, shard uint64, filter *roaring.Bitmap, result *raw.BSI) error {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()
	max, err := cu.Max()
	if err != nil {
		return err
	}
	depth := max / shardwidth.ShardWidth
	return result.From(cu, shard, uint8(depth), filter)
}

func readRawKind(tx *rbf.Tx, root uint32, shard uint64, filter *roaring.Bitmap, kinds []Kind, result *[4]*roaring.Bitmap) error {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	for i := range kinds {
		row := uint64(kinds[i])
		rx, err := bitmaps.Row(cu, shard, row)
		if err != nil {
			return err
		}
		rx = rx.Intersect(filter)
		kind := row - 1
		*result[kind] = *result[kind].Union(rx)
	}
	return nil
}
