package storage

import (
	"fmt"

	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"github.com/gernest/u128/internal/bitmaps"
	"github.com/gernest/u128/internal/rbf"
	"github.com/gernest/u128/internal/storage/bsi"
)

func readBSIRange(tx *rbf.Tx, root uint32, shard uint64, op bitmaps.OP, predicate, end int64) (*roaring.Bitmap, error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	// compute bit depth
	mx, err := cu.Max()
	if err != nil {
		return nil, fmt.Errorf("computing max value %w", err)
	}
	depth := mx / shardwidth.ShardWidth
	return bitmaps.Range(cu, op, shard, depth, predicate, end)
}

func readBSI(tx *rbf.Tx, root uint32, shard uint64, filter *roaring.Bitmap, result *bsi.BSI) error {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	// compute bit depth
	mx, err := cu.Max()
	if err != nil {
		return fmt.Errorf("computing max value %w", err)
	}
	depth := mx / shardwidth.ShardWidth

	return result.From(cu, shard, uint8(depth), filter)
}

func readMutexRows(tx *rbf.Tx, root uint32, shard uint64, rows *roaring.Bitmap) (*roaring.Bitmap, error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	if rows.Count() == 1 {
		return bitmaps.Row(cu, shard, rows.Slice()[0])
	}
	all := make([]*roaring.Bitmap, rows.Count())
	var err error
	var i int
	for row := range rows.RangeAll() {
		all[i], err = bitmaps.Row(cu, shard, row)
		if err != nil {
			return nil, err
		}
		i++
	}
	return all[0].Union(all[1:]...), nil

}
