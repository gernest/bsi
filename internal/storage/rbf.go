package storage

import (
	"github.com/gernest/roaring"
	"github.com/gernest/u128/internal/bitmaps"
	"github.com/gernest/u128/internal/rbf"
	"github.com/gernest/u128/internal/storage/bsi"
)

func readBSIRange(tx *rbf.Tx, root uint32, shard uint64, depth uint8, op bitmaps.OP, predicate, end int64) (*roaring.Bitmap, error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	return bitmaps.Range(cu, op, shard, uint64(depth), predicate, end)
}

func readBSI(tx *rbf.Tx, root uint32, shard uint64, depth uint8, filter *roaring.Bitmap, result *bsi.BSI) error {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

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
