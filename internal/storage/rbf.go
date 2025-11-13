package storage

import (
	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/raw"
	"github.com/gernest/roaring"
)

func readBSIRange(tx *rbf.Tx, root uint32, shard uint64, depth uint8, op bitmaps.OP, predicate, end int64) (*roaring.Bitmap, error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	return bitmaps.Range(cu, op, shard, uint64(depth), predicate, end)
}

func readBSIExists(tx *rbf.Tx, root uint32, shard uint64) (*roaring.Bitmap, error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	return bitmaps.Row(cu, shard, 0)
}

func readRaw(tx *rbf.Tx, root uint32, shard uint64, depth uint8, filter *roaring.Bitmap, result *raw.BSI) error {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	return result.From(cu, shard, uint8(depth), filter)
}
