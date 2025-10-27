package storage

import (
	"bytes"
	"fmt"
	"os"

	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"github.com/gernest/u128/internal/bitmaps"
	"github.com/gernest/u128/internal/rbf"
	"github.com/gernest/u128/internal/storage/bsi"
	"github.com/gernest/u128/internal/storage/keys"
)

func (db *Store) openRBF(key rbf.View, opts dataPath) (*rbf.DB, error) {
	da := rbf.NewDB(key.Path(opts.Path), nil)
	_, err := os.Stat(da.DataPath())
	created := os.IsNotExist(err)
	err = da.Open()
	if err != nil {
		return nil, err
	}
	if created {
		// update our views tree
		db.mu.Lock()
		db.tree.ReplaceOrInsert(key)
		db.mu.Unlock()
	}
	return da, nil
}

// Selectors defines which columns  to read from rbf.
type Selectors struct {
	Views  []rbf.View
	Shards [][]uint64
	Match  [][]*roaring.Bitmap
}

// Reset clears t for reuse.
func (t *Selectors) Reset() {
	t.Views = t.Views[:0]
	t.Shards = t.Shards[:0]
	t.Match = t.Match[:0]
}

func (t *Selectors) append(view rbf.View, shard uint64, ra *roaring.Bitmap) {
	if len(t.Views) == 0 || t.Views[len(t.Views)-1].Compare(&view) != 0 {
		t.Views = append(t.Views, view)
		t.Shards = append(t.Shards, nil)
		t.Match = append(t.Match, nil)
	}
	pos := len(t.Views) - 1
	t.Shards[pos] = append(t.Shards[pos], shard)
	t.Match[pos] = append(t.Match[pos], ra)
}

// startTimestamp returns the first timestamp registered in the database.
func startTimestamp(db *rbf.DB) (int64, error) {
	tx, err := db.Begin(false)
	if err != nil {
		return 0, fmt.Errorf("creating read transaction %w", err)
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return 0, fmt.Errorf("reading root records %w", err)
	}
	it := records.Iterator()
	it.Seek(rbf.Key{Column: keys.MetricsTimestamp})
	name, page, ok := it.Next()
	if !ok {
		return 0, nil
	}
	if !bytes.Equal(name.Column[:], keys.MetricsTimestamp[:]) {
		return 0, nil
	}
	start, _, err := readBSIMin(tx, page, name.Shard, nil)
	return start, nil
}

// readBSIRange performs a range search  in predicate...end bounds with upper bound being exclusive.
func readBSIRange(tx *rbf.Tx, root uint32, shard uint64, predicate, end int64) (*roaring.Bitmap, error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	// compute bit depth
	mx, err := cu.Max()
	if err != nil {
		return nil, fmt.Errorf("computing max value %w", err)
	}
	depth := mx / shardwidth.ShardWidth
	return bitmaps.Range(cu, bitmaps.BETWEEN, shard, depth, predicate, end)
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

func readBSIMin(tx *rbf.Tx, root uint32, shard uint64, filter *roaring.Bitmap) (min int64, count uint64, err error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	// compute bit depth
	mx, err := cu.Max()
	if err != nil {
		return 0, 0, fmt.Errorf("computing max value %w", err)
	}
	depth := mx / shardwidth.ShardWidth

	return bitmaps.Min(cu, filter, shard, depth)
}

func readHistogram(tx *rbf.Tx, root uint32, shard uint64, filter *roaring.Bitmap, result []bool) (*roaring.Bitmap, error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	yes, err := bitmaps.Row(cu, shard, 1)
	if err != nil {
		return nil, err
	}
	if !yes.Any() {
		return nil, nil
	}
	var i int
	for row := range filter.RangeAll() {
		result[i] = yes.Contains(row)
		i++
	}
	return yes.Intersect(filter), nil
}

func readFloatOrHistMetricType(tx *rbf.Tx, root uint32, shard uint64) (float, hist *roaring.Bitmap, err error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	float, err = bitmaps.Row(cu, shard, uint64(keys.Float))
	if err != nil {
		return
	}
	hist, err = bitmaps.Row(cu, shard, uint64(keys.Histogram))
	return

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
