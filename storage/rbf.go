package storage

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"github.com/gernest/u128/bitmaps"
	"github.com/gernest/u128/checksum"
	"github.com/gernest/u128/rbf"
	"github.com/gernest/u128/storage/array"
	"github.com/gernest/u128/storage/keys"
)

var viewSamplesPool = &sync.Pool{New: func() any {
	return &ViewSamples{series: make(map[uint64]*roaring.Bitmap)}
}}

func openRBF(path string, _ struct{}) (*rbf.DB, error) {
	db := rbf.NewDB(path, nil)
	err := db.Open()
	if err != nil {
		return nil, err
	}
	return db, nil
}

// ViewSamples collects all samples in a single view.
type ViewSamples struct {
	labels       array.Uint64
	ts           array.Uint64
	values       array.Uint64
	histograms   array.Bool
	series       map[uint64]*roaring.Bitmap
	seriesLabels [][]byte
	histogram    map[uint64][]byte
	hasHistogram *roaring.Bitmap
}

func (v *ViewSamples) Release() {
	v.Reset()
	viewSamplesPool.Put(v)
}

func (v *ViewSamples) Reset() {
	v.labels.Reset()
	v.ts.Reset()
	v.values.Reset()
	v.histograms.Reset()
	clear(v.series)
	clear(v.seriesLabels)
	v.seriesLabels = v.seriesLabels[:0]
}

type Selector struct {
	Columns []checksum.U128
	Rows    []*roaring.Bitmap
	Negate  []bool
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

// StartTimestamp returns the first timestamp registered in the database.
func StartTimestamp(db *rbf.DB) (int64, error) {
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

// SearchTimeRange finds all columns matching startTs, endTs time range.
func SearchTimeRange(result *Selectors, db *rbf.DB, startTs, endTs int64) error {
	from, to := rbf.ViewUnixMilli(startTs), rbf.ViewUnixMilli(endTs)
	tx, err := db.Begin(false)
	if err != nil {
		return fmt.Errorf("creating read transaction %w", err)
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return fmt.Errorf("reading root records %w", err)
	}
	it := records.Iterator()
	it.Seek(rbf.Key{Column: keys.MetricsTimestamp, View: from})
	for {
		name, page, ok := it.Next()
		if !ok {
			break
		}
		// Upper view is inclusive.
		if name.View.Compare(&to) > 0 {
			break
		}
		if !bytes.Equal(name.Column[:], keys.MetricsTimestamp[:]) {
			break
		}

		ra, err := readBSIRange(tx, page, name.Shard, startTs, endTs)
		if err != nil {
			return fmt.Errorf("reading time range %w", err)
		}
		if !ra.Any() {
			continue
		}

		if !ra.Any() {
			continue
		}

		result.append(name.View, name.Shard, ra.Clone())
	}

	return nil
}

// SearchLabels reads matchers rbf indexes and applies them to selectors in result.
func SearchLabels(result *Selectors, db *rbf.DB, matchers *Matchers) error {
	if !matchers.Any() {
		// fast patch: there is no way to satisfy labels conditions.
		result.Reset()
		return nil
	}
	tx, err := db.Begin(false)
	if err != nil {
		return fmt.Errorf("creating read transaction %w", err)
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return fmt.Errorf("reading root records %w", err)
	}

	for i := range result.Views {
		view := result.Views[i]
		for j := range result.Shards[i] {
			shard := result.Shards[i][j]
			ra := result.Match[i][j]
			err = readFilters(tx, records, view, shard, matchers, func(m *roaring.Bitmap) error {
				result.Match[i][j] = ra.Intersect(m.Clone())
				return nil
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// SearchLabelsAny is like SearchLabels but uses union of matchers. Used for exemplars.
func SearchLabelsAny(result *Selectors, db *rbf.DB, matchers []*Matchers) error {
	tx, err := db.Begin(false)
	if err != nil {
		return fmt.Errorf("creating read transaction %w", err)
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return fmt.Errorf("reading root records %w", err)
	}

	for i := range result.Views {
		view := result.Views[i]
		for j := range result.Shards[i] {
			shard := result.Shards[i][j]
			ra := result.Match[i][j]
			rx := roaring.NewBitmap()
			for _, ma := range matchers {
				err = readFilters(tx, records, view, shard, ma, func(m *roaring.Bitmap) error {
					rx.UnionInPlace(m)
					return nil
				})
			}
			result.Match[i][j] = ra.Intersect(rx.Clone())
		}
	}
	return nil
}

func readFilters(tx *rbf.Tx, records *rbf.Records, view rbf.View, shard uint64, ma *Matchers, cb func(m *roaring.Bitmap) error) error {
	var ra *roaring.Bitmap
	for i := range ma.Columns {
		page, ok := records.Get(rbf.Key{Column: ma.Columns[i], Shard: shard, View: view})
		if !ok {
			return fmt.Errorf("missing page for column")
		}
		rx, err := readMutexRows(tx, page, shard, ma.Rows[i])
		if err != nil {
			return fmt.Errorf("reading labels rows %w", err)
		}
		if i == 0 {
			ra = rx
		} else {
			ra = ra.Intersect(rx)
		}
		if !ra.Any() {
			return cb(ra)
		}

	}
	return cb(ra)
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

func readBSI(tx *rbf.Tx, root uint32, shard uint64, filter *roaring.Bitmap, result []uint64) error {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	// compute bit depth
	mx, err := cu.Max()
	if err != nil {
		return fmt.Errorf("computing max value %w", err)
	}
	depth := mx / shardwidth.ShardWidth

	ex := bitmaps.NewExtractor()
	defer ex.Release()

	return ex.BSI(cu, uint8(depth), shard, filter, result)
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
