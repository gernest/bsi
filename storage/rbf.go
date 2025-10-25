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

// TimeRange contains results for columns satisfying time range constraint. We don't explicitly
// store shards because we can derive it
type TimeRange struct {
	Views []rbf.View
	Match []*roaring.Bitmap
}

// Reset clears t for reuse.
func (t *TimeRange) Reset() {
	t.Views = t.Views[:0]
	t.Match = t.Match[:0]
}

// SearchTimeRange finds all columns matching startTs, endTs time range.
func SearchTimeRange(result *TimeRange, db *rbf.DB, startTs, endTs int64) error {
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
		if len(result.Match) == 0 {
			result.Views = append(result.Views, name.View)
			result.Match = append(result.Match, ra.Clone())
			continue
		}
		if result.Views[len(result.Views)-1].Compare(&name.View) == 0 {
			result.Match[len(result.Views)-1] = result.Match[len(result.Views)-1].Union(ra)
			continue
		}
		result.Views = append(result.Views, name.View)
		result.Match = append(result.Match, ra.Clone())
	}

	return nil
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
