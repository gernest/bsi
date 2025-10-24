package storage

import (
	"bytes"
	"fmt"
	"slices"
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

type rbfDB struct {
	rbf *rbf.DB
}

func (db *rbfDB) Close() error {
	return db.rbf.Close()
}

// GetTSID assigns tsid to labels .

// Apply implements DB.
func (db *rbfDB) Apply(data rbf.Map) error {
	tx, err := db.rbf.Begin(true)
	if err != nil {
		return fmt.Errorf("creating write transaction %w", err)
	}
	defer tx.Rollback()

	for k, v := range data {
		_, err := tx.AddRoaring(k, v)
		if err != nil {
			return fmt.Errorf("storing bitmap %v %w", k, err)
		}
	}
	return nil
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

// Search all shards form matching data.
func (db *rbfDB) Search(result *ViewSamples, startTs, endTs int64, selector Selector, union bool) error {

	tx, err := db.rbf.Begin(false)
	if err != nil {
		return fmt.Errorf("creating read transaction %w", err)
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return fmt.Errorf("reading root records %w", err)
	}
	filters := make([]*roaring.Bitmap, 0, len(selector.Columns))
	// Iterate over all shards for the timestamp column.
	it := records.Iterator()
	it.Seek(rbf.Key{Column: keys.MetricsTimestamp})
	for {
		name, page, ok := it.Next()
		if !ok {
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

		// we have matching timestamp for the current shard. Try to apply the filters.
		filters = filters[:0]

		for i := range selector.Columns {
			pg, ok := records.Get(rbf.Key{Column: selector.Columns[i], Shard: name.Shard})
			if !ok {
				filters = append(filters, roaring.NewBitmap())
				continue
			}
			a, err := readMutexRows(tx, pg, name.Shard, selector.Rows[i])
			if err != nil {
				return fmt.Errorf("reading filter rows %w", err)
			}
			if selector.Negate[i] {
				a = ra.Difference(a)
			}
			filters = append(filters, a)
		}
		if len(filters) > 0 {
			a := filters[0]
			if len(filters) > 1 {
				if union {
					a.Freeze()
					a.Union()
					a.UnionInPlace(filters[1:]...)
				} else {
					for _, n := range filters[1:] {
						a = a.Intersect(n)
					}
				}
			}

			// ra acts as existence bitmap. By intersecting with filter we yiled
			// columns which satisfies the filter that exists in the current shard.
			ra = ra.Intersect(a)
		}

		if !ra.Any() {
			continue
		}
		count := int(ra.Count())
		offset := result.ts.Len()
		ts := result.ts.Allocate(count)
		result.labels.Reset()
		labels := result.labels.Allocate(count)
		values := result.values.Allocate(count)
		histograms := result.histograms.Allocate(count)

		err = readBSI(tx, page, name.Shard, ra, ts)
		if err != nil {
			return fmt.Errorf("reading sample timestamp %w", err)
		}
		labelsPage, ok := records.Get(rbf.Key{Column: keys.MetricsLabels, Shard: name.Shard})
		if !ok {
			return fmt.Errorf("missing sample labels bitmap")
		}
		err = readBSI(tx, labelsPage, name.Shard, ra, labels)
		if err != nil {
			return fmt.Errorf("reading sample labels %w", err)
		}
		valuesPage, ok := records.Get(rbf.Key{Column: keys.MetricsValue, Shard: name.Shard})
		if !ok {
			return fmt.Errorf("missing sample values bitmap")
		}
		err = readBSI(tx, valuesPage, name.Shard, ra, values)
		if err != nil {
			return fmt.Errorf("reading sample values %w", err)
		}
		histogramPage, ok := records.Get(rbf.Key{Column: keys.MetricsHistogram, Shard: name.Shard})
		if !ok {
			return fmt.Errorf("missing sample histogram bitmap")
		}
		h, err := readHistogram(tx, histogramPage, name.Shard, ra, histograms)
		if err != nil {
			return fmt.Errorf("reading sample histograms %w", err)
		}
		if h != nil {
			result.hasHistogram = result.hasHistogram.Union(ra)
		}

		for i := range ts {
			sx, ok := result.series[labels[i]]
			if !ok {
				sx = roaring.NewBitmap()
				result.series[labels[i]] = sx
			}
			sx.DirectAdd(uint64(offset) + uint64(i))
		}
	}

	if result.ts.Len() == 0 {
		return nil
	}

	result.labels.Reset()
	all := result.labels.Allocate(len(result.series))
	all = all[:0]

	for a := range result.series {
		all = append(all, a)
	}
	slices.Sort(all)
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
