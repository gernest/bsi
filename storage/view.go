package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"

	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"github.com/gernest/u128/bitmaps"
	"github.com/gernest/u128/checksum"
	"github.com/gernest/u128/rbf"
	"github.com/gernest/u128/storage/array"
	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/keys"
	"github.com/gernest/u128/storage/magic"
	"github.com/gernest/u128/storage/tsid"
	"github.com/prometheus/prometheus/model/labels"
	"go.etcd.io/bbolt"
)

var (
	metricsSum    = []byte("sum")
	metricsData   = []byte("data")
	histogramData = []byte("floats")
	search        = []byte("index")
)

var viewSamplesPool = &sync.Pool{New: func() any {
	return &ViewSamples{series: make(map[uint64]*roaring.Bitmap)}
}}

// Unique ISO ISO 8601 (yer, week) tuple. Stores timeseries data using rbf for numerical
// data and bolt for non numerical data.
//
// Creates a total of 3 files.
// - data
// - wal
// - text
// dta and wal are managed by rbf and text is a bolt database.
type dbView struct {
	rbf  *rbf.DB
	meta *bbolt.DB
}

func (db *dbView) Close() error {
	return errors.Join(
		db.rbf.Close(),
		db.meta.Close(),
	)
}

// AllocateID assigns monotonically increasing sequences covering range size.
// Returns the upper bound which is exclusive. Count starts from 1.
//
// if size is 3 , it will return 4 which will yield sequence 1, 2, 3.
func (db *dbView) AllocateID(size uint64) (hi uint64, err error) {
	err = db.meta.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(metricsData)
		o := b.Sequence()
		if o == 0 {
			o++
		}
		hi = o + size
		return b.SetSequence(hi)
	})
	return
}

// GetTSID assigns tsid to labels .
func (db *dbView) GetTSID(out *tsid.B, labels [][]byte) error {

	// we make sure out.B has the same size as labels.
	size := len(labels)
	out.B = slices.Grow(out.B[:0], size)[:size]

	return db.meta.Update(func(tx *bbolt.Tx) error {
		metricsSumB := tx.Bucket(metricsSum)
		metricsDataB := tx.Bucket(metricsData)
		searchIndexB := tx.Bucket(search)

		for i := range labels {
			if i != 0 && bytes.Equal(labels[i], labels[i-1]) {
				// fast path: ingesting same series with multiple samples.
				out.B[i] = out.B[i-1]
				continue
			}
			sum := checksum.Hash(labels[i])
			if got := metricsSumB.Get(sum[:]); got != nil {
				// fast path: we have already processed labels in this view. We don't need
				// to do any more work.
				out.B[i].Decode(got)
				continue
			}

			// generate tsid
			id := &out.B[i]
			id.Reset()
			var err error
			id.ID, err = metricsSumB.NextSequence()
			if err != nil {
				return fmt.Errorf("generating metrics sequence %w", err)
			}

			// Building index
			for name, value := range buffer.RangeLabels(labels[i]) {
				labelNameB, err := searchIndexB.CreateBucketIfNotExists(name)
				if err != nil {
					return fmt.Errorf("creating label bucket %w", err)
				}
				view := checksum.Hash(name)

				if got := labelNameB.Get(value); got != nil {
					// fast path: we already assigned unique id for label value
					id.Views = append(id.Views, view)
					id.Rows = append(id.Rows, binary.BigEndian.Uint64(got))
				} else {
					// slow path: assign unique id to value
					nxt, err := labelNameB.NextSequence()
					if err != nil {
						return fmt.Errorf("assigning sequence id %w", err)
					}
					err = labelNameB.Put(value, binary.BigEndian.AppendUint64(nil, nxt))
					if err != nil {
						return fmt.Errorf("storing sequence id %w", err)
					}
					id.Views = append(id.Views, sum)
					id.Rows = append(id.Rows, nxt)
				}

			}

			// 2. store checksum => tsid in checksums bucket
			err = metricsSumB.Put(sum[:], id.Encode())
			if err != nil {
				return fmt.Errorf("storing metrics checksum %w", err)
			}

			// 3. store labels_sequence_id => labels_data in data bucket
			err = metricsDataB.Put(binary.BigEndian.AppendUint64(nil, id.ID), labels[i])
			if err != nil {
				return fmt.Errorf("storing metrics data %w", err)
			}
		}
		return nil
	})
}

func (db *dbView) TranslateHistogram(values []uint64, data [][]byte) error {
	return db.meta.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(histogramData)
		for i := range data {
			if len(data[i]) == 0 {
				continue
			}
			nxt, err := b.NextSequence()
			if err != nil {
				return fmt.Errorf("assigning histogram sequence %w", err)
			}
			values[i] = nxt
			err = b.Put(binary.BigEndian.AppendUint64(nil, nxt), data[i])
			if err != nil {
				return fmt.Errorf("storing histogram sequence %w", err)
			}
		}
		return nil
	})
}

// Apply implements DB.
func (db *dbView) Apply(data rbf.Map) error {
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

// Search all shards form matching data.
func (db *dbView) Search(result *ViewSamples, startTs, endTs int64, selectors []*labels.Matcher, union bool) error {
	matchers := map[string]*roaring.Bitmap{}
	err := db.buildIndex(selectors, func(field string, ra *roaring.Bitmap) error {
		matchers[field] = ra
		return nil
	})
	if err != nil {
		return err
	}

	for _, l := range selectors {
		switch l.Type {
		case labels.MatchEqual, labels.MatchRegexp:
			if _, ok := matchers[l.Name]; !ok {
				//fast path: we will never be able to fulfil match conditions.
				// we know for a fact that the label values are not in our database.
				return nil
			}
		}
	}
	tx, err := db.rbf.Begin(false)
	if err != nil {
		return fmt.Errorf("creating read transaction %w", err)
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return fmt.Errorf("reading root records %w", err)
	}

	filterSum := make([]checksum.U128, 0, len(matchers))
	filterSRa := make([]*roaring.Bitmap, 0, len(matchers))
	filters := make([]*roaring.Bitmap, 0, len(matchers))

	for k, v := range matchers {
		filterSum = append(filterSum, checksum.Hash(magic.Slice(k)))
		filterSRa = append(filterSRa, v)
	}

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

		for i := range filterSum {
			pg, ok := records.Get(rbf.Key{Column: filterSum[i], Shard: name.Shard})
			if !ok {
				filters = append(filters, roaring.NewBitmap())
				continue
			}
			a, err := readMutexRows(tx, pg, name.Shard, filterSRa[i])
			if err != nil {
				return fmt.Errorf("reading filter rows %w", err)
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
		if err != nil {
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

	// read series translation from store
	return db.meta.View(func(tx *bbolt.Tx) error {

		cu := tx.Bucket(metricsData).Cursor()

		var lo, hi [8]byte
		for a, b := range rangeSets(all) {
			binary.BigEndian.PutUint64(lo[:], a)
			binary.BigEndian.PutUint64(hi[:], b)

			for k, v := cu.Seek(lo[:]); v != nil && bytes.Compare(k, hi[:]) < 1; k, v = cu.Next() {

				// we are iterating in the same order as labels slice.
				result.seriesLabels = append(result.seriesLabels, bytes.Clone(v))
			}
		}

		if len(result.seriesLabels) != len(all) {
			panic(fmt.Sprintf("invalid labels state want %d labels got %d", len(all), len(result.series)))
		}
		if result.hasHistogram.Any() {
			// translate histograms
			cu = tx.Bucket(histogramData).Cursor()
			var lo, hi [8]byte
			for a, b := range rangeSets(all) {
				binary.BigEndian.PutUint64(lo[:], a)
				binary.BigEndian.PutUint64(hi[:], b)

				for k, v := cu.Seek(lo[:]); v != nil && bytes.Compare(k, hi[:]) < 1; k, v = cu.Next() {
					key := binary.BigEndian.Uint64(k)
					result.histogram[key] = bytes.Clone(v)
				}
			}
		}
		return nil
	})
}

func (db *dbView) buildIndex(matchers []*labels.Matcher, cb func(field string, ra *roaring.Bitmap) error) error {
	return db.meta.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(search)
		for _, l := range matchers {
			sb := b.Bucket(magic.Slice(l.Name))
			if sb == nil {
				continue
			}
			switch l.Type {
			case labels.MatchEqual, labels.MatchNotEqual:
				v := sb.Get(magic.Slice(l.Value))
				if v != nil {
					err := cb(l.Name, roaring.NewBitmap(
						binary.BigEndian.Uint64(v),
					))
					if err != nil {
						return err
					}
				}
			case labels.MatchRegexp, labels.MatchNotRegexp:
				cu := sb.Cursor()
				px := magic.Slice(l.Prefix())
				ra := roaring.NewBitmap()
				for k, v := cu.Seek(px); v != nil && bytes.HasPrefix(k, px); k, v = cu.Next() {
					ra.DirectAdd(
						binary.BigEndian.Uint64(v),
					)
				}
				err := cb(l.Name, ra)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
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

func rangeSets(ra []uint64) iter.Seq2[uint64, uint64] {
	return func(yield func(uint64, uint64) bool) {
		start := uint64(0)
		end := uint64(0)
		for i := range ra {
			v := ra[i]
			if start == 0 {
				start = v
				end = v
				continue
			}
			if v-end < 2 {
				end = v
				continue
			}
			if !yield(start, end) {
				return
			}
			start = v
			end = v
		}
		if start != 0 {
			yield(start, end)
		}
	}
}

func rangeSetsRa(ra *roaring.Bitmap) iter.Seq2[uint64, uint64] {
	return func(yield func(uint64, uint64) bool) {
		start := uint64(0)
		end := uint64(0)
		for v := range ra.RangeAll() {
			if start == 0 {
				start = v
				end = v
				continue
			}
			if v-end < 2 {
				end = v
				continue
			}
			if !yield(start, end) {
				return
			}
			start = v
			end = v
		}
		if start != 0 {
			yield(start, end)
		}
	}
}
