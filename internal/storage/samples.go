package storage

import (
	"context"
	"fmt"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/samples"
	"github.com/gernest/roaring"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"go.etcd.io/bbolt"
)

// Select implements storage.Querier.
//
// Search is not concurrent. Returned series is always sorted.
func (db *Store) Select(_ context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	shards, err := db.findShards(hints.Start, hints.End, matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	defer shardsPool.Put(shards)

	if shards.IsEmpty() {
		return storage.EmptySeriesSet()
	}

	var result samples.Samples
	result.Init()

	err = db.readTs(&result, shards, hints.Start, hints.End)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	err = db.translate(&result)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return result.Make()

}

func (db *Store) readTs(result *samples.Samples, vs *view, start, end int64) error {
	return db.read(vs, func(tx *rbf.Tx, records *rbf.Records, m meta) error {
		shard := m.shard
		tsP, ok := records.Get(MetricsTimestamp)
		if !ok {
			panic("missing ts root records")
		}
		ra, err := readBSIRange(tx, tsP, shard, m.depth.ts, bitmaps.BETWEEN, start, end)
		if err != nil {
			return err
		}
		if !ra.Any() {
			return nil
		}

		kind, ok := records.Get(MetricsType)
		if !ok {
			panic("missing metric type root records")
		}
		float, err := readBSIRange(tx, kind, shard, m.depth.kind, bitmaps.EQ, int64(Float), 0)
		if err != nil {
			return err
		}

		histogram, err := readBSIRange(tx, kind, shard, m.depth.kind, bitmaps.EQ, int64(Histogram), 0)
		if err != nil {
			return err
		}
		floatHistogram, err := readBSIRange(tx, kind, shard, m.depth.kind, bitmaps.EQ, int64(FloatHistogram), 0)
		if err != nil {
			return err
		}

		// metric samples can either be histograms or floats.
		ra = ra.Intersect(float.Union(histogram.Union(floatHistogram)))
		if !ra.Any() {
			return nil
		}

		ra, err = applyBSIFilters(tx, records, shard, ra, vs.match)
		if err != nil {
			return fmt.Errorf("applying filters %w", err)
		}

		return readSamples(result, m, tx, records, shard, ra)

	})
}

func (db *Store) translate(result *samples.Samples) error {
	readData := func(tx *bbolt.Tx, bucket []byte, values *roaring.Bitmap) {
		bu := tx.Bucket(bucket)
		readFromU64(bu, values, func(id uint64, value []byte) error {
			v := result.Own(value)
			result.Data[id] = v
			return nil
		})
	}

	return db.txt.View(func(tx *bbolt.Tx) error {
		if ra := result.KindBSI.EQ(int64(Histogram)); ra.Any() {
			readData(tx, histogramData, result.ValuesBSI.Transpose(ra))
		}
		if ra := result.KindBSI.EQ(int64(FloatHistogram)); ra.Any() {
			readData(tx, histogramData, result.ValuesBSI.Transpose(ra))
		}
		if ra := result.KindBSI.EQ(int64(Exemplar)); ra.Any() {
			readData(tx, exemplarData, result.ValuesBSI.Transpose(ra))
		}

		series := result.LabelsBSI.AsMap(nil)

		ra := roaring.NewBitmap()

		for k, v := range series {
			r, ok := result.Series[v]
			if !ok {
				r = roaring.NewBitmap()
				result.Series[v] = r
			}
			ra.DirectAdd(v)
			r.DirectAdd(k)
		}

		return readFromU64(tx.Bucket(metricsData), ra, func(id uint64, value []byte) error {
			result.SeriesData[id] = result.Own(value)
			return nil
		})
	})

}

func readSamples(result *samples.Samples, meta meta, tx *rbf.Tx, records *rbf.Records, shard uint64, match *roaring.Bitmap) error {
	ts := rawBSIPool.Get()
	kinds := rawBSIPool.Get()
	labels := rawBSIPool.Get()
	values := rawBSIPool.Get()

	defer func() {
		rawBSIPool.Put(ts)
		rawBSIPool.Put(kinds)
		rawBSIPool.Put(labels)
		rawBSIPool.Put(values)
	}()

	{
		root, ok := records.Get(MetricsTimestamp)
		if !ok {
			return fmt.Errorf("missing timestamp root record")
		}
		err := readRaw(tx, root, shard, meta.depth.ts, match, ts)
		if err != nil {
			return fmt.Errorf("reading timestamp %w", err)
		}
	}
	{
		root, ok := records.Get(MetricsType)
		if !ok {
			return fmt.Errorf("missing metric type root record")
		}
		err := readRaw(tx, root, shard, meta.depth.kind, match, kinds)
		if err != nil {
			return fmt.Errorf("reading metric type %w", err)
		}
	}
	{
		root, ok := records.Get(MetricsLabels)
		if !ok {
			return fmt.Errorf("missing labels root record")
		}
		err := readRaw(tx, root, shard, meta.depth.label, match, labels)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
	}
	{
		root, ok := records.Get(MetricsValue)
		if !ok {
			return fmt.Errorf("missing values root record")
		}
		err := readRaw(tx, root, shard, meta.depth.value, match, values)
		if err != nil {
			return fmt.Errorf("reading values %w", err)
		}
	}

	// everything was properly read. We update the samples now.
	result.TsBSI.Union(ts)
	result.KindBSI.Union(kinds)
	result.LabelsBSI.Union(labels)
	result.ValuesBSI.Union(values)
	return nil
}

func readSeries(result *samples.Samples, meta meta, tx *rbf.Tx, records *rbf.Records, shard uint64, match *roaring.Bitmap) error {

	// to avoid samples being in an incomplete state, we first read inti temporary bsi and update samples
	// wen we are done.
	labels := rawBSIPool.Get()
	defer rawBSIPool.Put(labels)

	root, ok := records.Get(MetricsLabels)
	if !ok {
		return fmt.Errorf("missing labels root record")
	}
	err := readRaw(tx, root, shard, meta.depth.label, match, labels)
	if err != nil {
		return fmt.Errorf("reading labels %w", err)
	}
	result.LabelsBSI.Union(labels)
	return nil
}

func readExemplars(result *samples.Samples, meta meta, tx *rbf.Tx, records *rbf.Records, shard uint64, match *roaring.Bitmap) error {

	kinds := rawBSIPool.Get()
	labels := rawBSIPool.Get()
	values := rawBSIPool.Get()

	defer func() {
		rawBSIPool.Put(kinds)
		rawBSIPool.Put(labels)
		rawBSIPool.Put(values)
	}()

	{
		root, ok := records.Get(MetricsType)
		if !ok {
			return fmt.Errorf("missing metric type root record")
		}
		err := readRaw(tx, root, shard, meta.depth.kind, match, kinds)
		if err != nil {
			return fmt.Errorf("reading metric type %w", err)
		}
	}
	{
		root, ok := records.Get(MetricsLabels)
		if !ok {
			return fmt.Errorf("missing labels root record")
		}
		err := readRaw(tx, root, shard, meta.depth.label, match, labels)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
	}
	{
		root, ok := records.Get(MetricsValue)
		if !ok {
			return fmt.Errorf("missing values root record")
		}
		err := readRaw(tx, root, shard, meta.depth.value, match, values)
		if err != nil {
			return fmt.Errorf("reading values %w", err)
		}
	}
	result.KindBSI.Union(kinds)
	result.LabelsBSI.Union(labels)
	result.ValuesBSI.Union(values)
	return nil
}
