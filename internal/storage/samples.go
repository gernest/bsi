package storage

import (
	"context"
	"fmt"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/keys"
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
	tx, err := db.rbf.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return err
	}

	for i := range vs.meta {
		shard := uint64(vs.meta[i].shard)
		tsP, ok := records.Get(rbf.Key{Column: keys.MetricsTimestamp, Shard: shard})
		if !ok {
			panic("missing ts root records")
		}
		ra, err := readBSIRange(tx, tsP, shard, vs.meta[i].depth.ts, bitmaps.BETWEEN, start, end)
		if err != nil {
			return err
		}
		if !ra.Any() {
			continue
		}

		kind, ok := records.Get(rbf.Key{Column: keys.MetricsType, Shard: shard})
		if !ok {
			panic("missing metric type root records")
		}
		float, err := readBSIRange(tx, kind, shard, vs.meta[i].depth.kind, bitmaps.EQ, int64(keys.Float), 0)
		if err != nil {
			return err
		}

		histogram, err := readBSIRange(tx, kind, shard, vs.meta[i].depth.kind, bitmaps.EQ, int64(keys.Histogram), 0)
		if err != nil {
			return err
		}
		floatHistogram, err := readBSIRange(tx, kind, shard, vs.meta[i].depth.kind, bitmaps.EQ, int64(keys.FloatHistogram), 0)
		if err != nil {
			return err
		}

		// metric samples can either be histograms or floats.
		ra = ra.Intersect(float.Union(histogram.Union(floatHistogram)))
		if !ra.Any() {
			continue
		}

		ra, err = applyBSIFilters(tx, records, shard, ra, vs.match)
		if err != nil {
			return fmt.Errorf("applying filters %w", err)
		}

		err = readSamples(result, vs.meta[i], tx, records, shard, ra)
		if err != nil {
			return err
		}
	}
	return nil
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
		if ra := result.KindBSI.EQ(int64(keys.Histogram)); ra.Any() {
			readData(tx, histogramData, result.ValuesBSI.Transpose(ra))
		}
		if ra := result.KindBSI.EQ(int64(keys.FloatHistogram)); ra.Any() {
			readData(tx, histogramData, result.ValuesBSI.Transpose(ra))
		}
		if ra := result.KindBSI.EQ(int64(keys.Exemplar)); ra.Any() {
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

	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsTimestamp, Shard: shard})
		if !ok {
			return fmt.Errorf("missing timestamp root record")
		}
		err := readRaw(tx, root, shard, meta.depth.ts, match, &result.TsBSI)
		if err != nil {
			return fmt.Errorf("reading timestamp %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsType, Shard: shard})
		if !ok {
			return fmt.Errorf("missing metric type root record")
		}
		err := readRaw(tx, root, shard, meta.depth.kind, match, &result.KindBSI)
		if err != nil {
			return fmt.Errorf("reading metric type %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsLabels, Shard: shard})
		if !ok {
			return fmt.Errorf("missing labels root record")
		}
		err := readRaw(tx, root, shard, meta.depth.label, match, &result.LabelsBSI)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsValue, Shard: shard})
		if !ok {
			return fmt.Errorf("missing values root record")
		}
		err := readRaw(tx, root, shard, meta.depth.value, match, &result.ValuesBSI)
		if err != nil {
			return fmt.Errorf("reading values %w", err)
		}
	}
	return nil
}

func readSeries(result *samples.Samples, meta meta, tx *rbf.Tx, records *rbf.Records, shard uint64, match *roaring.Bitmap) error {
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsLabels, Shard: shard})
		if !ok {
			return fmt.Errorf("missing labels root record")
		}
		err := readRaw(tx, root, shard, meta.depth.label, match, &result.LabelsBSI)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
	}
	return nil
}

func readExemplars(result *samples.Samples, meta meta, tx *rbf.Tx, records *rbf.Records, shard uint64, match *roaring.Bitmap) error {

	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsType, Shard: shard})
		if !ok {
			return fmt.Errorf("missing metric type root record")
		}
		err := readRaw(tx, root, shard, meta.depth.kind, match, &result.TsBSI)
		if err != nil {
			return fmt.Errorf("reading metric type %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsLabels, Shard: shard})
		if !ok {
			return fmt.Errorf("missing labels root record")
		}
		err := readRaw(tx, root, shard, meta.depth.label, match, &result.LabelsBSI)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsValue, Shard: shard})
		if !ok {
			return fmt.Errorf("missing values root record")
		}
		err := readRaw(tx, root, shard, meta.depth.value, match, &result.ValuesBSI)
		if err != nil {
			return fmt.Errorf("reading values %w", err)
		}
	}
	return nil
}
