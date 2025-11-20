package storage

import (
	"context"
	"fmt"

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
	var shards view
	err := db.findShards(&shards, matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	var result samples.Samples
	result.Init()

	err = db.readTs(&result, &shards, hints.Start, hints.End)
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
	return db.read(vs, start, end, baseMetrics, func(tx *rbf.Tx, records *rbf.Records, ra *roaring.Bitmap, shard uint64, kinds Kinds) error {
		return readSamples(result, tx, records, shard, ra, kinds)
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
		if ra := result.Kinds[Histogram-1]; ra.Any() {
			readData(tx, histogramData, result.ValuesBSI.Transpose(ra))
		}
		if ra := result.Kinds[FloatHistogram-1]; ra.Any() {
			readData(tx, histogramData, result.ValuesBSI.Transpose(ra))
		}
		if ra := result.Kinds[Exemplar-1]; ra.Any() {
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

func readSamples(result *samples.Samples, tx *rbf.Tx, records *rbf.Records, shard uint64, match *roaring.Bitmap, kinds Kinds) error {

	{
		root, ok := records.Get(rbf.Key{Column: MetricsTimestamp, Shard: shard})
		if !ok {
			return fmt.Errorf("missing timestamp root record")
		}
		err := readRaw(tx, root, shard, match, &result.TsBSI)
		if err != nil {
			return fmt.Errorf("reading timestamp %w", err)
		}
	}
	{

		for i := range kinds {
			result.Kinds[i] = result.Kinds[i].Union(kinds[i])
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: MetricsLabels, Shard: shard})
		if !ok {
			return fmt.Errorf("missing labels root record")
		}
		err := readRaw(tx, root, shard, match, &result.LabelsBSI)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: MetricsValue, Shard: shard})
		if !ok {
			return fmt.Errorf("missing values root record")
		}
		err := readRaw(tx, root, shard, match, &result.ValuesBSI)
		if err != nil {
			return fmt.Errorf("reading values %w", err)
		}
	}

	return nil
}

func readSeries(result *samples.Samples, tx *rbf.Tx, records *rbf.Records, shard uint64, match *roaring.Bitmap) error {
	root, ok := records.Get(rbf.Key{Column: MetricsLabels, Shard: shard})
	if !ok {
		return fmt.Errorf("missing labels root record")
	}
	err := readRaw(tx, root, shard, match, &result.LabelsBSI)
	if err != nil {
		return fmt.Errorf("reading labels %w", err)
	}
	return nil
}

func readExemplars(result *samples.Samples, tx *rbf.Tx, records *rbf.Records, shard uint64, match *roaring.Bitmap, kinds Kinds) error {

	{
		for i := range kinds {
			result.Kinds[i] = result.Kinds[i].Union(kinds[i])
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: MetricsLabels, Shard: shard})
		if !ok {
			return fmt.Errorf("missing labels root record")
		}
		err := readRaw(tx, root, shard, match, &result.LabelsBSI)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: MetricsValue, Shard: shard})
		if !ok {
			return fmt.Errorf("missing values root record")
		}
		err := readRaw(tx, root, shard, match, &result.ValuesBSI)
		if err != nil {
			return fmt.Errorf("reading values %w", err)
		}
	}
	return nil
}
