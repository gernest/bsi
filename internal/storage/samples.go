package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/internal/bitmaps"
	"github.com/gernest/u128/internal/rbf"
	"github.com/gernest/u128/internal/storage/keys"
	"github.com/gernest/u128/internal/storage/magic"
	"github.com/gernest/u128/internal/storage/samples"
	"github.com/gernest/u128/internal/storage/views"
	"github.com/prometheus/prometheus/storage"
	"go.etcd.io/bbolt"
)

var viewsPool views.Pool

func (db *Store) Select(start, end int64) storage.SeriesSet {
	v, err := db.selectViews(start, end)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	defer viewsPool.Put(v)

	if len(v.Shards) == 0 {
		return storage.EmptySeriesSet()
	}
	result := samples.Get()
	defer result.Release()

	err = db.readTs(result, v, start, end)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	err = db.applyTranslation(result)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return result.Make()

}

// selectViews returns all views between start and end. Upper view is inclusive,
// we ant to search the upper week for valid samples.
func (db *Store) selectViews(start, end int64) (vs *views.List, err error) {
	vs = viewsPool.Get()

	err = db.txt.View(func(tx *bbolt.Tx) error {
		cu := tx.Bucket(admin).Cursor()
		for k, v := cu.First(); v != nil; k, v = cu.Next() {
			o := magic.ReinterpretSlice[views.Meta](v)
			if o[0].InRange(start, end) {
				vs.Shards = append(vs.Shards, binary.BigEndian.Uint64(k))
				vs.Meta = append(vs.Meta, o[0])
			}
		}
		return nil
	})
	if err != nil {
		viewsPool.Put(vs)
	}
	return
}

func (db *Store) readTs(result *samples.Samples, vs *views.List, start, end int64) error {
	tx, err := db.rbf.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return err
	}

	for i := range vs.Shards {
		shard := vs.Shards[i]
		tsP, ok := records.Get(rbf.Key{Column: keys.MetricsTimestamp, Shard: shard})
		if !ok {
			panic("missing ts root records")
		}
		ra, err := readBSIRange(tx, tsP, shard, vs.Meta[i].TsDepth, bitmaps.BETWEEN, start, end)
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

		float, err := readBSIRange(tx, kind, shard, vs.Meta[i].KindDepth, bitmaps.EQ, int64(keys.Float), 0)
		if err != nil {
			return err
		}
		histogram, err := readBSIRange(tx, kind, shard, vs.Meta[i].KindDepth, bitmaps.EQ, int64(keys.Histogram), 0)
		if err != nil {
			return err
		}
		ra = ra.Intersect(float.Union(histogram))
		if !ra.Any() {
			continue
		}
		err = readSamples(result, vs.Meta[i], tx, records, shard, ra)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Store) applyTranslation(result *samples.Samples) error {

	var b [8]byte

	readData := func(tx *bbolt.Tx, bucket []byte, columns *roaring.Bitmap) {
		bu := tx.Bucket(bucket)
		for column := range columns.RangeAll() {
			binary.BigEndian.PutUint64(b[:], column)
			v := result.Own(bu.Get(b[:]))
			result.Data[column] = v
		}
	}

	return db.txt.View(func(tx *bbolt.Tx) error {
		if ra := result.KindBSI.GetColumns(int64(keys.Histogram), nil); ra.Any() {
			readData(tx, histogramData, result.ValuesBSI.Transpose(ra))
		}
		if ra := result.KindBSI.GetColumns(int64(keys.Exemplar), nil); ra.Any() {
			readData(tx, exemplarData, ra)
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

		return readFromU64(tx.Bucket(metaData), ra, func(id uint64, value []byte) error {
			result.SeriesData[id] = result.Own(value)
			return nil
		})
	})

}

func readSamples(result *samples.Samples, meta views.Meta, tx *rbf.Tx, records *rbf.Records, shard uint64, match *roaring.Bitmap) error {

	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsTimestamp, Shard: shard})
		if !ok {
			return fmt.Errorf("missing timestamp root record")
		}
		err := readBSI(tx, root, shard, meta.TsDepth, match, &result.TsBSI)
		if err != nil {
			return fmt.Errorf("reading timestamp %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsType, Shard: shard})
		if !ok {
			return fmt.Errorf("missing metric type root record")
		}
		err := readBSI(tx, root, shard, meta.KindDepth, match, &result.TsBSI)
		if err != nil {
			return fmt.Errorf("reading metric type %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsLabels, Shard: shard})
		if !ok {
			return fmt.Errorf("missing labels root record")
		}
		err := readBSI(tx, root, shard, meta.LabelsDepth, match, &result.LabelsBSI)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsValue, Shard: shard})
		if !ok {
			return fmt.Errorf("missing values root record")
		}
		err := readBSI(tx, root, shard, meta.ValueDepth, match, &result.ValuesBSI)
		if err != nil {
			return fmt.Errorf("reading values %w", err)
		}
	}
	return nil
}
