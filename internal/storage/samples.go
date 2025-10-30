package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/bits"
	"slices"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/internal/bitmaps"
	"github.com/gernest/u128/internal/checksum"
	"github.com/gernest/u128/internal/rbf"
	"github.com/gernest/u128/internal/storage/keys"
	"github.com/gernest/u128/internal/storage/magic"
	"github.com/gernest/u128/internal/storage/samples"
	"github.com/gernest/u128/internal/storage/views"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"go.etcd.io/bbolt"
)

var shardsPool views.Pool

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

	result := samples.Get()
	defer result.Release()

	err = db.readTs(result, shards, hints.Start, hints.End)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	err = db.translate(result)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return result.Make()

}

func (db *Store) findShards(start, end int64, matchers []*labels.Matcher) (vs *views.List, err error) {
	vs = shardsPool.Get()

	err = db.txt.View(func(tx *bbolt.Tx) error {
		cu := tx.Bucket(admin).Cursor()
		for k, v := cu.First(); v != nil; k, v = cu.Next() {
			o := magic.ReinterpretSlice[views.Meta](v)
			if o[0].InRange(start, end) {
				vs.Shards = append(vs.Shards, binary.BigEndian.Uint64(k))
				vs.Meta = append(vs.Meta, o[0])
			}
		}
		if len(vs.Meta) == 0 {
			return nil
		}
		if len(matchers) > 0 {
			searchB := tx.Bucket(search)
			cu := searchB.Cursor()
			for _, m := range matchers {
				switch m.Type {
				case labels.MatchEqual:
					b, _ := cu.Seek(magic.Slice(m.Name))
					if !bytes.Equal(b, magic.Slice(m.Name)) {
						// no bucket for label name observed yet. We will never satisfy
						// matching conditions
						vs.Reset()
						return nil
					}
					mb := searchB.Bucket(b)
					value := mb.Get(magic.Slice(m.Value))
					if value == nil {
						vs.Reset()
						return nil
					}
					va := binary.BigEndian.Uint64(value)
					vs.Search = append(vs.Search, views.Search{
						Column: checksum.Hash(b),
						Value:  []uint64{va},
						Depth:  uint8(bits.Len64(va)),
						OP:     bitmaps.EQ,
					})
				case labels.MatchNotEqual:
					b, _ := cu.Seek(magic.Slice(m.Name))
					if !bytes.Equal(b, magic.Slice(m.Name)) {
						continue
					}
					mb := searchB.Bucket(b)
					value := mb.Get(magic.Slice(m.Value))
					if value == nil {
						continue
					}
					va := binary.BigEndian.Uint64(value)
					vs.Search = append(vs.Search, views.Search{
						Column: checksum.Hash(b),
						Value:  []uint64{va},
						Depth:  uint8(bits.Len64(mb.Sequence())),
						OP:     bitmaps.NEQ,
					})
				case labels.MatchRegexp:
					b, _ := cu.Seek(magic.Slice(m.Name))
					if !bytes.Equal(b, magic.Slice(m.Name)) {
						// no bucket for label name observed yet. We will never satisfy
						// matching conditions
						vs.Reset()
						return nil
					}
					mb := searchB.Bucket(b)
					values := make([]uint64, 0, 64)

					mc := mb.Cursor()
					prefix := magic.Slice(m.Prefix())
					for a, b := mc.Seek(prefix); b != nil && bytes.HasPrefix(a, prefix) && m.Matches(magic.String(a)); a, b = mc.Next() {
						va := binary.BigEndian.Uint64(b)
						values = append(values, va)
					}
					if len(values) == 0 {
						vs.Reset()
						return nil
					}
					slices.Sort(values)
					vs.Search = append(vs.Search, views.Search{
						Column: checksum.Hash(b),
						Value:  values,
						Depth:  uint8(bits.Len64(values[len(values)-1])),
						OP:     bitmaps.NEQ,
					})

				case labels.MatchNotRegexp:
					b, _ := cu.Seek(magic.Slice(m.Name))
					if !bytes.Equal(b, magic.Slice(m.Name)) {
						continue
					}
					mb := searchB.Bucket(b)
					values := make([]uint64, 0, 64)

					mc := mb.Cursor()
					prefix := magic.Slice(m.Prefix())
					for a, b := mc.Seek(prefix); b != nil && bytes.HasPrefix(a, prefix) && m.Matches(magic.String(a)); a, b = mc.Next() {
						va := binary.BigEndian.Uint64(b)
						values = append(values, va)
					}
					if len(values) == 0 {
						continue
					}
					slices.Sort(values)
					vs.Search = append(vs.Search, views.Search{
						Column: checksum.Hash(b),
						Value:  values,
						Depth:  uint8(bits.Len64(mb.Sequence())),
						OP:     bitmaps.NEQ,
					})
				}

			}
		}
		return nil
	})
	if err != nil {
		shardsPool.Put(vs)
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
