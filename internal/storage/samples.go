package storage

import (
	"fmt"
	"sync"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/internal/checksum"
	"github.com/gernest/u128/internal/rbf"
	"github.com/gernest/u128/internal/storage/array"
	"github.com/gernest/u128/internal/storage/buffer"
	"github.com/gernest/u128/internal/storage/keys"
	"go.etcd.io/bbolt"
)

var samplePool = &sync.Pool{New: func() any {
	return &Samples{
		Series:     make(map[checksum.U128]*roaring.Bitmap),
		SeriesData: make(map[checksum.U128][]byte),
		KindMap:    make(map[keys.Kind]*roaring.Bitmap),
		KindData:   make(map[keys.Kind]map[uint64][]byte),
	}
}}

// Samples is a reusable container of timeseries samples.
type Samples struct {
	Labels     array.Uint64
	Timestamp  array.Uint64
	Values     array.Uint64
	Series     map[checksum.U128]*roaring.Bitmap
	SeriesData map[checksum.U128][]byte
	KindMap    map[keys.Kind]*roaring.Bitmap
	KindData   map[keys.Kind]map[uint64][]byte
	b          buffer.B
}

// NewSamples returns new Samples instance. Call Release when done using it to free
// up resources.
func NewSamples() *Samples {
	return samplePool.Get().(*Samples)
}

// Release returns s to the pool.
func (s *Samples) Release() {
	s.Reset()
	samplePool.Put(s)
}

// Reset clears s for reuse.
func (s *Samples) Reset() {
	s.Labels.Reset()
	s.Timestamp.Reset()
	s.Values.Reset()
	clear(s.Series)
	clear(s.SeriesData)
	clear(s.KindMap)
	clear(s.KindData)
	s.b.B = s.b.B[:0]
}

func (s *Samples) kind(kind keys.Kind) *roaring.Bitmap {
	a, ok := s.KindMap[kind]
	if !ok {
		a = roaring.NewBitmap()
		s.KindMap[kind] = a
	}
	return a
}

func (s *Samples) kindData(kind keys.Kind) map[uint64][]byte {
	a, ok := s.KindData[kind]
	if !ok {
		a = map[uint64][]byte{}
		s.KindData[kind] = a
	}
	return a
}

// ReadSamples sequentially reads samples based on selector.
func (db *Store) ReadSamples(result *Samples, selector *Selectors) error {
	// we only work with a single rbf transaction
	da, done, err := db.rbf.Do(db.dataPath, struct{}{})
	if err != nil {
		return err
	}
	defer done.Close()

	tx, err := da.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return err
	}
	for i := range selector.Views {
		view := selector.Views[i]
		offset := result.Timestamp.Len()
		for j := range selector.Shards[i] {
			shard := selector.Shards[i][j]
			ra := selector.Match[i][j]
			if !ra.Any() {
				// fast path: nothing matches in the  current shard
				continue
			}
			err = readSamples(result, tx, records, view, shard, ra)
			if err != nil {
				return err
			}
		}
		err = db.translateView(result, view, offset)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Store) translateView(result *Samples, view rbf.View, offset int) error {
	da, done, err := db.txt.Do(view, txtOptions{dataPath: db.dataPath})
	if err != nil {
		return err
	}
	defer done.Close()

	return da.View(func(tx *bbolt.Tx) error {
		if h, ok := result.KindMap[keys.Histogram]; ok && h.Any() {
			ls := result.Values.B
			it := h.IteratorAt(uint64(offset))

			v2idx := map[uint64]uint64{}

			ra := roaring.NewBitmap()
			for nxt, eof := it.Next(); !eof; nxt, eof = it.Next() {
				v2idx[ls[nxt]] = nxt
				ra.DirectAdd(ls[nxt])
			}
			data := result.kindData(keys.Histogram)

			_ = readFromU64(tx.Bucket(histogramData), ra, func(id uint64, value []byte) error {
				data[v2idx[id]] = result.b.Own(value)
				return nil
			})

		}

		// translate labels
		ra := roaring.NewBitmap()
		ls := result.Labels.B[offset:]
		ra.DirectAddNPlain(ls...)

		v2idx := map[uint64]uint64{}
		for i := range ls {
			v2idx[ls[i]] = uint64(offset + i)
		}
		readFromU64(tx.Bucket(metricsData), ra, func(id uint64, value []byte) error {
			sum := checksum.Hash(value)
			rx, ok := result.Series[sum]
			if ok {
				rx.DirectAdd(v2idx[id])
				return nil
			}
			rx = roaring.NewBitmap()
			rx.DirectAdd(v2idx[id])
			result.Series[sum] = rx
			result.SeriesData[sum] = result.b.Own(value)
			return nil
		})
		return nil
	})
}

func readSamples(result *Samples, tx *rbf.Tx, records *rbf.Records, view rbf.View, shard uint64, match *roaring.Bitmap) error {
	offset := uint64(result.Timestamp.Len())

	{
		// samples can only be from histogram or floats
		root, ok := records.Get(rbf.Key{View: view, Column: keys.MetricsType, Shard: shard})
		if !ok {
			return fmt.Errorf("missing timestamp root record")
		}
		hist, float, err := readFloatOrHistMetricType(tx, root, shard)
		if err != nil {
			return fmt.Errorf("reading metric type %w", err)
		}

		// valid samples
		match = match.Intersect(hist.Intersect(float))
		if !match.Any() {
			return nil
		}

		hist.IntersectInPlace(match)
		float.IntersectInPlace(match)

		if !hist.Any() {
			a := result.kind(keys.Float)
			// all floats
			for i := range match.Count() {
				a.DirectAdd(offset + i)
			}
		} else if !float.Any() {
			// all histograms
			b := result.kind(keys.Histogram)
			for i := range match.Count() {
				b.DirectAdd(offset + i)
			}
		} else {
			a := result.kind(keys.Float)
			b := result.kind(keys.Histogram)
			var i uint64
			for row := range match.RangeAll() {
				if float.Contains(row) {
					a.DirectAdd(offset + i)
				} else {
					b.DirectAdd(offset + i)
				}
				i++
			}
		}
	}

	size := int(match.Count())
	{
		root, ok := records.Get(rbf.Key{View: view, Column: keys.MetricsTimestamp, Shard: shard})
		if !ok {
			return fmt.Errorf("missing timestamp root record")
		}
		data := result.Timestamp.Allocate(size)
		err := readBSI(tx, root, shard, match, data)
		if err != nil {
			return fmt.Errorf("reading timestamp %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{View: view, Column: keys.MetricsLabels, Shard: shard})
		if !ok {
			return fmt.Errorf("missing labels root record")
		}
		data := result.Labels.Allocate(size)
		err := readBSI(tx, root, shard, match, data)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{View: view, Column: keys.MetricsValue, Shard: shard})
		if !ok {
			return fmt.Errorf("missing values root record")
		}
		data := result.Values.Allocate(size)
		err := readBSI(tx, root, shard, match, data)
		if err != nil {
			return fmt.Errorf("reading values %w", err)
		}
	}
	return nil
}
