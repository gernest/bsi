package storage

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/internal/checksum"
	"github.com/gernest/u128/internal/rbf"
	"github.com/gernest/u128/internal/storage/array"
	"github.com/gernest/u128/internal/storage/buffer"
	"github.com/gernest/u128/internal/storage/keys"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"go.etcd.io/bbolt"
)

var samplePool = &sync.Pool{New: func() any {
	return &Samples{
		series:     make(map[checksum.U128]*roaring.Bitmap),
		seriesData: make(map[checksum.U128][]byte),
		kindMap:    make(map[keys.Kind]*roaring.Bitmap),
		kindData:   make(map[keys.Kind]map[uint64][]byte),
	}
}}

// Samples is a reusable container of timeseries samples.
type Samples struct {
	labels     array.Uint64
	ts         array.Uint64
	values     array.Uint64
	series     map[checksum.U128]*roaring.Bitmap
	seriesData map[checksum.U128][]byte
	kindMap    map[keys.Kind]*roaring.Bitmap
	kindData   map[keys.Kind]map[uint64][]byte
	ls         []checksum.U128
	active     checksum.U128
	b          buffer.B
	ref        int32
}

// NewSamples returns new Samples instance. Call Release when done using it to free
// up resources.
func NewSamples() *Samples {
	s := samplePool.Get().(*Samples)
	s.Retain()
	return s
}

// Release returns s to the pool.
func (s *Samples) Release() {
	s.ref--
	if s.ref == 0 {
		s.Reset()
		samplePool.Put(s)
	}
}

func (s *Samples) Retain() {
	s.ref++
}

// Reset clears s for reuse.
func (s *Samples) Reset() {
	s.labels.Reset()
	s.ts.Reset()
	s.values.Reset()
	clear(s.series)
	clear(s.seriesData)
	clear(s.kindMap)
	clear(s.kindData)
	s.b.B = s.b.B[:0]
	s.ref = 0
	s.ls = s.ls[:0]
}

func (s *Samples) getKind(kind keys.Kind) *roaring.Bitmap {
	a, ok := s.kindMap[kind]
	if !ok {
		a = roaring.NewBitmap()
		s.kindMap[kind] = a
	}
	return a
}

func (s *Samples) getKindData(kind keys.Kind) map[uint64][]byte {
	a, ok := s.kindData[kind]
	if !ok {
		a = map[uint64][]byte{}
		s.kindData[kind] = a
	}
	return a
}

// Make builds storage.SeriesSet from samples s.  We sort series by the u128 checksum
// to ensure consistent output during tests.
func (s *Samples) Make() storage.SeriesSet {
	if len(s.ts.B) == 0 {
		return storage.EmptySeriesSet()
	}
	s.ls = s.ls[:0]
	for v := range s.series {
		s.ls = append(s.ls, v)
	}
	sort.Slice(s.ls, func(i, j int) bool {
		return bytes.Compare(s.ls[i][:], s.ls[j][:]) < 0
	})
	return s
}

// At implements storage.SeriesSet.
func (s *Samples) At() storage.Series {
	return s
}

// Next implements storage.SeriesSet.
func (s *Samples) Next() bool {
	if len(s.ls) == 0 {
		s.Release()
		return false
	}
	s.active = s.ls[0]
	s.ls = s.ls[1:]
	return true
}

// Err implements storage.SeriesSet.
func (s *Samples) Err() error { return nil }

// Warnings implements storage.SeriesSet.
func (s *Samples) Warnings() annotations.Annotations { return nil }

// Labels implements storage.SeriesSet.
func (s *Samples) Labels() labels.Labels {
	return buffer.WrapLabel(s.seriesData[s.active])
}

// Iterator implements storage.SeriesSet.
func (s *Samples) Iterator(c chunkenc.Iterator) chunkenc.Iterator {
	if c != nil {
		i, ok := c.(*Iter)
		if ok {
			i.Reset()
			i.Init(s)
			return i
		}
	}
	i := new(Iter)
	i.Init(s)
	return i
}

// Iter implements chunkenc.Iterator on top of Samples.
type Iter struct {
	po *roaring.Iterator

	s *Samples

	t  int64
	f  float64
	hs *histogram.Histogram
	fh *histogram.FloatHistogram
}

// Init initializes i state.
func (i *Iter) Init(s *Samples) {
	s.Retain()
	i.po = s.series[s.active].Iterator()
}

// Reset clears i for reuse.
func (i *Iter) Reset() {
	*i = Iter{}
}

var _ chunkenc.Iterator = (*Iter)(nil)

// Next implements chunkenc.Iterator.
func (i *Iter) Next() chunkenc.ValueType {
	idx, eof := i.po.Next()
	if eof {
		i.s.Release()
		return chunkenc.ValNone
	}
	i.t = int64(i.s.ts.B[idx])
	if i.s.getKind(keys.Float).Contains(idx) {
		i.f = math.Float64frombits(i.s.values.B[idx])
		i.hs = nil
		i.fh = nil
		return chunkenc.ValFloat
	}
	if i.s.getKind(keys.Histogram).Contains(idx) {
		var hs prompb.Histogram
		hs.Unmarshal(i.s.kindData[keys.Histogram][i.s.values.B[idx]])
		if hs.IsFloatHistogram() {
			i.fh = hs.ToFloatHistogram()
			i.hs = nil
			return chunkenc.ValFloatHistogram
		}
		i.hs = hs.ToIntHistogram()
		i.fh = nil
		return chunkenc.ValHistogram
	}
	// We need to ensure all positions are accounted for in kinds bitmap. If we arrive
	// here it means something is terribly wrong with out position handling logic.
	panic(fmt.Errorf("unexpected position %d", idx))
}

// Seek implements chunkenc.Iterator.
func (i *Iter) Seek(t int64) chunkenc.ValueType {
	v := uint64(t)
	idx := sort.Search(len(i.s.ts.B), func(idx int) bool {
		return i.s.ts.B[idx] < v
	})
	i.po.Seek(uint64(idx))
	return i.Next()
}

// At implements chunkenc.Iterator.
func (i *Iter) At() (int64, float64) {
	return i.t, i.f
}

// AtHistogram implements chunkenc.Iterator.
func (i *Iter) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return i.t, i.hs
}

// AtFloatHistogram implements chunkenc.Iterator.
func (i *Iter) AtFloatHistogram(w *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	if i.hs != nil {
		return i.t, i.hs.ToFloat(w)
	}
	return i.t, i.fh
}

// AtT implements chunkenc.Iterator.
func (i *Iter) AtT() int64 {
	return i.t
}

// Err implements chunkenc.Iterator.
func (i *Iter) Err() error {
	return nil
}

func (db *Store) translateView(result *Samples, view rbf.View, offset int) error {
	da, done, err := db.txt.Do(view, dataPath{Path: db.dataPath})
	if err != nil {
		return err
	}
	defer done.Close()

	return da.View(func(tx *bbolt.Tx) error {
		if h, ok := result.kindMap[keys.Histogram]; ok && h.Any() {
			ls := result.values.B
			it := h.IteratorAt(uint64(offset))

			v2idx := map[uint64]uint64{}

			ra := roaring.NewBitmap()
			for nxt, eof := it.Next(); !eof; nxt, eof = it.Next() {
				v2idx[ls[nxt]] = nxt
				ra.DirectAdd(ls[nxt])
			}
			data := result.getKindData(keys.Histogram)

			_ = readFromU64(tx.Bucket(histogramData), ra, func(id uint64, value []byte) error {
				data[v2idx[id]] = result.b.Own(value)
				return nil
			})

		}

		// translate labels
		ra := roaring.NewBitmap()
		ls := result.labels.B[offset:]
		ra.DirectAddNPlain(ls...)

		v2idx := map[uint64]uint64{}
		for i := range ls {
			v2idx[ls[i]] = uint64(offset + i)
		}
		readFromU64(tx.Bucket(metricsData), ra, func(id uint64, value []byte) error {
			sum := checksum.Hash(value)
			rx, ok := result.series[sum]
			if ok {
				rx.DirectAdd(v2idx[id])
				return nil
			}
			rx = roaring.NewBitmap()
			rx.DirectAdd(v2idx[id])
			result.series[sum] = rx
			result.seriesData[sum] = result.b.Own(value)
			return nil
		})
		return nil
	})
}

func readSamples(result *Samples, tx *rbf.Tx, records *rbf.Records, shard uint64, match *roaring.Bitmap) error {
	offset := uint64(result.ts.Len())

	{
		// samples can only be from histogram or floats
		root, ok := records.Get(rbf.Key{Column: keys.MetricsType, Shard: shard})
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
			a := result.getKind(keys.Float)
			// all floats
			for i := range match.Count() {
				a.DirectAdd(offset + i)
			}
		} else if !float.Any() {
			// all histograms
			b := result.getKind(keys.Histogram)
			for i := range match.Count() {
				b.DirectAdd(offset + i)
			}
		} else {
			a := result.getKind(keys.Float)
			b := result.getKind(keys.Histogram)
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
		root, ok := records.Get(rbf.Key{Column: keys.MetricsTimestamp, Shard: shard})
		if !ok {
			return fmt.Errorf("missing timestamp root record")
		}
		data := result.ts.Allocate(size)
		err := readBSI(tx, root, shard, match, data)
		if err != nil {
			return fmt.Errorf("reading timestamp %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsLabels, Shard: shard})
		if !ok {
			return fmt.Errorf("missing labels root record")
		}
		data := result.labels.Allocate(size)
		err := readBSI(tx, root, shard, match, data)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsValue, Shard: shard})
		if !ok {
			return fmt.Errorf("missing values root record")
		}
		data := result.values.Allocate(size)
		err := readBSI(tx, root, shard, match, data)
		if err != nil {
			return fmt.Errorf("reading values %w", err)
		}
	}
	return nil
}
