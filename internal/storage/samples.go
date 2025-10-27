package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/internal/checksum"
	"github.com/gernest/u128/internal/rbf"
	"github.com/gernest/u128/internal/storage/bsi"
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
	s := new(Samples)
	s.Init()
	return s
}}

// Samples is a reusable container of timeseries samples.
type Samples struct {
	series     map[checksum.U128]*roaring.Bitmap
	seriesData map[checksum.U128][]byte
	data       map[uint64][]byte
	kind       bsi.BSI
	labels     bsi.BSI
	ts         bsi.BSI
	values     bsi.BSI
	ls         []checksum.U128
	b          buffer.B
	ref        int32
	active     checksum.U128
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

func (s *Samples) Init() {
	s.series = make(map[checksum.U128]*roaring.Bitmap)
	s.seriesData = make(map[checksum.U128][]byte)
	s.data = make(map[uint64][]byte)
	s.labels.Init()
	s.ts.Init()
	s.values.Init()
}

// Reset clears s for reuse.
func (s *Samples) Reset() {
	s.labels.Reset()
	s.ts.Reset()
	s.values.Reset()
	clear(s.series)
	clear(s.seriesData)
	clear(s.data)
	s.kind.Reset()
	s.b.B = s.b.B[:0]
	s.ref = 0
	s.ls = s.ls[:0]
}

// Make builds storage.SeriesSet from samples s.  We sort series by the u128 checksum
// to ensure consistent output during tests.
func (s *Samples) Make() storage.SeriesSet {
	if !s.ts.Any() {
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
	s  *Samples
	hs *histogram.Histogram
	fh *histogram.FloatHistogram
	t  int64
	f  float64
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
	ts, _ := i.s.ts.GetValue(idx)
	i.t = int64(ts)
	kind, ok := i.s.kind.GetValue(idx)
	if !ok {
		panic(fmt.Errorf("missing kind for samples at post=%d", idx))
	}
	switch keys.Kind(kind) {
	case keys.Float:
		v, _ := i.s.values.GetValue(idx)
		i.f = math.Float64frombits(uint64(v))
		i.hs = nil
		i.fh = nil
		return chunkenc.ValFloat
	case keys.Histogram:
		var hs prompb.Histogram
		hs.Unmarshal(i.s.data[idx])
		if hs.IsFloatHistogram() {
			i.fh = hs.ToFloatHistogram()
			i.hs = nil
			return chunkenc.ValFloatHistogram
		}
		i.hs = hs.ToIntHistogram()
		i.fh = nil
		return chunkenc.ValHistogram
	default:
		panic(fmt.Errorf("unexpected kind: %v  at=%d ", kind, idx))
	}
}

// Seek implements chunkenc.Iterator.
func (i *Iter) Seek(t int64) chunkenc.ValueType {
	panic("not supported")
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

func (db *Store) Select(start, end time.Time) storage.SeriesSet {
	v := db.selectViews(start, end)
	defer v.Release()

	if v.IsEmpty() {
		return storage.EmptySeriesSet()
	}

	result := NewSamples()
	defer result.Release()

	return result.Make()

}

type views struct {
	year []uint16
	week []uint8
}

var viewsPool = &sync.Pool{New: func() any {
	return &views{
		year: make([]uint16, 0, 4<<10),
		week: make([]uint8, 0, 4<<10),
	}
}}

func (v *views) IsEmpty() bool {
	return len(v.year) == 0
}

func (v *views) Release() {
	v.year = v.year[:0]
	v.week = v.week[:0]
	viewsPool.Put(v)
}

// selectViews returns all views between start and end. Upper view is inclusive,
// we ant to search the upper week for valid samples.
func (db *Store) selectViews(start, end time.Time) (v *views) {
	v = viewsPool.Get().(*views)
	lo := rbf.ViewTS(start)
	hi := rbf.ViewTS(end)

	db.mu.RLock()
	db.tree.AscendGreaterOrEqual(lo, func(item rbf.View) bool {
		if item.Compare(&hi) > 0 {
			return false
		}
		v.year = append(v.year, item.Year)
		v.week = append(v.week, item.Week)
		return true
	})
	db.mu.RUnlock()
	return
}

func (db *Store) translateView(result *Samples, view rbf.View, offset int) error {
	da, done, err := db.txt.Do(view, dataPath{Path: db.dataPath})
	if err != nil {
		return err
	}
	defer done.Close()

	local := map[uint64][]byte{}
	var b [8]byte

	readData := func(tx *bbolt.Tx, bucket []byte, columns *roaring.Bitmap) {
		clear(local)
		bu := tx.Bucket(bucket)
		for column := range columns.RangeAll() {
			value, _ := result.kind.GetValue(column)
			if v, ok := local[value]; ok {
				result.data[column] = v
				continue
			}
			binary.BigEndian.PutUint64(b[:], value)
			v := result.b.Own(bu.Get(b[:]))
			local[value] = v
		}
	}

	return da.View(func(tx *bbolt.Tx) error {
		if ra := result.kind.GetColumns(int64(keys.Histogram)); ra.Any() {
			readData(tx, histogramData, ra)
		}
		if ra := result.kind.GetColumns(int64(keys.Exemplar)); ra.Any() {
			readData(tx, exemplarData, ra)
		}
		if ra := result.kind.GetColumns(int64(keys.Metadata)); ra.Any() {
			readData(tx, metaData, ra)
		}

		series := result.labels.AsMap()
		// Series are repetitive we map series value to column position they appeared.
		v2Columns := map[uint64]*roaring.Bitmap{}
		ra := roaring.NewBitmap()
		for k, v := range series {
			r, ok := v2Columns[v]
			if !ok {
				r = roaring.NewBitmap()
				v2Columns[v] = r
			}
			ra.DirectAdd(v)
			r.DirectAdd(k)
		}

		return readFromU64(tx.Bucket(metaData), ra, func(id uint64, value []byte) error {
			sum := checksum.Hash(value)
			sr, ok := result.series[sum]
			if ok {
				// update columns
				sr.UnionInPlace(v2Columns[id])
				return nil
			}
			// new series
			result.series[sum] = v2Columns[id]
			result.seriesData[sum] = result.b.Own(value)
			return nil
		})
	})
}

func readSamples(result *Samples, tx *rbf.Tx, records *rbf.Records, shard uint64, match *roaring.Bitmap) error {

	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsTimestamp, Shard: shard})
		if !ok {
			return fmt.Errorf("missing timestamp root record")
		}
		err := readBSI(tx, root, shard, match, &result.ts)
		if err != nil {
			return fmt.Errorf("reading timestamp %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsType, Shard: shard})
		if !ok {
			return fmt.Errorf("missing metric type root record")
		}
		err := readBSI(tx, root, shard, match, &result.ts)
		if err != nil {
			return fmt.Errorf("reading metric type %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsLabels, Shard: shard})
		if !ok {
			return fmt.Errorf("missing labels root record")
		}
		err := readBSI(tx, root, shard, match, &result.labels)
		if err != nil {
			return fmt.Errorf("reading labels %w", err)
		}
	}
	{
		root, ok := records.Get(rbf.Key{Column: keys.MetricsValue, Shard: shard})
		if !ok {
			return fmt.Errorf("missing values root record")
		}
		err := readBSI(tx, root, shard, match, &result.values)
		if err != nil {
			return fmt.Errorf("reading values %w", err)
		}
	}
	return nil
}
