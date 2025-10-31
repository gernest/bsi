package samples

import (
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/internal/storage/bsi"
	"github.com/gernest/u128/internal/storage/buffer"
	"github.com/gernest/u128/internal/storage/keys"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

var samplePool = &sync.Pool{New: func() any {
	s := new(Samples)
	s.Init()
	return s
}}

// Samples is a reusable container of timeseries samples.
type Samples struct {
	Series     map[uint64]*roaring.Bitmap
	SeriesData map[uint64][]byte
	Data       map[uint64][]byte
	KindBSI    bsi.BSI
	LabelsBSI  bsi.BSI
	TsBSI      bsi.BSI
	ValuesBSI  bsi.BSI
	ls         []uint64
	b          buffer.B
	ref        int32
	active     uint64
}

func Get() *Samples {
	s := samplePool.Get().(*Samples)
	s.Retain()
	return s
}

func (s *Samples) Own(v []byte) []byte {
	return s.b.Own(v)
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
	s.Series = make(map[uint64]*roaring.Bitmap)
	s.SeriesData = make(map[uint64][]byte)
	s.Data = make(map[uint64][]byte)
	s.LabelsBSI.Init()
	s.TsBSI.Init()
	s.ValuesBSI.Init()
	s.KindBSI.Init()
}

// Reset clears s for reuse.
func (s *Samples) Reset() {
	s.LabelsBSI.Reset()
	s.TsBSI.Reset()
	s.ValuesBSI.Reset()
	clear(s.Series)
	clear(s.SeriesData)
	clear(s.Data)
	s.KindBSI.Reset()
	s.b.B = s.b.B[:0]
	s.ref = 0
	s.ls = s.ls[:0]
}

// MakeSeries iterates all sample series and yields labels.
func (s *Samples) MakeSeries(cb func(name, value []byte) error) error {
	s.ls = s.ls[:0]
	for v := range s.Series {
		s.ls = append(s.ls, v)
	}
	slices.Sort(s.ls)
	for i := range s.ls {
		id := s.ls[i]
		for name, value := range buffer.RangeLabels(s.SeriesData[id]) {
			err := cb(name, value)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// MakeExemplar returns exemplars. Series values must be of kind.Exemplar.
func (s *Samples) MakeExemplar() (result []exemplar.QueryResult) {
	s.ls = s.ls[:0]
	for v := range s.Series {
		s.ls = append(s.ls, v)
	}
	slices.Sort(s.ls)
	var e prompb.Exemplar
	scratch := labels.NewScratchBuilder(64)
	for i := range s.ls {
		id := s.ls[i]
		series := buffer.WrapLabel(s.SeriesData[id]).Copy()
		exe := make([]exemplar.Exemplar, 0)
		for value := range s.Series[id].RangeAll() {
			exeID, _ := s.ValuesBSI.GetValue(value)
			e.Reset()
			e.Unmarshal(s.Data[exeID])
			exe = append(exe, e.ToExemplar(&scratch, nil))
		}
		result = append(result, exemplar.QueryResult{
			SeriesLabels: series,
			Exemplars:    exe,
		})
	}
	return
}

// Make builds storage.SeriesSet from samples s.  We sort series by the u128 checksum
// to ensure consistent output during tests.
func (s *Samples) Make() storage.SeriesSet {
	if !s.TsBSI.Any() {
		return storage.EmptySeriesSet()
	}
	s.ls = s.ls[:0]
	for v := range s.Series {
		s.ls = append(s.ls, v)
	}
	slices.Sort(s.ls)

	s.Retain()

	// We may have s stay in memory much longer depending on PromQL. We ensure
	// small memory footprint is occupied until s is released.
	s.LabelsBSI.Reset() // we never use this again.
	s.KindBSI.Optimize()
	s.TsBSI.Optimize()
	s.ValuesBSI.Optimize()

	for i := range s.ls {
		fmt.Println(s.ls[i], s.Series[s.ls[i]].Slice(), buffer.WrapLabel(s.SeriesData[s.ls[i]]))
	}
	fmt.Println(s.KindBSI.AsMap(nil))
	fmt.Println(s.TsBSI.AsMap(nil))
	return s
}

// At implements storage.SeriesSet.
func (s *Samples) At() storage.Series {
	s.Retain()
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
	return buffer.WrapLabel(s.SeriesData[s.active])
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
	po  []uint64
	idx int
	s   *Samples
	hs  *histogram.Histogram
	fh  *histogram.FloatHistogram
	t   int64
	f   float64
}

// Init initializes i state.
func (i *Iter) Init(s *Samples) {
	s.Retain()
	i.s = s
	i.po = s.Series[s.active].Slice()
}

// Reset clears i for reuse.
func (i *Iter) Reset() {
	*i = Iter{}
}

var _ chunkenc.Iterator = (*Iter)(nil)

// Next implements chunkenc.Iterator.
func (i *Iter) Next() chunkenc.ValueType {
	if i.idx >= len(i.po) {
		i.s.Release()
		return chunkenc.ValNone
	}
	idx := i.po[i.idx]
	i.idx++
	fmt.Println(idx, i.idx, i.po)
	ts, _ := i.s.TsBSI.GetValue(idx)
	i.t = int64(ts)
	kind, ok := i.s.KindBSI.GetValue(idx)
	if !ok {
		panic(fmt.Errorf("missing kind for samples at post=%d", idx))
	}
	switch keys.Kind(kind) {
	case keys.Float:
		v, _ := i.s.ValuesBSI.GetValue(idx)
		i.f = math.Float64frombits(uint64(v))
		i.hs = nil
		i.fh = nil
		return chunkenc.ValFloat
	case keys.Histogram:
		var hs prompb.Histogram
		hs.Unmarshal(i.s.Data[idx])
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
	fmt.Println("seek", t)
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
