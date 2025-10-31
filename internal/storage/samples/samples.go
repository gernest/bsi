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
	return s
}

// At implements storage.SeriesSet.
func (s *Samples) At() storage.Series {
	s.Retain()
	return &series{
		id: s.active,
		s:  s,
	}
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

type series struct {
	s  *Samples
	id uint64
}

var _ storage.Series = (*series)(nil)

func (s *series) Release() {
	s.s.Release()
	s.s = nil
}

// Labels implements storage.Series.
func (s *series) Labels() labels.Labels {
	return buffer.WrapLabel(s.s.SeriesData[s.id]).Copy()
}

// Iterator implements storage.SeriesSet.
func (s *series) Iterator(c chunkenc.Iterator) chunkenc.Iterator {
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
	po  *roaring.Iterator
	s   *series
	id  uint64
	typ chunkenc.ValueType
}

// Init initializes i state.
func (i *Iter) Init(s *series) {
	i.s = s
	i.po = s.s.Series[s.id].Iterator()
}

// Reset clears i for reuse.
func (i *Iter) Reset() {
	*i = Iter{}
}

var _ chunkenc.Iterator = (*Iter)(nil)

// Next implements chunkenc.Iterator.
func (i *Iter) Next() chunkenc.ValueType {
	id, eof := i.po.Next()
	if eof {
		i.s.Release()
		i.s = nil
		i.typ = chunkenc.ValNone
		return i.typ
	}
	i.id = id
	kind, ok := i.s.s.KindBSI.GetValue(id)
	if !ok {
		panic(fmt.Sprintf("missing metric type at %d", id))
	}
	switch keys.Kind(kind) {
	case keys.Float:
		i.typ = chunkenc.ValFloat
	case keys.Histogram:
		i.typ = chunkenc.ValHistogram
	case keys.FloatHistogram:
		i.typ = chunkenc.ValFloatHistogram
	default:
		panic(fmt.Sprintf("unknown metric type %d", kind))
	}
	return i.typ
}

// Seek implements chunkenc.Iterator.
func (i *Iter) Seek(t int64) chunkenc.ValueType {
	return i.typ
}

// At implements chunkenc.Iterator.
func (i *Iter) At() (int64, float64) {
	ts, _ := i.s.s.TsBSI.GetValue(i.id)
	v, _ := i.s.s.ValuesBSI.GetValue(i.id)
	return int64(ts), math.Float64frombits(v)
}

// AtHistogram implements chunkenc.Iterator.
func (i *Iter) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	ts, _ := i.s.s.TsBSI.GetValue(i.id)
	v, _ := i.s.s.ValuesBSI.GetValue(i.id)
	var h prompb.Histogram
	h.Unmarshal(i.s.s.Data[v])
	return int64(ts), h.ToIntHistogram()
}

// AtFloatHistogram implements chunkenc.Iterator.
func (i *Iter) AtFloatHistogram(_ *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	ts, _ := i.s.s.TsBSI.GetValue(i.id)
	v, _ := i.s.s.ValuesBSI.GetValue(i.id)
	var h prompb.Histogram
	h.Unmarshal(i.s.s.Data[v])
	return int64(ts), h.ToFloatHistogram()
}

// AtT implements chunkenc.Iterator.
func (i *Iter) AtT() int64 {
	ts, _ := i.s.s.TsBSI.GetValue(i.id)
	return int64(ts)
}

// Err implements chunkenc.Iterator.
func (i *Iter) Err() error {
	return nil
}
