// Package samples implements in memory representation of timeseries samples. We
// use BSI encoding for all columns, both in memory and on disc. Decoding is done on demand
// by promql.
package samples

import (
	"bytes"
	"fmt"
	"math"
	"slices"

	"github.com/gernest/bsi/internal/pools"
	"github.com/gernest/bsi/internal/storage/buffer"
	"github.com/gernest/bsi/internal/storage/raw"
	"github.com/gernest/roaring"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

var histogramPool = pools.Pool[*prompb.Histogram]{Init: histogramItems{}}

type histogramItems struct{}

var _ pools.Items[*prompb.Histogram] = (*histogramItems)(nil)

func (histogramItems) Init() *prompb.Histogram {
	return new(prompb.Histogram)
}

func (histogramItems) Reset(v *prompb.Histogram) *prompb.Histogram {
	v.Reset()
	return v
}

// Samples is a full representation of samples in memory.
type Samples struct {
	Series     map[uint64]*roaring.Bitmap
	SeriesData map[uint64][]byte
	Data       map[uint64][]byte
	ls         []uint64
	KindBSI    [4]*roaring.Bitmap
	LabelsBSI  raw.BSI
	TsBSI      raw.BSI
	ValuesBSI  raw.BSI
	active     uint64
}

// Own copies v and returns the copy from internal buffer. This ensures sample
// outlive the transaction used to retrieve translation data, v is assumed to be
// memory mapped.
func (s *Samples) Own(v []byte) []byte {
	return bytes.Clone(v)
}

// Init initialize s fields.
func (s *Samples) Init() {
	s.Series = make(map[uint64]*roaring.Bitmap)
	s.SeriesData = make(map[uint64][]byte)
	s.Data = make(map[uint64][]byte)
	s.LabelsBSI.Init()
	s.TsBSI.Init()
	s.ValuesBSI.Init()
	for i := range s.KindBSI {
		s.KindBSI[i] = roaring.NewBitmap()
	}
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
	result = make([]exemplar.QueryResult, 0, len(s.ls))
	var e prompb.Exemplar
	scratch := labels.NewScratchBuilder(64)
	for i := range s.ls {
		id := s.ls[i]
		series := buffer.WrapLabel(s.SeriesData[id]).Copy()
		ra := s.Series[id]
		exe := make([]exemplar.Exemplar, 0, ra.Count())
		for value := range ra.RangeAll() {
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

	// We may have s stay in memory much longer depending on PromQL. We ensure
	// small memory footprint is occupied.
	s.LabelsBSI.Reset() // we never use this again.
	for i := range s.KindBSI {
		s.KindBSI[i].Optimize()
	}
	s.TsBSI.Optimize()
	s.ValuesBSI.Optimize()
	return s
}

// At implements storage.SeriesSet.
func (s *Samples) At() storage.Series {
	return &series{
		id: s.active,
		s:  s,
	}
}

// Next implements storage.SeriesSet.
func (s *Samples) Next() bool {
	if len(s.ls) == 0 {
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
	s   *series
	ts  raw.Sorted
	idx int
	typ chunkenc.ValueType
}

// Init initializes i state.
func (i *Iter) Init(s *series) {
	i.s = s
	s.s.TsBSI.Sorted(s.s.Series[s.id], &i.ts)
	i.idx = -1
	i.typ = chunkenc.ValNone
}

// Reset clears i for reuse.
func (i *Iter) Reset() {
	i.s = nil
	i.ts.ID = i.ts.ID[:0]
	i.ts.Value = i.ts.Value[:0]
	i.idx = 0
	i.typ = chunkenc.ValNone
}

var _ chunkenc.Iterator = (*Iter)(nil)

// Next implements chunkenc.Iterator.
func (i *Iter) Next() chunkenc.ValueType {
	i.idx++
	if i.idx >= i.ts.Len() {
		i.s = nil
		i.typ = chunkenc.ValNone
		return i.typ
	}
	kind := i.s.s.KindBSI
	id := i.ts.ID[i.idx]
	switch {
	case kind[0].Contains(id):
		i.typ = chunkenc.ValFloat
	case kind[1].Contains(id):
		i.typ = chunkenc.ValHistogram
	case kind[2].Contains(id):
		i.typ = chunkenc.ValFloatHistogram
	default:
		panic(fmt.Sprintf("unknown metric type at column %d", id))
	}
	return i.typ
}

// Seek implements chunkenc.Iterator.
func (i *Iter) Seek(t int64) chunkenc.ValueType {
	return i.typ
}

// At implements chunkenc.Iterator.
func (i *Iter) At() (int64, float64) {
	ts := i.ts.Value[i.idx]
	v, _ := i.s.s.ValuesBSI.GetValue(i.ts.ID[i.idx])
	return int64(ts), math.Float64frombits(v)
}

// AtHistogram implements chunkenc.Iterator.
func (i *Iter) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	ts := i.ts.Value[i.idx]
	v, _ := i.s.s.ValuesBSI.GetValue(i.ts.ID[i.idx])
	h := histogramPool.Get()
	h.Unmarshal(i.s.s.Data[v])
	r := h.ToIntHistogram()
	histogramPool.Put(h)
	return int64(ts), r
}

// AtFloatHistogram implements chunkenc.Iterator.
func (i *Iter) AtFloatHistogram(_ *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	ts := i.ts.Value[i.idx]
	v, _ := i.s.s.ValuesBSI.GetValue(i.ts.ID[i.idx])
	h := histogramPool.Get()
	h.Unmarshal(i.s.s.Data[v])
	r := h.ToFloatHistogram()
	histogramPool.Put(h)
	return int64(ts), r
}

// AtT implements chunkenc.Iterator.
func (i *Iter) AtT() int64 {
	return i.ts.Value[i.idx]
}

// Err implements chunkenc.Iterator.
func (i *Iter) Err() error {
	return nil
}
