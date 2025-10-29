package views

import (
	"math/bits"
	"sync"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/internal/bitmaps"
	"github.com/gernest/u128/internal/checksum"
	"github.com/gernest/u128/internal/storage/keys"
	"github.com/gernest/u128/internal/storage/magic"
	"github.com/gernest/u128/internal/storage/tsid"
)

type Pool struct {
	p sync.Pool
}

func (p *Pool) Get() *List {
	if v := p.p.Get(); v != nil {
		return v.(*List)
	}
	return &List{}
}

func (p *Pool) Put(v *List) {
	v.Meta = v.Meta[:0]
	v.Shards = v.Shards[:0]
}

type List struct {
	Shards []uint64
	Meta   []Meta
}

type Meta struct {
	Min         int64
	Max         int64
	MaxID       uint64
	TsDepth     uint8
	ValueDepth  uint8
	KindDepth   uint8
	LabelsDepth uint8
}

func (s *Meta) InRange(lo, hi int64) bool {
	return lo < s.Max && hi > s.Min
}

func (s *Meta) Update(other *Meta) {
	if s.Min == 0 {
		s.Min = other.Min
	}
	s.Min = min(s.Min, other.Min)
	s.Max = max(s.Max, other.Max)
	s.MaxID = max(s.MaxID, other.MaxID)
	s.TsDepth = max(s.TsDepth, other.TsDepth)
	s.ValueDepth = max(s.ValueDepth, other.ValueDepth)
}

func (s Meta) Bytes() []byte {
	return magic.ReinterpretSlice[byte]([]Meta{s})
}

type Map map[uint64]*Data

func (s Map) Get(shard uint64) *Data {
	r, ok := s[shard]
	if !ok {
		r = &Data{Columns: make(map[checksum.U128]*roaring.Bitmap)}
		s[shard] = r
	}
	return r
}

type Data struct {
	Meta    Meta
	Columns map[checksum.U128]*roaring.Bitmap
}

func (s *Data) AddIndex(start uint64, values []tsid.ID) {
	labels := s.Get(keys.MetricsLabels)
	var hi uint64

	for i := range values {
		la := &values[i]
		id := start + uint64(i)
		hi = max(la.ID, hi)
		bitmaps.BSI(labels, id, int64(la.ID))
		for j := range la.Views {
			bitmaps.Mutex(s.Get(la.Views[j]), id, la.Rows[j])
		}
	}
	s.Meta.LabelsDepth = uint8(bits.Len64(hi)) + 1

}

func (s *Data) AddTS(start uint64, values []int64) {
	ra := s.Get(keys.MetricsTimestamp)
	var lo, hi int64

	for i := range values {
		if lo == 0 {
			lo = values[i]
		}
		lo = min(lo, values[i])
		hi = max(hi, values[i])
		bitmaps.BSI(ra, start+uint64(i), values[i])
	}
	s.Meta.Min = lo
	s.Meta.Max = hi
	s.Meta.TsDepth = uint8(bits.Len64(max(uint64(lo), uint64(hi)))) + 1
}

func (s *Data) AddValues(start uint64, values []uint64) {
	ra := s.Get(keys.MetricsTimestamp)
	var hi uint64
	for i := range values {
		hi = max(hi, values[i])
		bitmaps.BSI(ra, start+uint64(i), int64(values[i]))
	}
	s.Meta.ValueDepth = uint8(bits.Len64(hi)) + 1
}

func (s *Data) AddKind(start uint64, values []keys.Kind) {
	ra := s.Get(keys.MetricsTimestamp)
	var hi keys.Kind
	for i := range values {
		hi = max(hi, values[i])
		bitmaps.BSI(ra, start+uint64(i), int64(values[i]))
	}
	s.Meta.KindDepth = uint8(bits.Len64(uint64(hi))) + 1
}

func (s *Data) Get(col checksum.U128) *roaring.Bitmap {
	r, ok := s.Columns[col]
	if !ok {
		r = roaring.NewMapBitmap()
		s.Columns[col] = r
	}
	return r
}
