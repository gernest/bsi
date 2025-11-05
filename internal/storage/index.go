package storage

import (
	"math/bits"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/storage/keys"
	"github.com/gernest/bsi/internal/storage/tsid"
	"github.com/gernest/roaring"
)

type viewsItems struct{}

func (viewsItems) Init() *view { return new(view) }

func (viewsItems) Reset(v *view) *view { return v.Reset() }

var _ pooledItem[*view] = (*viewsItems)(nil)

// view is like a posting list with all shards that might contain data. This also
// contains translation of label matchers which is shard agnostic.
//
// All data for view is read from txt database. Think of this as a guideline on
// what to read from rbf storage.
type view struct {
	meta []meta
	// when provided  applies an intersection of all match
	match []match
	// when provided applies a union of intersecting []match.
	// used with exemplars search.
	matchAny [][]match
}

type match struct {
	rows   []row
	column uint64
	op     bitmaps.OP
}

type row struct {
	predicate int64
	end       int64
}

func (v *row) Depth() uint8 {
	return uint8(bits.Len64(max(uint64(v.predicate), uint64(v.end))))
}

func (v *view) IsEmpty() bool {
	return len(v.meta) == 0
}

func (v *view) Reset() *view {
	v.meta = v.meta[:0]
	v.match = v.match[:0]
	v.matchAny = v.matchAny[:0]
	return v
}

// batch builds rbf containers in memory to allow much faster batch ingestion via
// (*rbf.Tx)AddRoaring call. We automatically organize bitmaps in shards.
type batch map[uint64]*data

func (s batch) Get(shard uint64) *data {
	r, ok := s[shard]
	if !ok {
		r = &data{columns: make(map[uint64]*roaring.Bitmap)}
		r.meta.shard = shard
		s[shard] = r
	}
	return r
}

// data holds all columns bitmaps and metadata for a single shard. Columns contains
// both core (timestamp, value, kind) and indexed columns which are xxhash of the
// label names.
type data struct {
	columns map[uint64]*roaring.Bitmap
	meta    meta
}

// meta  is in memory metadata about rbf shard.
type meta struct {
	// min is the minimum timestamp observed in the shard.
	min int64
	// max is the maximum timestamp observed in the shard
	max int64
	// shard is the current shard shard for this metadata. A  single shard represent
	// 1048576 samples.
	shard uint64
	// below we store bit depth inside this shard for core columns. This removes
	// the need to compute depth during reading.
	depth struct {
		ts    uint8
		value uint8
		kind  uint8
		label uint8
	}
}

func (s *meta) InRange(lo, hi int64) bool {
	return lo < s.max && hi > s.min
}

func (s *meta) Update(other *meta) {
	if s.min == 0 {
		s.min = other.min
	}
	s.min = min(s.min, other.min)
	s.max = max(s.max, other.max)
	s.depth.ts = max(s.depth.ts, other.depth.ts)
	s.depth.value = max(s.depth.value, other.depth.value)
	s.depth.label = max(s.depth.label, other.depth.label)
	s.depth.kind = max(s.depth.kind, other.depth.kind)
}

// AddIndex builds search index from tsid.Prometheus validates that label names are unique,
// which allows us to treat each label name observed as a unique bitmap.
//
// We encode series id as BSI in (keys.MetricsLabels, each label name generates a unique bitmap
// that stores the (column_id, label_value) tuple encoded BSI.
func (s *data) AddIndex(start uint64, values []tsid.ID, kinds []keys.Kind) {
	labels := s.get(keys.MetricsLabels)
	var hi uint64
	id := start
	for i := range values {
		if kinds[i] == keys.None {
			continue
		}
		la := values[i]
		hi = max(la[0].Value, hi)
		bitmaps.BSI(labels, id, int64(la[0].Value))
		for j := range la {
			if j == 0 {
				continue
			}
			bitmaps.BSI(s.get(la[j].ID), id, int64(la[j].Value))
		}
		id++
	}
	s.meta.depth.label = uint8(bits.Len64(hi)) + 1

}

// AddTS encodes values as BSI in keys.MetricsTimestamp bitmap. Unlike prometheus
// we never check for Out Of Order series because they are irrelevant, we always
// sort by time when iterating over series samples.
func (s *data) AddTS(start uint64, values []int64, kinds []keys.Kind) {
	ra := s.get(keys.MetricsTimestamp)
	var lo, hi int64
	id := start
	for i := range values {
		if kinds[i] == keys.None {
			continue
		}
		if lo == 0 {
			lo = values[i]
		}
		lo = min(lo, values[i])
		hi = max(hi, values[i])
		bitmaps.BSI(ra, id, values[i])
		id++
	}
	s.meta.min = lo
	s.meta.max = hi
	s.meta.depth.ts = uint8(bits.Len64(max(uint64(lo), uint64(hi)))) + 1
}

// AddValues builds keys.MetricsValue bitmap. Depending on metric kind value can
// either be a float64 or a dense uint64.
//
// Float metrics will have values in the IEEE 754 binary representation.
func (s *data) AddValues(start uint64, values []uint64, kinds []keys.Kind) {
	ra := s.get(keys.MetricsValue)
	var hi uint64
	id := start
	for i := range values {
		if kinds[i] == keys.None {
			continue
		}
		hi = max(hi, values[i])
		bitmaps.BSI(ra, id, int64(values[i]))
		id++
	}
	s.meta.depth.value = uint8(bits.Len64(hi)) + 1
}

// AddKind builds keys.MetricsType bitmap. This tracks metric value types. keys.None
// values are ignored.
//
// keys.None occurs when we have mixed metadata rows , which resets the values to
// keys.None to avoid leaking metadata into rbf storage, since all metadata lives
// in the txt database.
func (s *data) AddKind(start uint64, values []keys.Kind) {
	ra := s.get(keys.MetricsType)
	var hi keys.Kind
	id := start
	for i := range values {
		if values[i] == keys.None {
			continue
		}
		hi = max(hi, values[i])
		bitmaps.BSI(ra, id, int64(values[i]))
		id++
	}
	s.meta.depth.kind = uint8(bits.Len64(uint64(hi))) + 1
}

func (s *data) get(col uint64) *roaring.Bitmap {
	r, ok := s.columns[col]
	if !ok {
		r = roaring.NewMapBitmap()
		s.columns[col] = r
	}
	return r
}
