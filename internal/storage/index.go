package storage

import (
	"fmt"
	"math/bits"
	"strconv"
	"strings"
	"time"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/tsid"
	"github.com/gernest/roaring"
)

type viewsItems struct{}

func (viewsItems) Init() *view { return new(view) }

func (viewsItems) Reset(v *view) *view { return v.Reset() }

var _ PooledItem[*view] = (*viewsItems)(nil)

// view is like a posting list with all shards that might contain data. This also
// contains translation of label matchers which is shard agnostic.
//
// All data for view is read from txt database. Think of this as a guideline on
// what to read from rbf storage.
type view struct {
	partition []yyyyMM
	meta      [][]meta
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

type yyyyMM struct {
	year  int
	month time.Month
}

func partitionKey(t int64) yyyyMM {
	yy, mm, _ := time.UnixMilli(t).Date()
	return yyyyMM{year: yy, month: mm}
}

func parsePartitionKey(k string) (yyyyMM, error) {
	y, m, ok := strings.Cut(k, "_")
	if !ok {
		return yyyyMM{}, fmt.Errorf("invalid partition key")
	}
	yy, err := strconv.Atoi(y)
	if err != nil {
		return yyyyMM{}, fmt.Errorf("invalid partition year %w", err)
	}
	mm, err := strconv.Atoi(m)
	if err != nil {
		return yyyyMM{}, fmt.Errorf("invalid partition month %w", err)
	}
	return yyyyMM{year: yy, month: time.Month(mm)}, nil
}

func (y yyyyMM) String() string {
	return fmt.Sprintf("%04d_%02d", y.year, y.month)
}

type partitions map[yyyyMM]batch

func (pa partitions) get(shard uint64, ts int64) *data {
	yy, mm, _ := time.UnixMilli(ts).Date()
	key := yyyyMM{
		year: yy, month: mm,
	}
	ba, ok := pa[key]
	if !ok {
		ba = make(batch)
		pa[key] = ba
	}
	return ba.Get(shard)
}

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
// We encode series id as BSI in (MetricsLabels, each label name generates a unique bitmap
// that stores the (column_id, label_value) tuple encoded BSI.
func (s *data) AddIndex(start uint64, values []tsid.ID, kinds []Kind) {
	labels := s.get(MetricsLabels)
	var hi uint64
	id := start
	for i := range values {
		if kinds[i] == None {
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

func (s *data) Index(id uint64, value tsid.ID) {
	bitmaps.BSI(s.get(MetricsLabels), id, int64(value[0].Value))
	for i := 1; i < len(value); i++ {
		bitmaps.BSI(s.get(value[i].ID), id, int64(value[i].Value))
	}
	depth := uint8(bits.Len64(uint64(value[0].Value))) + 1
	s.meta.depth.label = max(s.meta.depth.ts, depth)
}

func (s *data) Timestamp(id uint64, value int64) {
	ra := s.get(MetricsTimestamp)
	bitmaps.BSI(ra, id, value)
	if s.meta.min == 0 {
		s.meta.min = value
	} else {
		s.meta.min = min(s.meta.min, value)
	}
	s.meta.max = max(s.meta.max, value)
	depth := uint8(bits.Len64(uint64(value))) + 1
	s.meta.depth.ts = max(s.meta.depth.ts, depth)
}

func (s *data) Value(id uint64, value uint64) {
	ra := s.get(MetricsValue)
	bitmaps.BSI(ra, id, int64(value))
	depth := uint8(bits.Len64(uint64(value))) + 1
	s.meta.depth.value = max(s.meta.depth.ts, depth)
}

func (s *data) Kind(id uint64, value Kind) {
	ra := s.get(MetricsType)
	bitmaps.BSI(ra, id, int64(value))
	depth := uint8(bits.Len64(uint64(value))) + 1
	s.meta.depth.kind = max(s.meta.depth.ts, depth)
}

func (s *data) get(col uint64) *roaring.Bitmap {
	r, ok := s.columns[col]
	if !ok {
		r = roaring.NewMapBitmap()
		s.columns[col] = r
	}
	return r
}

func (db *Store) read(vs *view, cb func(tx *rbf.Tx, records *rbf.Records, m meta) error) error {
	for i := range vs.partition {
		err := db.partition(vs.partition[i], false, func(tx *rbf.Tx) error {
			records, err := tx.RootRecords()
			if err != nil {
				return err
			}
			for _, m := range vs.meta[i] {
				err := cb(tx, records, m)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}
