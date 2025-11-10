package storage

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"math/bits"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/pools"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/gernest/bsi/internal/storage/raw"
	"github.com/gernest/bsi/internal/storage/samples"
	"github.com/gernest/bsi/internal/storage/tsid"
	"github.com/gernest/bsi/internal/storage/work"
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"go.etcd.io/bbolt"
)

var (
	workPool   = pools.Pool[*partitionShardWork]{Init: workItems{}}
	rawBSIPool = pools.Pool[*raw.BSI]{Init: bsiItems{}}
)

type bsiItems struct{}

func (bsiItems) Init() *raw.BSI {
	b := new(raw.BSI)
	b.Init()
	return b
}

func (bsiItems) Reset(v *raw.BSI) *raw.BSI { return v.Reset() }

var _ pools.PooledItem[*raw.BSI] = (*bsiItems)(nil)

type viewsItems struct{}

func (viewsItems) Init() *view { return new(view) }

func (viewsItems) Reset(v *view) *view { return v.Reset() }

var _ pools.PooledItem[*view] = (*viewsItems)(nil)

type partitionShard struct {
	Partition uint16
	Shard     uint16
}

type partitionShardWork = work.Work[partitionShard]

type workItems struct{}

func (workItems) Init() *partitionShardWork {
	return new(partitionShardWork)
}

func (workItems) Reset(v *partitionShardWork) *partitionShardWork {
	return v.Reset()
}

var _ pools.PooledItem[*partitionShardWork] = (*workItems)(nil)

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
	column string
	rows   []uint64
	depth  uint8
	op     bitmaps.OP
}

func (m *match) Column() uint64 {
	return xxhash.Sum64String(m.column)
}

func (v *view) IsEmpty() bool {
	return len(v.meta) == 0
}

func (v *view) Reset() *view {
	v.partition = v.partition[:0]
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

func (y *yyyyMM) Compare(o *yyyyMM) int {
	x := cmp.Compare(y.year, o.year)
	if x != 0 {
		return x
	}
	return cmp.Compare(y.month, o.month)
}

type shardYM struct {
	shard uint64
	ym    yyyyMM
}

func (s shardYM) String() string {
	return partitionPath("", s)
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

func partitionPath(base string, ym shardYM) string {
	return filepath.Join(base, ym.ym.String(), fmt.Sprintf("%06d", ym.shard))
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
	// true if the shard is full.
	full bool
}

const mask = uint64(shardwidth.ShardWidth - 1)

func (s *meta) SetFull(id uint64) {
	if s.full {
		return
	}
	s.full = (id & mask) == 0
}

func (s *meta) InRange(lo, hi int64) bool {
	return lo <= s.max && hi >= s.min
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
	if !s.full {
		s.full = other.full
	}
}

func (s *data) Index(id uint64, value tsid.ID) {
	bitmaps.BSI(s.get(MetricsLabels), id, int64(value[0].Value))
	for i := 1; i < len(value); i++ {
		bitmaps.BSI(s.get(value[i].ID), id, int64(value[i].Value))
	}
	depth := uint8(bits.Len64(uint64(value[0].Value))) + 1
	s.meta.depth.label = max(s.meta.depth.label, depth)
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
	s.meta.depth.value = max(s.meta.depth.value, depth)
}

func (s *data) Kind(id uint64, value Kind) {
	ra := s.get(MetricsType)
	bitmaps.BSI(ra, id, int64(value))
	depth := uint8(bits.Len64(uint64(value))) + 1
	s.meta.depth.kind = max(s.meta.depth.kind, depth)
}

func (s *data) get(col uint64) *roaring.Bitmap {
	r, ok := s.columns[col]
	if !ok {
		r = roaring.NewMapBitmap()
		s.columns[col] = r
	}
	return r
}

func (db *Store) readSeries(result *samples.Samples, vs *view, start, end int64) error {
	return db.read(vs, func(tx *rbf.Tx, records *rbf.Records, m meta) error {
		shard := m.shard
		tsP, ok := records.Get(MetricsTimestamp)
		if !ok {
			return errors.New("missing ts root records")
		}
		ra, err := readBSIRange(tx, tsP, shard, m.depth.ts, bitmaps.BETWEEN, start, end)
		if err != nil {
			return err
		}
		if !ra.Any() {
			return nil
		}

		kind, ok := records.Get(MetricsType)
		if !ok {
			return errors.New("missing metric type root records")
		}
		float, err := readBSIRange(tx, kind, shard, m.depth.kind, bitmaps.EQ, int64(Float), 0)
		if err != nil {
			return err
		}
		histogram, err := readBSIRange(tx, kind, shard, m.depth.kind, bitmaps.EQ, int64(Histogram), 0)
		if err != nil {
			return err
		}

		// metric samples can either be histograms or floats.
		ra = ra.Intersect(float.Union(histogram))
		if !ra.Any() {
			return nil
		}

		ra, err = applyBSIFilters(tx, records, shard, ra, vs.match)
		if err != nil {
			return fmt.Errorf("applying filters %w", err)
		}

		return readSeries(result, m, tx, records, shard, ra)
	})

}

func (db *Store) read(vs *view, cb func(tx *rbf.Tx, records *rbf.Records, m meta) error) error {

	w := workPool.Get()
	defer workPool.Put(w)

	// Generate shards positions that we will be working with concurrently
	w.Init(func(yield func(partitionShard) bool) {
		for i := range vs.partition {
			for j := range vs.meta[i] {
				if !yield(partitionShard{Partition: uint16(i), Shard: uint16(j)}) {
					return
				}
			}
		}
	})

	// Distribute work cross all available cores.
	w.Do(runtime.GOMAXPROCS(0), func(item partitionShard) error {
		m := vs.meta[item.Partition][item.Shard]
		err := db.partition(vs.partition[item.Partition], m.shard, false, func(tx *rbf.Tx) error {
			records, err := tx.RootRecords()
			if err != nil {
				return err
			}
			return cb(tx, records, m)
		})
		if err != nil {
			return fmt.Errorf("reading shard=%s %w", shardYM{
				shard: m.shard,
				ym:    vs.partition[item.Partition],
			}, err)
		}
		return nil
	})

	return nil
}

// walkPartitions iterates over all partitions within the lo and hi range. his is inclusive because we
// want the last (year, month) to be searched as well.
//
// Returns any error returned by cb.
func walkPartitions(tx *bbolt.Tx, lo, hi yyyyMM, cb func(key yyyyMM, m meta) error) error {
	adminB := tx.Bucket(admin)
	acu := adminB.Cursor()
	from := []byte(lo.String())
	to := []byte(hi.String())

	for a, b := acu.Seek(from); a != nil && b == nil && bytes.Compare(a, to) < 1; a, b = acu.Next() {

		ym, err := parsePartitionKey(magic.String(a))
		if err != nil {
			return err
		}
		err = adminB.Bucket(a).ForEach(func(_, v []byte) error {
			return cb(ym, magic.ReinterpretSlice[meta](v)[0])
		})
		if err != nil {
			return err
		}
	}
	return nil
}
