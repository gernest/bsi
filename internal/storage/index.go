package storage

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/pools"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/gernest/bsi/internal/storage/samples"
	"github.com/gernest/bsi/internal/storage/tsid"
	"github.com/gernest/bsi/internal/storage/work"
	"github.com/gernest/roaring"
	"go.etcd.io/bbolt"
)

var (
	workPool = pools.Pool[*partitionShardWork]{Init: workItems{}}
)

type viewsItems struct{}

func (viewsItems) Init() *view { return new(view) }

func (viewsItems) Reset(v *view) *view { return v.Reset() }

var _ pools.Items[*view] = (*viewsItems)(nil)

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

var _ pools.Items[*partitionShardWork] = (*workItems)(nil)

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
		r = &data{
			columns: make(map[uint64]*roaring.Bitmap),
			meta:    make(map[uint64]*minMax),
		}
		s[shard] = r
	}
	return r
}

// data holds all columns bitmaps and metadata for a single shard. Columns contains
// both core (timestamp, value, kind) and indexed columns which are xxhash of the
// label names.
type data struct {
	columns map[uint64]*roaring.Bitmap
	meta    map[uint64]*minMax
}

// meta  is in memory metadata about rbf shard.
type meta struct {
	depth []bounds
	shard uint64
	full  bool
}

func (m *meta) Get(col uint64) uint8 {
	i, ok := slices.BinarySearchFunc(m.depth, bounds{col: col}, func(i, j bounds) int {
		return cmp.Compare(i.col, j.col)
	})
	if ok {
		return m.depth[i].minMax.depth()
	}
	return 0
}

func (s *data) Index(id uint64, value tsid.ID) {
	for i := range value {
		s.add(value[i].ID, id, int64(value[i].Value))
	}
}

func (s *data) Timestamp(id uint64, value int64) {
	s.add(MetricsTimestamp, id, value)
}

func (s *data) Value(id uint64, value uint64) {
	s.add(MetricsValue, id, int64(value))
}

func (s *data) Kind(id uint64, value Kind) {
	s.add(MetricsType, id, int64(value))
}

func (s *data) add(col uint64, id uint64, val int64) {
	bitmaps.BSI(s.get(col), id, val)
	x, ok := s.meta[col]
	if !ok {
		s.meta[col] = &minMax{
			min: val,
			max: val,
		}
		return
	}
	x.set(val)
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
		ra, err := readBSIRange(tx, tsP, shard, m.Get(MetricsTimestamp), bitmaps.BETWEEN, start, end)
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
		float, err := readBSIRange(tx, kind, shard, m.Get(MetricsType), bitmaps.EQ, int64(Float), 0)
		if err != nil {
			return err
		}
		histogram, err := readBSIRange(tx, kind, shard, m.Get(MetricsType), bitmaps.EQ, int64(Histogram), 0)
		if err != nil {
			return err
		}

		// metric samples can either be histograms or floats.
		ra = ra.Intersect(float.Union(histogram))
		if !ra.Any() {
			return nil
		}

		ra, err = applyBSIFilters(tx, records, &m, ra, vs.match)
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
	return w.Do(runtime.GOMAXPROCS(0), func(item partitionShard) error {
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
		err = adminB.Bucket(a).ForEach(func(k, v []byte) error {
			o := magic.ReinterpretSlice[bounds](v)
			return cb(ym, meta{
				shard: binary.BigEndian.Uint64(k),
				depth: slices.Clone(o),
			})
		})
		if err != nil {
			return err
		}
	}
	return nil
}
