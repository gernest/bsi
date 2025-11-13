package storage

import (
	"cmp"
	"errors"
	"fmt"
	"iter"
	"math"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/pools"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/samples"
	"github.com/gernest/bsi/internal/storage/tsid"
	"github.com/gernest/bsi/internal/storage/work"
	"github.com/gernest/roaring"
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
	meta []meta
	// when provided  applies an intersection of all match
	match []match
	// when provided applies a union of intersecting []match.
	// used with exemplars search.
	matchAny [][]match
}

type match struct {
	column string
	rows   []uint64
}

func (m *match) Column() uint64 {
	return xxhash.Sum64String(m.column)
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
	year  uint16
	month uint8
	full  bool
}

func (m *meta) Key(col uint64) rbf.Key {
	return rbf.Key{
		Column: col,
		Shard:  m.shard,
		Year:   m.year,
		Month:  m.month,
	}
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
	s.add(value[0].ID, id, int64(value[0].Value))
	metricID := value[0].Value
	for i := 1; i < len(value); i++ {
		bitmaps.Mutex(s.get(value[i].ID), metricID, value[i].Value)
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
	return db.read(vs, func(tx *rbf.Tx, records *rbf.Records, m *meta) error {
		shard := m.shard
		tsP, ok := records.Get(m.Key(MetricsTimestamp))
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

		kind, ok := records.Get(m.Key(MetricsType))
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

		return readSeries(result, m, tx, records, shard, ra)
	})

}

func (db *Store) read(vs *view, cb func(tx *rbf.Tx, records *rbf.Records, m *meta) error) error {
	tx, err := db.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return err
	}
	for i := range vs.meta {
		err := cb(tx, records, &vs.meta[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Store) read2(vs *view, sa *samples.Samples, start, end int64, cb func(tx *rbf.Tx, records *rbf.Records, filter *roaring.Bitmap, m *meta) error) error {
	tx, err := db.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return err
	}
	for i := range vs.meta {
		// we are looking for column ids that satisfy request conditions within
		// (partition, shard).
		//
		// 1. select all columns ids from MetricsTimestamp column that has values
		// BETWEEN start and end time.
		// We skip execution if no column ids matched.
		//
		// 2. apply matchers
		// matchers are used to generate bitmap of all metrics series ids to read from.
		//
		// If (*view).match is specified , we perform intersection of all metrics_id and
		// if (*view).matchAny is specified , we perform union of all metrics_id for the respective
		// set.
		//
		// 3. intersect 1 & 2 bitmaps, which yields correct column ids to read
		// in the current (partition, shard) tuple.
		m := &vs.meta[i]
		ra, err := selectTimeRange(tx, records, start, end, m)
		if err != nil {
			return err
		}
		if !ra.Any() {
			continue
		}
		series, err := selectSeriesID(tx, records, vs, m)
		if err != nil {
			return err
		}
		ra = ra.Intersect(series)
		if !ra.Any() {
			// fast path: skip current shard.
			continue
		}
		err = cb(tx, records, ra, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func selectSeriesID(tx *rbf.Tx, records *rbf.Records, vs *view, m *meta) (*roaring.Bitmap, error) {
	var (
		ids *roaring.Bitmap
		err error
	)
	if len(vs.match) > 0 {
		ids, err = selectSeriesIDAll(tx, records, vs.match, m)
		if err != nil {
			return nil, err
		}
	}
	if len(vs.matchAny) > 0 {
		ids, err = selectSeriesIDAny(tx, records, vs.matchAny, m)
		if err != nil {
			return nil, err
		}
	}
	if ids != nil {
		if !ids.Any() {
			return ids, nil
		}
		// apply union of all series.
		root, ok := records.Get(m.Key(MetricsLabels))
		if !ok {
			return nil, fmt.Errorf("bitmap %s not found", m.Key(MetricsLabels))
		}
		all := make([]*roaring.Bitmap, 0, ids.Count())
		depth := m.Get(MetricsLabels)
		for value := range ids.RangeAll() {
			ra, err := readBSIRange(tx, root, m.shard, depth, bitmaps.EQ, int64(value), 0)
			if err != nil {
				return nil, err
			}
			all = append(all, ra)
		}
		return all[0].Union(all[1:]...), nil
	}
	return roaring.NewBitmap(), nil
}

func selectSeriesIDAny(tx *rbf.Tx, records *rbf.Records, matchers [][]match, m *meta) (*roaring.Bitmap, error) {
	if len(matchers) == 0 {
		return roaring.NewBitmap(), nil
	}
	all := make([]*roaring.Bitmap, len(matchers))
	var err error
	for i := range matchers {
		all[i], err = selectSeriesIDAll(tx, records, matchers[i], m)
		if err != nil {
			return nil, err
		}
	}
	return all[0].Union(all[1:]...), nil
}

func selectSeriesIDAll(tx *rbf.Tx, records *rbf.Records, matchers []match, m *meta) (*roaring.Bitmap, error) {
	if len(matchers) == 0 {
		return roaring.NewBitmap(), nil
	}
	ra, err := selectRow(tx, records, matchers[0].Column(), matchers[0].rows, m)
	if err != nil {
		return nil, err
	}
	if !ra.Any() {
		return ra, nil
	}

	for i := 1; i < len(matchers); i++ {
		rx, err := selectRow(tx, records, matchers[i].Column(), matchers[i].rows, m)
		if err != nil {
			return nil, err
		}
		if !rx.Any() {
			return rx, nil
		}
		ra = ra.Intersect(rx)
		if !ra.Any() {
			return ra, nil
		}
	}
	return ra, nil
}

func selectRow(tx *rbf.Tx, records *rbf.Records, col uint64, rows []uint64, m *meta) (*roaring.Bitmap, error) {
	result := roaring.NewBitmap()
	for shard, page := range rangeShards(records, col, m.year, m.month) {
		for i := range rows {
			ra, err := readRow(tx, page, shard, rows[i])
			if err != nil {
				return nil, err
			}
			result = result.Union(ra)
		}

	}
	return result, nil
}

func readRow(tx *rbf.Tx, root uint32, shard, row uint64) (*roaring.Bitmap, error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	return bitmaps.Row(cu, shard, row)
}

func rangeShards(records *rbf.Records, col uint64, year uint16, month uint8) iter.Seq2[uint64, uint32] {
	it := records.Iterator()
	lo := rbf.Key{
		Column: col,
		Year:   year,
		Month:  month,
	}
	hi := rbf.Key{
		Column: col,
		Year:   year,
		Month:  month,
		Shard:  math.MaxUint64,
	}
	return func(yield func(uint64, uint32) bool) {
		co := rbf.CompareRecord{}

		for it.Seek(lo); !it.Done(); {
			name, page, ok := it.Next()
			if !ok {
				break
			}
			if co.Compare(name, hi) < 0 {
				if !yield(name.Shard, page) {
					return
				}
				continue
			}
			break
		}
	}
}

// Returns all column ids observed within start and end range inclusively. Returns an error if the timestamp
// bitmap is missing.
func selectTimeRange(tx *rbf.Tx, records *rbf.Records, start, end int64, m *meta) (*roaring.Bitmap, error) {
	ts, ok := records.Get(m.Key(MetricsTimestamp))
	if !ok {
		return nil, fmt.Errorf("bitmap %s not found", m.Key(MetricsTimestamp))
	}
	return readBSIRange(tx, ts, m.shard, m.Get(MetricsTimestamp), bitmaps.BETWEEN, start, end)
}
