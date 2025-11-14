package storage

import (
	"cmp"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/pools"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/row"
	"github.com/gernest/bsi/internal/storage/samples"
	"github.com/gernest/bsi/internal/storage/tsid"
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
)

type viewsItems struct{}

func (viewsItems) Init() *view { return new(view) }

func (viewsItems) Reset(v *view) *view { return v.Reset() }

var _ pools.Items[*view] = (*viewsItems)(nil)

type partitionShard struct {
	Partition uint16
	Shard     uint16
}

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
	rows   *roaring.Bitmap
}

func (m *match) Column() uint64 {
	return xxhash.Sum64String(m.column)
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

type data map[rbf.Key]*roaring.Bitmap

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

func (s data) Index(id uint64, value tsid.ID) {
	s.bsi(value[0].ID, id, int64(value[0].Value))
	metricID := value[0].Value
	for i := 1; i < len(value); i++ {
		s.mutex(value[i].ID, metricID, value[i].Value)
	}
}

func (s data) Timestamp(id uint64, value int64) {
	s.bsi(MetricsTimestamp, id, value)
}

func (s data) Value(id uint64, value uint64) {
	s.bsi(MetricsValue, id, int64(value))
}

func (s data) Kind(id uint64, value Kind) {
	s.mutex(MetricsType, id, uint64(value))
}

func (s data) bsi(col uint64, id uint64, val int64) {
	shard := id / shardwidth.ShardWidth
	bitmaps.BSI(s.get(shard, col), id, val)
}

func (s data) mutex(col uint64, id uint64, val uint64) {
	shard := id / shardwidth.ShardWidth
	bitmaps.Mutex(s.get(shard, col), id, val)
}

func (s data) get(shard, col uint64) *roaring.Bitmap {
	kx := rbf.Key{Column: col, Shard: shard}
	r, ok := s[kx]
	if !ok {
		r = roaring.NewMapBitmap()
		s[kx] = r
	}
	return r
}

func (db *Store) readSeries(result *samples.Samples, vs *view, start, end int64) error {
	return db.read(vs, start, end, false, func(tx *rbf.Tx, records *rbf.Records, ra *roaring.Bitmap, shard uint64) error {
		return readSeries(result, tx, records, shard, ra)
	})
}

func (db *Store) read(vs *view, start, end int64, exemplar bool, cb func(tx *rbf.Tx, records *rbf.Records, filter *roaring.Bitmap, shard uint64) error) error {
	tx, err := db.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return err
	}

	// 1. select all column ids in the time range
	ts, err := row.Range(tx, records, row.Unlimited(), MetricsTimestamp, bitmaps.BETWEEN, start, end)
	if err != nil {
		return err
	}

	// 2. select all series based on matcher conditions
	series, err := selectSeriesID(tx, records, vs)
	if err != nil {
		return err
	}

	// 3. select column ids matching series, limit to valid shards found in ts
	filter, err := row.Mutex(tx, records, ts.Limit(), MetricsLabels, series)
	if err != nil {
		return err
	}

	// 4. read exemplars
	//
	// we store exemplar along with the other timeseries. We need to distinguish between normal timeseries
	// and exemplar columns.
	exe, err := row.Mutex(tx, records, ts.Limit(), MetricsType, roaring.NewBitmap(uint64(Exemplar)))
	if err != nil {
		return err
	}
	if exemplar {
		filter = filter.Intersect(exe)
	} else {
		filter = filter.Difference(exe)
	}

	// 4. intersect time range with filter to get matching column ids.
	match := ts.Intersect(row.NewRowFromBitmap(filter))

	// matching logic is is complete now. After this only reading is done.
	if !match.Any() {
		// fast path: nothing matched return early.
		return nil
	}

	for i := range match.Segments {
		err := cb(tx, records, match.Segments[i].Data(), match.Segments[i].Shard())
		if err != nil {
			return err
		}
	}
	return nil
}

func selectSeriesID(tx *rbf.Tx, records *rbf.Records, vs *view) (*roaring.Bitmap, error) {
	if len(vs.match) > 0 {
		return selectSeriesIDAll(tx, records, vs.match)
	}
	if len(vs.matchAny) > 0 {
		return selectSeriesIDAny(tx, records, vs.matchAny)
	}

	return roaring.NewBitmap(), nil
}

func selectSeriesIDAny(tx *rbf.Tx, records *rbf.Records, matchers [][]match) (*roaring.Bitmap, error) {
	if len(matchers) == 0 {
		return roaring.NewBitmap(), nil
	}
	all := make([]*roaring.Bitmap, len(matchers))
	var err error
	for i := range matchers {
		all[i], err = selectSeriesIDAll(tx, records, matchers[i])
		if err != nil {
			return nil, err
		}
	}
	return all[0].Union(all[1:]...), nil
}

func selectSeriesIDAll(tx *rbf.Tx, records *rbf.Records, matchers []match) (*roaring.Bitmap, error) {
	if len(matchers) == 0 {
		return roaring.NewBitmap(), nil
	}
	ra, err := selectRow(tx, records, matchers[0].Column(), matchers[0].rows)
	if err != nil {
		return nil, err
	}
	if !ra.Any() {
		return ra, nil
	}

	for i := 1; i < len(matchers); i++ {
		rx, err := selectRow(tx, records, matchers[i].Column(), matchers[i].rows)
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

func selectRow(tx *rbf.Tx, records *rbf.Records, col uint64, rows *roaring.Bitmap) (*roaring.Bitmap, error) {
	return row.Mutex(tx, records, row.Unlimited(), col, rows)
}
