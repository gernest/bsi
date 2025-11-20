package storage

import (
	"sort"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/row"
	"github.com/gernest/bsi/internal/storage/samples"
	"github.com/gernest/bsi/internal/storage/tsid"
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
)

type view struct {
	match    []match
	matchAny [][]match
}

func (v *view) Clamp() {
	v.match = clamp(v.match)
	for i := range v.matchAny {
		v.matchAny[i] = clamp(v.matchAny[i])
	}
}

func clamp(all []match) []match {
	if len(all) < 2 {
		return all
	}
	sort.SliceStable(all, func(i, _ int) bool {
		return all[i].exists
	})
	for i := range all {
		if !all[i].exists {
			if i != 0 {
				all = all[i-1:]
			}
			break
		}
	}
	return all
}

type match struct {
	rows   *roaring.Bitmap
	column string
	id     uint64
	exists bool
}

func (m *match) Column() uint64 {
	if m.exists {
		return MetricsLabelsExistence
	}
	return m.id
}

func (v *view) Reset() *view {
	v.match = v.match[:0]
	v.matchAny = v.matchAny[:0]
	return v
}

type data map[rbf.Key]*roaring.Bitmap

func (s data) Index(id uint64, value tsid.ID) {
	var (
		idx      int
		metricID uint64
	)

	for col, val := range value.Range() {
		if idx == 0 {
			metricID = val
			s.bsi(col, id, int64(val))
			s.exists(MetricsLabelsExistence, metricID)
		} else {
			s.mutex(col, metricID, val)
		}
		idx++
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

func (s data) exists(col uint64, id uint64) {
	shard := id / shardwidth.ShardWidth
	bitmaps.Exist(s.get(shard, col), id)
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
	return db.read(vs, start, end, all, func(tx *rbf.Tx, records *rbf.Records, ra *roaring.Bitmap, shard uint64) error {
		return readSeries(result, tx, records, shard, ra)
	})
}

type readState uint8

const (
	all readState = 1 + iota
	baseMetrics
	baseExemplar
)

func (db *Store) read(vs *view, start, end int64, state readState, cb func(tx *rbf.Tx, records *rbf.Records, filter *roaring.Bitmap, shard uint64) error) error {
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
	filter, err := row.Eq(tx, records, ts.Limit(), MetricsLabels, series)
	if err != nil {
		return err
	}

	if state != all {
		exe, err := row.Mutex(tx, records, ts.Limit(), MetricsType, roaring.NewBitmap(uint64(Exemplar)))
		if err != nil {
			return err
		}
		if state == baseExemplar {
			filter = filter.Intersect(exe)
		} else {
			filter = filter.Difference(exe)
		}
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
	ra, err := selectRow(tx, records, &matchers[0])
	if err != nil {
		return nil, err
	}
	if !ra.Any() {
		return ra, nil
	}

	for i := 1; i < len(matchers); i++ {
		rx, err := selectRow(tx, records, &matchers[i])
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

func selectRow(tx *rbf.Tx, records *rbf.Records, m *match) (*roaring.Bitmap, error) {
	if m.exists {
		return row.Exists(tx, records, row.Unlimited(), m.Column())
	}
	return row.Mutex(tx, records, row.Unlimited(), m.Column(), m.rows)
}
