package storage

import (
	"hash/maphash"
	"iter"
	"math"
	"time"

	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"github.com/gernest/u128/bitmaps"
	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/keys"
	"github.com/gernest/u128/storage/tsid"
)

const ()

var bytesPool buffer.Pool

type Map struct {
	h2ra  map[uint64]*roaring.Bitmap
	h2txt map[uint64]string
	seed  maphash.Seed
}

func NewMap() *Map {
	return &Map{
		h2ra:  make(map[uint64]*roaring.Bitmap),
		h2txt: make(map[uint64]string),
		seed:  maphash.MakeSeed(),
	}
}

// Range iterates over all computed bitmaps.
func (m *Map) Range() iter.Seq2[string, *roaring.Bitmap] {
	return func(yield func(string, *roaring.Bitmap) bool) {
		for k, ra := range m.h2ra {
			ra.Optimize()
			if !yield(m.h2txt[k], ra) {
				return
			}
		}
	}
}

func (m *Map) Get(name []byte) *roaring.Bitmap {
	sum := maphash.Bytes(m.seed, name)
	ra, ok := m.h2ra[sum]
	if !ok {
		ra = roaring.NewMapBitmap()
		m.h2ra[sum] = ra
		m.h2txt[sum] = string(name)
	}
	return ra
}

// Index builds bitmap index for timeseries.
func (m *Map) Index(tsid *tsid.ID, id uint64, ts int64, value float64) {
	shard := id / shardwidth.ShardWidth

	vb := bytesPool.Get()

	// all data is partitioned using ISO 8601 year and week
	year, week := time.UnixMilli(ts).ISOWeek()
	view := keys.View(vb, year, week)

	kb := bytesPool.Get()

	// 1. store value
	valueB := m.Get(keys.Key(kb, keys.DataIndex, keys.MetricsValue, view, shard))
	bitmaps.BSI(valueB, id, int64(math.Float64bits(value)))

	// 2. store timestamp
	tsB := m.Get(keys.Key(kb, keys.DataIndex, keys.MetricsTimestamp, view, shard))
	bitmaps.BSI(tsB, id, ts)

	// 3. store labels
	labelsB := m.Get(keys.Key(kb, keys.DataIndex, keys.MetricsLabels, view, shard))
	bitmaps.BSI(labelsB, id, int64(tsid.ID))

	// Create search index
	for i := range tsid.Rows {
		ra := m.Get(keys.Key(kb, keys.SearchIndex, tsid.Views[i], view, shard))
		bitmaps.BSI(ra, id, int64(tsid.Rows[i]))
	}
}
