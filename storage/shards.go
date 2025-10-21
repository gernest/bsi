package storage

import (
	"iter"

	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"github.com/gernest/u128/bitmaps"
	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/keys"
	"github.com/gernest/u128/storage/magic"
	"github.com/gernest/u128/storage/tsid"
)

const ()

var bytesPool buffer.Pool

type Map struct {
	data map[string]*roaring.Bitmap
}

func NewMap() *Map {
	return &Map{
		data: make(map[string]*roaring.Bitmap),
	}
}

// Range iterates over all computed bitmaps.
func (m *Map) Range() iter.Seq2[string, *roaring.Bitmap] {
	return func(yield func(string, *roaring.Bitmap) bool) {
		for k, ra := range m.data {
			ra.Optimize()
			if !yield(k, ra) {
				return
			}
		}
	}
}

// Get returns a bitmap for the given name. We use unsafe to refer to name, make sure
// name is live until  Range method returns.
func (m *Map) Get(name []byte) *roaring.Bitmap {
	sum := magic.String(name)
	ra, ok := m.data[sum]
	if !ok {
		ra = roaring.NewMapBitmap()
		m.data[sum] = ra
	}
	return ra
}

// Index builds bitmap index for timeseries.
func (m *Map) Index(tsid *tsid.ID, id uint64, ts int64, value uint64, isHistogram bool, view []byte) {
	shard := id / shardwidth.ShardWidth

	kb := bytesPool.Get()

	// 1. store value
	valueB := m.Get(keys.Key(kb, keys.DataIndex, keys.MetricsValue, view, shard))
	bitmaps.BSI(valueB, id, int64(value))

	// 2. store timestamp
	tsB := m.Get(keys.Key(kb, keys.DataIndex, keys.MetricsTimestamp, view, shard))
	bitmaps.BSI(tsB, id, ts)

	// 3. store labels
	labelsB := m.Get(keys.Key(kb, keys.DataIndex, keys.MetricsLabels, view, shard))
	bitmaps.BSI(labelsB, id, int64(tsid.ID))

	histogramB := m.Get(keys.Key(kb, keys.DataIndex, keys.MetricsValue, view, shard))
	bitmaps.Boolean(histogramB, id, isHistogram)

	// Create search index
	for i := range tsid.Rows {
		ra := m.Get(keys.Key(kb, keys.SearchIndex, tsid.Views[i], view, shard))
		bitmaps.BSI(ra, id, int64(tsid.Rows[i]))
	}
}
