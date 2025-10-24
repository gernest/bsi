package storage

import (
	"github.com/gernest/roaring/shardwidth"
	"github.com/gernest/u128/bitmaps"
	"github.com/gernest/u128/rbf"
	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/keys"
	"github.com/gernest/u128/storage/tsid"
)

var bytesPool buffer.Pool

// buildIndex builds bitmap index for timeseries.
func buildIndex(m rbf.Map, tsid *tsid.ID, id uint64, ts int64, value uint64, isHistogram bool) {
	shard := id / shardwidth.ShardWidth

	// 1. store value
	valueB := m.Get(keys.MetricsValue, shard)
	bitmaps.BSI(valueB, id, int64(value))

	// 2. store timestamp
	tsB := m.Get(keys.MetricsTimestamp, shard)
	bitmaps.BSI(tsB, id, ts)

	// 3. store labels
	labelsB := m.Get(keys.MetricsLabels, shard)
	bitmaps.BSI(labelsB, id, int64(tsid.ID))

	histogramB := m.Get(keys.MetricsHistogram, shard)
	bitmaps.Boolean(histogramB, id, isHistogram)

	// Create search index
	for i := range tsid.Rows {
		ra := m.Get(tsid.Views[i], shard)
		bitmaps.Mutex(ra, id, tsid.Rows[i])
	}
}
