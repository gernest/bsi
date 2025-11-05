package storage

import (
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.etcd.io/bbolt"
)

// Stats returns database stats.
func (db *Store) Stats(statsByLabelName string, _ int) (result *tsdb.Stats, err error) {
	err = db.txt.View(func(tx *bbolt.Tx) error {
		dataB := tx.Bucket(metaData)
		dataI := dataB.Inspect()
		result = &tsdb.Stats{
			NumSeries: uint64(dataI.KeyN),
		}

		adminB := tx.Bucket(admin)
		cu := adminB.Cursor()
		if _, v := cu.First(); v != nil {
			result.MinTime = magic.ReinterpretSlice[meta](v)[0].min
		}
		if _, v := cu.Last(); v != nil {
			result.MaxTime = magic.ReinterpretSlice[meta](v)[0].max
		}
		indexB := tx.Bucket(search)
		cu = indexB.Cursor()

		px := &index.PostingsStats{}
		// we are iterating over buckets
		for k, v := cu.First(); k != nil && v == nil; k, v = cu.Next() {
			mb := indexB.Bucket(k)
			i := mb.Inspect()
			if magic.String(k) == statsByLabelName {
				px.CardinalityMetricsStats = append(px.CardinalityMetricsStats, index.Stat{
					Name:  string(k),
					Count: uint64(i.KeyN),
				})
			}
			px.NumLabelPairs += i.KeyN
			px.CardinalityLabelStats = append(px.CardinalityLabelStats, index.Stat{
				Name:  string(k),
				Count: uint64(i.KeyN),
			})
		}
		result.IndexPostingStats = px
		return nil
	})
	return
}
