package storage

import (
	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/samples"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
)

// SelectExemplar implements storage.ExemplarQuerier.
func (db *Store) SelectExemplar(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	shards := shardsPool.Get()
	defer shardsPool.Put(shards)
	err := db.findShardsAmy(shards, start, end, matchers)
	if err != nil {
		return nil, err
	}

	var result samples.Samples
	result.Init()

	err = db.readExemplar(&result, shards, start, end)
	if err != nil {
		return nil, err
	}

	err = db.translate(&result)
	if err != nil {
		return nil, err
	}
	return result.MakeExemplar(), nil
}

func (db *Store) readExemplar(result *samples.Samples, vs *view, start, end int64) error {

	return db.read(vs, func(tx *rbf.Tx, records *rbf.Records, m *meta) error {
		shard := m.shard
		tsP, ok := records.Get(m.Key(MetricsTimestamp))
		if !ok {
			panic("missing ts root records")
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
			panic("missing metric type root records")
		}
		exe, err := readBSIRange(tx, kind, shard, m.Get(MetricsType), bitmaps.EQ, int64(Exemplar), 0)
		if err != nil {
			return err
		}

		ra = ra.Intersect(exe)
		if !ra.Any() {
			return nil
		}

		return readExemplars(result, m, tx, records, shard, ra)

	})
}
