package storage

import (
	"fmt"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/samples"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
)

// SelectExemplar implements storage.ExemplarQuerier.
func (db *Store) SelectExemplar(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	shards, err := db.findShardsAmy(start, end, matchers)
	if err != nil {
		return nil, err
	}
	defer shardsPool.Put(shards)

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
	tx, err := db.rbf.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return err
	}

	for i := range vs.meta {
		shard := uint64(vs.meta[i].shard)
		tsP, ok := records.Get(rbf.Key{Column: MetricsTimestamp, Shard: shard})
		if !ok {
			panic("missing ts root records")
		}
		ra, err := readBSIRange(tx, tsP, shard, vs.meta[i].depth.ts, bitmaps.BETWEEN, start, end)
		if err != nil {
			return err
		}
		if !ra.Any() {
			continue
		}

		kind, ok := records.Get(rbf.Key{Column: MetricsType, Shard: shard})
		if !ok {
			panic("missing metric type root records")
		}
		exe, err := readBSIRange(tx, kind, shard, vs.meta[i].depth.kind, bitmaps.EQ, int64(Exemplar), 0)
		if err != nil {
			return err
		}

		ra = ra.Intersect(exe)
		if !ra.Any() {
			continue
		}

		ra, err = applyBSIFiltersAny(tx, records, shard, ra, vs.matchAny)
		if err != nil {
			return fmt.Errorf("applying filters %w", err)
		}

		err = readExemplars(result, vs.meta[i], tx, records, shard, ra)
		if err != nil {
			return err
		}
	}
	return nil
}
