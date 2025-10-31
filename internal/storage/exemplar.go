package storage

import (
	"fmt"

	"github.com/gernest/u128/internal/bitmaps"
	"github.com/gernest/u128/internal/rbf"
	"github.com/gernest/u128/internal/storage/keys"
	"github.com/gernest/u128/internal/storage/samples"
	"github.com/gernest/u128/internal/storage/views"
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

	result := samples.Get()
	defer result.Release()

	err = db.readExemplar(result, shards, start, end)
	if err != nil {
		return nil, err
	}

	err = db.translate(result)
	if err != nil {
		return nil, err
	}
	return result.MakeExemplar(), nil
}

func (db *Store) readExemplar(result *samples.Samples, vs *views.List, start, end int64) error {
	tx, err := db.rbf.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return err
	}

	for i := range vs.Shards {
		shard := vs.Shards[i]
		tsP, ok := records.Get(rbf.Key{Column: keys.MetricsTimestamp, Shard: shard})
		if !ok {
			panic("missing ts root records")
		}
		ra, err := readBSIRange(tx, tsP, shard, vs.Meta[i].TsDepth, bitmaps.BETWEEN, start, end)
		if err != nil {
			return err
		}
		if !ra.Any() {
			continue
		}

		kind, ok := records.Get(rbf.Key{Column: keys.MetricsType, Shard: shard})
		if !ok {
			panic("missing metric type root records")
		}
		exe, err := readBSIRange(tx, kind, shard, vs.Meta[i].KindDepth, bitmaps.EQ, int64(keys.Exemplar), 0)
		if err != nil {
			return err
		}

		ra = ra.Intersect(exe)
		if !ra.Any() {
			continue
		}

		ra, err = applyBSIFiltersAny(tx, records, shard, ra, vs.SearchAny)
		if err != nil {
			return fmt.Errorf("applying filters %w", err)
		}

		err = readExemplars(result, vs.Meta[i], tx, records, shard, ra)
		if err != nil {
			return err
		}
	}
	return nil
}
