package storage

import (
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/keys"
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/gernest/bsi/internal/storage/samples"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"
)

// LabelValues implements storage.LabelQuerier.
func (db *Store) LabelValues(start, end int64, limit int, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	matchers = append([]*labels.Matcher{
		{
			Name:  labels.MetricName,
			Type:  labels.MatchEqual,
			Value: name,
		},
	}, matchers...)
	match := map[string]struct{}{}
	err := db.series(start, end, matchers, func(n, value []byte) error {
		if magic.String(n) == name {
			match[string(value)] = struct{}{}
			limit--
		}
		if limit > 0 {
			return nil
		}
		return io.EOF
	})
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, nil, err
		}
	}
	values := make([]string, 0, len(match))
	for k := range match {
		values = append(values, k)
	}
	slices.Sort(values)
	return values, nil, nil
}

// LabelNames implements storage.LabelQuerier.
func (db *Store) LabelNames(start, end int64, limit int, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	match := map[string]struct{}{}
	err := db.series(start, end, matchers, func(name, _ []byte) error {
		match[string(name)] = struct{}{}
		limit--
		if limit > 0 {
			return nil
		}
		return io.EOF
	})
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, nil, err
		}
	}
	names := make([]string, 0, len(match))
	for k := range match {
		names = append(names, k)
	}
	slices.Sort(names)
	return names, nil, nil
}

func (db *Store) series(start, end int64, matchers []*labels.Matcher, cb func(name, value []byte) error) error {
	shards, err := db.findShards(start, end, matchers)
	if err != nil {
		return err
	}
	defer shardsPool.Put(shards)

	if shards.IsEmpty() {
		return nil
	}

	var result samples.Samples
	result.Init()

	err = db.readSeries(&result, shards, start, end)
	if err != nil {
		return nil
	}

	err = db.translate(&result)
	if err != nil {
		return nil
	}
	return result.MakeSeries(cb)
}

func (db *Store) readSeries(result *samples.Samples, vs *view, start, end int64) error {
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
		tsP, ok := records.Get(rbf.Key{Column: keys.MetricsTimestamp, Shard: shard})
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

		kind, ok := records.Get(rbf.Key{Column: keys.MetricsType, Shard: shard})
		if !ok {
			panic("missing metric type root records")
		}
		float, err := readBSIRange(tx, kind, shard, vs.meta[i].depth.kind, bitmaps.EQ, int64(keys.Float), 0)
		if err != nil {
			return err
		}
		histogram, err := readBSIRange(tx, kind, shard, vs.meta[i].depth.kind, bitmaps.EQ, int64(keys.Histogram), 0)
		if err != nil {
			return err
		}

		// metric samples can either be histograms or floats.
		ra = ra.Intersect(float.Union(histogram))
		if !ra.Any() {
			continue
		}

		ra, err = applyBSIFilters(tx, records, shard, ra, vs.match)
		if err != nil {
			return fmt.Errorf("applying filters %w", err)
		}

		err = readSeries(result, vs.meta[i], tx, records, shard, ra)
		if err != nil {
			return err
		}
	}
	return nil
}
