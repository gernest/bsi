package storage

import (
	"errors"
	"io"
	"slices"

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
	shards := shardsPool.Get()
	defer shardsPool.Put(shards)

	err := db.findShards(shards, start, end, matchers)
	if err != nil {
		return err
	}

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
