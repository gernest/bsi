package storage

import (
	"context"
	"errors"
	"io"
	"math"

	"github.com/gernest/u128/internal/storage/magic"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"go.etcd.io/bbolt"
)

// LabelValues implements storage.LabelQuerier.
func (db *Store) LabelValues(_ context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	var values []string
	err := db.txt.View(func(tx *bbolt.Tx) error {
		searchB := tx.Bucket(search)

		b := searchB.Bucket(magic.Slice(name))
		if b != nil {
			limit := math.MaxInt
			if hints != nil {
				limit = hints.Limit
			}
			return b.ForEach(func(k, _ []byte) error {
				text := magic.String(k)
				for _, m := range matchers {
					if !m.Matches(text) {
						return nil
					}
				}
				values = append(values, string(k))
				limit--
				if limit > 0 {
					return nil
				}
				return io.EOF
			})
		}
		return nil
	})
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, nil, err
		}
	}
	return values, nil, nil
}

// LabelNames implements storage.LabelQuerier
func (db *Store) LabelNames(_ context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	var names []string
	err := db.txt.View(func(tx *bbolt.Tx) error {
		searchB := tx.Bucket(search)
		limit := math.MaxInt
		if hints != nil {
			limit = hints.Limit
		}

		if len(matchers) == 0 {
			// fast path: iterate over buckets only
			cu := searchB.Cursor()
			for k, v := cu.First(); k != nil && v == nil; k, v = cu.Next() {
				names = append(names, string(k))
				limit--
				if limit > 0 {
					continue
				}
				return io.EOF
			}
			return nil
		}

		for _, m := range matchers {
			b := searchB.Bucket(magic.Slice(m.Name))
			if b == nil {
				continue
			}
			switch m.Type {
			case labels.MatchEqual:
				if b.Get(magic.Slice(m.Value)) == nil {
					continue
				}
			case labels.MatchNotEqual:
				cu := b.Cursor()
				for k, _ := cu.First(); k != nil; k, _ = cu.Next() {
					if m.Matches(magic.String(k)) {
						continue
					}
					break
				}
			case labels.MatchRegexp:
				cu := b.Cursor()
				for k, _ := cu.First(); k != nil; k, _ = cu.Next() {
					if !m.Matches(magic.String(k)) {
						continue
					}
					break
				}
			case labels.MatchNotRegexp:
				cu := b.Cursor()
				for k, _ := cu.First(); k != nil; k, _ = cu.Next() {
					if m.Matches(magic.String(k)) {
						continue
					}
					break
				}
			}
			names = append(names, m.Name)
			limit--
			if limit > 0 {
				continue
			}
			return io.EOF
		}
		return nil
	})
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, nil, err
		}
	}
	return names, nil, nil
}
