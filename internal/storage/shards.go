package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"slices"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/pools"
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/prometheus/prometheus/model/labels"
	"go.etcd.io/bbolt"
)

var shardsPool = pools.Pool[*view]{Init: viewsItems{}}

// Search for all shards that have timestamps within the start and end range. To avoid opeting
// another database transaction, we also decode matchers for searching in our RBF storage.
func (db *Store) findShards(vs *view, start, end int64, matchers []*labels.Matcher) (err error) {
	return db.txt.View(func(tx *bbolt.Tx) error {
		err := findPartitions(tx, start, end, vs)
		if err != nil {
			return err
		}
		if len(vs.meta) == 0 {
			return nil
		}
		vs.match = slices.Collect(find(tx, matchers))
		return nil
	})

}

func (db *Store) findShardsAmy(vs *view, start, end int64, matchers [][]*labels.Matcher) (err error) {
	vs = shardsPool.Get()

	return db.txt.View(func(tx *bbolt.Tx) error {
		err := findPartitions(tx, start, end, vs)
		if err != nil {
			return err
		}
		if len(vs.meta) == 0 {
			return nil
		}
		for i := range matchers {
			vs.matchAny = append(vs.matchAny, slices.Collect(find(tx, matchers[i])))
		}
		return nil
	})
}

func find(tx *bbolt.Tx, matchers []*labels.Matcher) iter.Seq[match] {
	return func(yield func(match) bool) {
		if len(matchers) == 0 {
			return
		}
		searchB := tx.Bucket(search)
		cu := searchB.Cursor()
		for _, m := range matchers {
			switch m.Type {
			case labels.MatchEqual, labels.MatchNotEqual:
				b, _ := cu.Seek(magic.Slice(m.Name))
				var va uint64
				op := bitmaps.EQ
				if m.Type == labels.MatchNotEqual {
					op = bitmaps.NEQ
				}
				if bytes.Equal(b, magic.Slice(m.Name)) {
					mb := searchB.Bucket(b)
					value := mb.Get(magic.Slice(m.Value))
					if value != nil {
						va = binary.BigEndian.Uint64(value)
					}
				}
				ma := match{
					column: m.Name,
					rows:   []uint64{va},
					op:     op,
				}
				if !yield(ma) {
					return
				}
			case labels.MatchRegexp, labels.MatchNotRegexp:
				b, _ := cu.Seek(magic.Slice(m.Name))
				values := make([]uint64, 0, 64)
				op := bitmaps.EQ
				if bytes.Equal(b, magic.Slice(m.Name)) {
					mb := searchB.Bucket(b)
					mc := mb.Cursor()
					for a, b := mc.First(); b != nil; a, b = mc.Next() {
						if m.Matches(magic.String(a)) {
							va := binary.BigEndian.Uint64(b)
							values = append(values, va)
						}
					}
				}

				ma := match{
					column: m.Name,
					rows:   values,
					op:     op,
				}
				if !yield(ma) {
					return
				}
			}

		}
	}
}

func findPartitions(tx *bbolt.Tx, start, end int64, vs *view) error {
	adminB := tx.Bucket(admin)
	acu := adminB.Cursor()
	lo := []byte(partitionKey(start).String())
	hi := []byte(partitionKey(end).String())

	// Top level buckets are for partitions
	for name, value := acu.Seek(lo); name != nil && value == nil && bytes.Compare(name, hi) < 1; name, value = acu.Next() {

		key, err := parsePartitionKey(magic.String(name))
		if err != nil {
			return fmt.Errorf("parsing partition %w", err)
		}
		cu := adminB.Bucket(name).Cursor()

		for k, v := cu.First(); v != nil; k, v = cu.Next() {

			o := magic.ReinterpretSlice[bounds](v)
			if o[0].minMax.InRange(start, end) {
				vs.meta = append(vs.meta, meta{
					depth: slices.Clone(o),
					shard: binary.BigEndian.Uint64(k),
					year:  uint16(key.year),
					month: uint8(key.month),
				})
			}
		}
	}
	return nil
}
