package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/pools"
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/prometheus/prometheus/model/labels"
	"go.etcd.io/bbolt"
)

var shardsPool = pools.Pool[*view]{Init: viewsItems{}}

// Search for all shards that have timestamps within the start and end range. To avoid opeting
// another database transaction, we also decode matchers for searching in our RBF storage.
func (db *Store) findShards(start, end int64, matchers []*labels.Matcher) (vs *view, err error) {
	vs = shardsPool.Get()

	err = db.txt.View(func(tx *bbolt.Tx) error {
		err := findPartitions(tx, start, end, vs)
		if err != nil {
			return err
		}
		if len(vs.meta) == 0 {
			return nil
		}
		if len(matchers) > 0 {
			searchB := tx.Bucket(search)
			cu := searchB.Cursor()
			for _, m := range matchers {
				switch m.Type {
				case labels.MatchEqual:
					b, _ := cu.Seek(magic.Slice(m.Name))
					if !bytes.Equal(b, magic.Slice(m.Name)) {
						// no bucket for label name observed yet. We will never satisfy
						// matching conditions
						vs.Reset()
						return nil
					}
					mb := searchB.Bucket(b)
					value := mb.Get(magic.Slice(m.Value))
					if value == nil {
						vs.Reset()
						return nil
					}
					va := binary.BigEndian.Uint64(value)
					vs.match = append(vs.match, match{
						column: xxhash.Sum64(b),
						rows: []row{
							{predicate: int64(va)},
						},
						op: bitmaps.EQ,
					})
				case labels.MatchNotEqual:
					b, _ := cu.Seek(magic.Slice(m.Name))
					if !bytes.Equal(b, magic.Slice(m.Name)) {
						continue
					}
					mb := searchB.Bucket(b)
					value := mb.Get(magic.Slice(m.Value))
					if value == nil {
						continue
					}
					va := binary.BigEndian.Uint64(value)
					vs.match = append(vs.match, match{
						column: xxhash.Sum64(b),
						rows: []row{
							{predicate: int64(va)},
						},
						op: bitmaps.NEQ,
					})
				case labels.MatchRegexp:
					b, _ := cu.Seek(magic.Slice(m.Name))
					if !bytes.Equal(b, magic.Slice(m.Name)) {
						// no bucket for label name observed yet. We will never satisfy
						// matching conditions
						vs.Reset()
						return nil
					}
					mb := searchB.Bucket(b)
					values := make([]row, 0, 64)

					mc := mb.Cursor()
					prefix := magic.Slice(m.Prefix())
					for a, b := mc.Seek(prefix); b != nil && bytes.HasPrefix(a, prefix) && m.Matches(magic.String(a)); a, b = mc.Next() {
						va := binary.BigEndian.Uint64(b)
						values = append(values, row{predicate: int64(va)})
					}
					if len(values) == 0 {
						vs.Reset()
						return nil
					}
					sort.Slice(values, func(i, j int) bool {
						return values[i].predicate < values[j].predicate
					})
					vs.match = append(vs.match, match{
						column: xxhash.Sum64(b),
						rows:   values,
						op:     bitmaps.EQ,
					})

				case labels.MatchNotRegexp:
					b, _ := cu.Seek(magic.Slice(m.Name))
					if !bytes.Equal(b, magic.Slice(m.Name)) {
						continue
					}
					mb := searchB.Bucket(b)
					values := make([]row, 0, 64)

					mc := mb.Cursor()
					prefix := magic.Slice(m.Prefix())
					for a, b := mc.Seek(prefix); b != nil && bytes.HasPrefix(a, prefix) && !m.Matches(magic.String(a)); a, b = mc.Next() {
						va := binary.BigEndian.Uint64(b)
						values = append(values, row{predicate: int64(va)})
					}
					if len(values) == 0 {
						continue
					}
					sort.Slice(values, func(i, j int) bool {
						return values[i].predicate < values[j].predicate
					})
					vs.match = append(vs.match, match{
						column: xxhash.Sum64(b),
						rows:   values,
						op:     bitmaps.EQ,
					})
				}

			}
		}
		return nil
	})
	if err != nil {
		shardsPool.Put(vs)
	}
	return
}

func (db *Store) findShardsAmy(start, end int64, matchers [][]*labels.Matcher) (vs *view, err error) {
	vs = shardsPool.Get()

	err = db.txt.View(func(tx *bbolt.Tx) error {
		err := findPartitions(tx, start, end, vs)
		if err != nil {
			return err
		}
		if len(vs.meta) == 0 {
			return nil
		}
		if len(matchers) > 0 {
			searchB := tx.Bucket(search)
			cu := searchB.Cursor()
		top:
			for i := range matchers {
				ls := make([]match, 0, len(matchers[i]))
			local:
				for _, m := range matchers[i] {
					switch m.Type {
					case labels.MatchEqual:
						b, _ := cu.Seek(magic.Slice(m.Name))
						if !bytes.Equal(b, magic.Slice(m.Name)) {
							// no bucket for label name observed yet. We will never satisfy
							// matching conditions
							vs.Reset()
							continue top
						}
						mb := searchB.Bucket(b)
						value := mb.Get(magic.Slice(m.Value))
						if value == nil {
							continue top
						}
						va := binary.BigEndian.Uint64(value)
						ls = append(ls, match{
							column: xxhash.Sum64(b),
							rows: []row{
								{predicate: int64(va)},
							},
							op: bitmaps.EQ,
						})
					case labels.MatchNotEqual:
						b, _ := cu.Seek(magic.Slice(m.Name))
						if !bytes.Equal(b, magic.Slice(m.Name)) {
							continue local
						}
						mb := searchB.Bucket(b)
						value := mb.Get(magic.Slice(m.Value))
						if value == nil {
							continue local
						}
						va := binary.BigEndian.Uint64(value)
						ls = append(ls, match{
							column: xxhash.Sum64(b),
							rows: []row{
								{predicate: int64(va)},
							},
							op: bitmaps.NEQ,
						})
					case labels.MatchRegexp:
						b, _ := cu.Seek(magic.Slice(m.Name))
						if !bytes.Equal(b, magic.Slice(m.Name)) {
							// no bucket for label name observed yet. We will never satisfy
							// matching conditions
							vs.Reset()
							continue top
						}
						mb := searchB.Bucket(b)
						values := make([]row, 0, 64)

						mc := mb.Cursor()
						prefix := magic.Slice(m.Prefix())
						for a, b := mc.Seek(prefix); b != nil && bytes.HasPrefix(a, prefix) && m.Matches(magic.String(a)); a, b = mc.Next() {
							va := binary.BigEndian.Uint64(b)
							values = append(values, row{predicate: int64(va)})
						}
						if len(values) == 0 {
							continue top
						}
						sort.Slice(values, func(i, j int) bool {
							return values[i].predicate < values[j].predicate
						})
						ls = append(ls, match{
							column: xxhash.Sum64(b),
							rows:   values,
							op:     bitmaps.EQ,
						})

					case labels.MatchNotRegexp:
						b, _ := cu.Seek(magic.Slice(m.Name))
						if !bytes.Equal(b, magic.Slice(m.Name)) {
							continue
						}
						mb := searchB.Bucket(b)
						values := make([]row, 0, 64)

						mc := mb.Cursor()
						prefix := magic.Slice(m.Prefix())
						for a, b := mc.Seek(prefix); b != nil && bytes.HasPrefix(a, prefix) && !m.Matches(magic.String(a)); a, b = mc.Next() {
							va := binary.BigEndian.Uint64(b)
							values = append(values, row{predicate: int64(va)})
						}
						if len(values) == 0 {
							continue top
						}
						sort.Slice(values, func(i, j int) bool {
							return values[i].predicate < values[j].predicate
						})
						ls = append(ls, match{
							column: xxhash.Sum64(b),
							rows:   values,
							op:     bitmaps.EQ,
						})
					}

				}
				if len(ls) > 0 {
					vs.matchAny = append(vs.matchAny, ls)
				}
			}
		}
		return nil
	})
	if err != nil {
		shardsPool.Put(vs)
	}
	return
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
		var all []meta
		cu := adminB.Bucket(name).Cursor()

		for _, v := cu.First(); v != nil; _, v = cu.Next() {
			o := magic.ReinterpretSlice[meta](v)
			if o[0].InRange(start, end) {
				all = append(all, o...)
			}
		}
		if len(all) > 0 {
			vs.partition = append(vs.partition, key)
			vs.meta = append(vs.meta, all)
		}

	}
	return nil

}
