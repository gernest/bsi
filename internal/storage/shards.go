package storage

import (
	"bytes"
	"encoding/binary"
	"iter"
	"slices"

	"github.com/gernest/bsi/internal/pools"
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/gernest/roaring"
	"github.com/prometheus/prometheus/model/labels"
	"go.etcd.io/bbolt"
)

var shardsPool = pools.Pool[*view]{Init: viewsItems{}}

// Search for all shards that have timestamps within the start and end range. To avoid opeting
// another database transaction, we also decode matchers for searching in our RBF storage.
func (db *Store) findShards(vs *view, matchers []*labels.Matcher) (err error) {
	return db.txt.View(func(tx *bbolt.Tx) error {
		vs.match = slices.Collect(find(tx, matchers))
		vs.Clamp()
		return nil
	})
}

func (db *Store) findShardsAmy(vs *view, matchers [][]*labels.Matcher) (err error) {
	return db.txt.View(func(tx *bbolt.Tx) error {
		for i := range matchers {
			vs.matchAny = append(vs.matchAny, slices.Collect(find(tx, matchers[i])))
		}
		vs.Clamp()
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
			case labels.MatchEqual:
				b, _ := cu.Seek(magic.Slice(m.Name))
				ra := roaring.NewBitmap()
				ma := match{
					column: m.Name,
					rows:   ra,
				}
				if bytes.Equal(b, magic.Slice(m.Name)) {
					mb := searchB.Bucket(b)
					value := mb.Get(magic.Slice(m.Value))
					if value != nil {
						ra.DirectAdd(binary.BigEndian.Uint64(value))
					}
				} else if m.Value == "" {
					// the label name is missing and we have label_name="" matcher.
					ma = match{exists: true}
				}

				if !yield(ma) {
					return
				}
			case labels.MatchRegexp:
				b, _ := cu.Seek(magic.Slice(m.Name))
				ra := roaring.NewBitmap()
				if bytes.Equal(b, magic.Slice(m.Name)) {
					mb := searchB.Bucket(b)
					mc := mb.Cursor()
					prefix := magic.Slice(m.Prefix())
					for a, b := mc.Seek(prefix); b != nil && bytes.HasPrefix(a, prefix); a, b = mc.Next() {
						if m.Matches(magic.String(a)) {
							va := binary.BigEndian.Uint64(b)
							ra.DirectAdd(va)
						}
					}
				}
				ma := match{
					column: m.Name,
					rows:   ra,
				}
				if !yield(ma) {
					return
				}

			case labels.MatchNotEqual, labels.MatchNotRegexp:
				b, _ := cu.Seek(magic.Slice(m.Name))
				ra := roaring.NewBitmap()
				if !bytes.Equal(b, magic.Slice(m.Name)) || m.Value == "" {
					// label name does not exists. Select all valid samples
					ma := match{
						exists: true,
						rows:   ra,
					}
					if !yield(ma) {
						return
					}
					continue
				}

				mb := searchB.Bucket(b)
				mc := mb.Cursor()
				for a, b := mc.First(); b != nil; a, b = mc.Next() {
					if m.Matches(magic.String(a)) {
						va := binary.BigEndian.Uint64(b)
						ra.DirectAdd(va)
					}
				}
				ma := match{
					column: m.Name,
					rows:   ra,
				}
				if !yield(ma) {
					return
				}

			}

		}
	}
}
