package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"github.com/prometheus/prometheus/model/labels"
	"go.etcd.io/bbolt"
)

// Delete clears all samples in the time range. If matchers is provided we will also match
// just lke what Select does.
//
// The following data stays in storage forever,
// - all symbols:  labels.Labels and their translation index
// - indexed columns: These columns are very small (usually in kb) and maps to
// metrics ids which we remain forever in our translation store.
//
// Like LMDB , this operation does not result in database shrinking. Deleted pages are
// reused. Creating snapshot after this operation will result in smaller snapshot
// size.
func (db *Store) Delete(start, end int64, matchers ...*labels.Matcher) error {
	var shards view
	err := db.findShards(&shards, matchers)
	if err != nil {
		return err
	}

	// select all columns to be deleted in read transactions.
	deletion := map[rbf.Key]*roaring.Bitmap{}
	translated := map[Kind]*roaring.Bitmap{}

	err = db.read(&shards, start, end, all, func(tx *rbf.Tx, records *rbf.Records, filter *roaring.Bitmap, shard uint64, kinds Kinds) error {
		clone := filter.Clone()
		deletion[rbf.Key{Column: MetricsTimestamp, Shard: shard}] = clone
		deletion[rbf.Key{Column: MetricsType, Shard: shard}] = clone
		deletion[rbf.Key{Column: MetricsLabels, Shard: shard}] = clone
		deletion[rbf.Key{Column: MetricsValue, Shard: shard}] = clone
		root, _ := records.Get(rbf.Key{Column: MetricsLabels, Shard: shard})

		cu := tx.CursorFromRoot(root)
		defer cu.Close()

		mx, err := cu.Max()
		if err != nil {
			return err
		}
		depth := mx / shardwidth.ShardWidth

		for i := range kinds {
			if !kinds[i].Any() {
				continue
			}
			kind := Kind(i + 1)
			switch kind {
			case Histogram, FloatHistogram, Exemplar:
				ra, ok := translated[kind]
				if !ok {
					ra = roaring.NewBitmap()
					translated[kind] = ra
				}
				err := bitmaps.Transpose(cu, uint8(depth), shard, kinds[i], func(value int64) {
					ra.DirectAdd(uint64(value))
				})
				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	tx, err := db.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return err
	}
	for key, ra := range deletion {
		if ra.Count() == shardwidth.ShardWidth {
			// fast path: we are deleting full shards, drop the whole bitmap.
			err := tx.DeleteBitmap(key)
			if err != nil {
				return err
			}
		}
		// slow path: rewrite the bitmap
		root, ok := records.Get(key)
		if !ok {
			continue
		}
		err = clearRecords(tx, root, ra, nil)

	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	if len(translated) == 0 {
		return nil
	}

	return db.txt.Update(func(tx *bbolt.Tx) error {
		for k, ra := range translated {
			var bu []byte
			switch k {
			case Histogram, FloatHistogram:
				bu = histogramData
			case Exemplar:
				bu = exemplarData
			default:
				return fmt.Errorf("unknown translated  metric kind %d", k)
			}
			err := deleteFromU64(tx.Bucket(bu), ra)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func deleteFromU64(b *bbolt.Bucket, all *roaring.Bitmap) error {
	cu := b.Cursor()
	var lo, hi [8]byte
	for a, b := range rangeSetsRa(all) {
		binary.BigEndian.PutUint64(lo[:], a)
		binary.BigEndian.PutUint64(hi[:], b)

		for k, v := cu.Seek(lo[:]); v != nil && bytes.Compare(k, hi[:]) < 1; k, v = cu.Next() {
			err := cu.Delete()
			if err != nil {
				return err
			}

		}
	}
	return nil
}

func clearRecords(tx *rbf.Tx, root uint32, columns *roaring.Bitmap, rowSet func(uint64)) error {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()
	if rowSet == nil {
		rowSet = func(_ uint64) {}
	}
	rewriteExisting := roaring.NewBitmapBitmapTrimmer(columns, func(key roaring.FilterKey, data *roaring.Container, filter *roaring.Container, writeback roaring.ContainerWriteback) error {
		if filter.N() == 0 {
			return nil
		}
		existing := data.N()
		// nothing to delete. this can't happen normally, but the rewriter calls
		// us with an empty data container when it's done.
		if existing == 0 {
			return nil
		}
		data = data.DifferenceInPlace(filter)
		if data.N() != existing {
			rowSet(key.Row())
			return writeback(key, data)
		}
		return nil
	})

	return cu.ApplyRewriter(0, rewriteExisting)
}

type translatedSHard struct {
	shard uint64
	data  *roaring.Bitmap
}
