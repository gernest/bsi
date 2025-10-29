package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gernest/roaring/shardwidth"
	"github.com/gernest/u128/internal/rbf"
	"github.com/gernest/u128/internal/storage/keys"
	"github.com/gernest/u128/internal/storage/magic"
	"github.com/gernest/u128/internal/storage/rows"
	"github.com/gernest/u128/internal/storage/seq"
	"github.com/gernest/u128/internal/storage/tsid"
	"github.com/gernest/u128/internal/storage/views"
	"go.etcd.io/bbolt"
)

var tsidPool tsid.Pool

// Store implements timeseries database.
type Store struct {
	rbf *rbf.DB
	txt *bbolt.DB
}

// Init initializes store on dataPath.
func (db *Store) Init(dataPath string) error {
	err := os.MkdirAll(dataPath, 0755)
	if err != nil {
		return fmt.Errorf("setup data path %w", err)
	}
	db.rbf = rbf.NewDB(dataPath, nil)
	err = db.rbf.Open()
	if err != nil {
		return err
	}
	tdb, err := bbolt.Open(filepath.Join(dataPath, "txt"), 0600, nil)
	if err != nil {
		return err
	}

	tdb.Update(func(tx *bbolt.Tx) error {
		_, err = tx.CreateBucket(admin)
		if err != nil {
			return fmt.Errorf("creating sum bucket %w", err)
		}
		_, err = tx.CreateBucket(metricsSum)
		if err != nil {
			return fmt.Errorf("creating sum bucket %w", err)
		}
		_, err = tx.CreateBucket(metricsData)
		if err != nil {
			return fmt.Errorf("creating data bucket %w", err)
		}
		_, err = tx.CreateBucket(histogramData)
		if err != nil {
			return fmt.Errorf("creating histogram bucket %w", err)
		}
		_, err = tx.CreateBucket(exemplarData)
		if err != nil {
			return fmt.Errorf("creating exemplar bucket %w", err)
		}
		_, err = tx.CreateBucket(metaData)
		if err != nil {
			return fmt.Errorf("creating metadata bucket %w", err)
		}
		_, err = tx.CreateBucket(search)
		if err != nil {
			return fmt.Errorf("creating search bucket %w", err)
		}
		return nil
	})

	db.txt = tdb

	return nil
}

// Close implements storage.Storage.
func (db *Store) Close() error {
	return errors.Join(
		db.txt.Close(), db.rbf.Close(),
	)
}

// StartTime implements storage.Storage.
func (db *Store) StartTime() (ts int64, err error) {
	err = db.txt.View(func(tx *bbolt.Tx) error {
		adminB := tx.Bucket(admin)
		_, v := adminB.Cursor().First()
		if v != nil {
			ts = magic.ReinterpretSlice[views.Meta](v)[0].Min
		}
		return nil
	})
	return
}

// AddRows index and store rows.
func (db *Store) AddRows(rows *rows.Rows) error {

	ids := tsidPool.Get()
	defer tsidPool.Put(ids)

	hi, err := assignTSID(db.txt, ids, rows.Labels)
	if err != nil {
		return fmt.Errorf("assigning tsid to rows %w", err)
	}

	seen := map[keys.Kind]bool{
		keys.Float: true,
	}

	for i := range rows.Kind {
		if seen[rows.Kind[i]] {
			continue
		}
		switch rows.Kind[i] {
		case keys.Histogram:
			err = assignU64ToHistograms(db.txt, rows.Value, rows.Histogram)
			if err != nil {
				return fmt.Errorf("translating histograms %w", err)
			}
		case keys.Exemplar:
			err = assignU64ToExemplars(db.txt, rows.Value, rows.Exemplar)
			if err != nil {
				return fmt.Errorf("translating exemplars %w", err)
			}
		case keys.Metadata:
			err = assignU64ToMetadata(db.txt, rows.Value, rows.Metadata)
			if err != nil {
				return fmt.Errorf("translating metadata %w", err)
			}
		}
		seen[rows.Kind[i]] = true
	}

	ma := make(views.Map)

	lo := hi - uint64(len(rows.Timestamp))

	for start, se := range seq.RangeShardSequence(seq.Sequence{Lo: lo, Hi: hi}) {
		shard := se.Lo / shardwidth.ShardWidth
		offset := start + int(se.Hi-se.Lo) - 1
		sx := ma.Get(shard)
		sx.AddTS(se.Lo, rows.Timestamp[start:offset])
		sx.AddValues(se.Lo, rows.Value[start:offset])
		sx.AddKind(se.Lo, rows.Kind[start:offset])
		sx.AddIndex(se.Lo, ids.B[start:offset])
	}

	err = db.apply(ma)
	if err != nil {
		return err
	}
	return db.saveMetadata(ma)
}

func (db *Store) saveMetadata(ma views.Map) error {
	var key [8]byte
	return db.txt.Update(func(tx *bbolt.Tx) error {
		adminB := tx.Bucket(admin)

		for shard, data := range ma {
			binary.BigEndian.PutUint64(key[:], shard)
			if v := adminB.Get(key[:]); v != nil {
				data.Meta.Update(&magic.ReinterpretSlice[views.Meta](v)[0])
			}
			err := adminB.Put(key[:], data.Meta.Bytes())
			if err != nil {
				return fmt.Errorf("updating shard data %w", err)
			}
		}
		return nil
	})
}

func (db *Store) apply(ma views.Map) error {

	tx, err := db.rbf.Begin(true)
	if err != nil {
		return fmt.Errorf("creating write transaction %w", err)
	}
	defer tx.Rollback()
	for shard, v := range ma {

		for col, ra := range v.Columns {
			ra.Optimize()
			_, err = tx.AddRoaring(rbf.Key{Column: col, Shard: shard}, ra)
			if err != nil {
				return fmt.Errorf("writing bitmap %w", err)
			}
		}
	}
	return tx.Commit()
}
