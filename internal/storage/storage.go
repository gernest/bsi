package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"

	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/gernest/bsi/internal/storage/rows"
	"github.com/gernest/bsi/internal/storage/seq"
	"github.com/gernest/bsi/internal/storage/tsid"
	"github.com/gernest/bsi/internal/storage/views"
	"github.com/gernest/roaring/shardwidth"
	"github.com/prometheus/common/promslog"
	"go.etcd.io/bbolt"
)

var tsidPool tsid.Pool

// Store implements timeseries database.
type Store struct {
	rbf *rbf.DB
	txt *bbolt.DB
	lo  *slog.Logger

	retention atomic.Int64
	deletion  atomic.Uint64
}

// Init initializes store on dataPath.
func (db *Store) Init(dataPath string, lo *slog.Logger) error {
	if lo == nil {
		lo = promslog.NewNopLogger()
	}
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
	db.lo = lo

	return nil
}

// Close implements storage.Storage.
func (db *Store) Close() error {
	return errors.Join(
		db.txt.Close(), db.rbf.Close(),
	)
}

// MinTs returns the lowest timestamp currently observed in the database.
func (db *Store) MinTs() (ts int64, err error) {
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

// MaxTs returns the highest timestamp currently observed in the database.
func (db *Store) MaxTs() (ts int64, err error) {
	err = db.txt.View(func(tx *bbolt.Tx) error {
		adminB := tx.Bucket(admin)
		_, v := adminB.Cursor().Last()
		if v != nil {
			ts = magic.ReinterpretSlice[views.Meta](v)[0].Max
		}
		return nil
	})
	return
}

// AddRows index and store rows.
func (db *Store) AddRows(rows *rows.Rows) error {

	ids := tsidPool.Get()
	defer tsidPool.Put(ids)

	hi, err := translate(db.txt, ids, rows)
	if err != nil {
		return fmt.Errorf("assigning tsid to rows %w", err)
	}

	ma := make(views.Map)

	lo := hi - uint64(rows.Size())

	for start, se := range seq.RangeShardSequence(seq.Sequence{Lo: lo, Hi: hi}) {
		shard := se.Lo / shardwidth.ShardWidth
		offset := start + int(se.Hi-se.Lo) - 1
		sx := ma.Get(shard)
		sx.AddTS(se.Lo, rows.Timestamp[start:offset], rows.Kind)
		sx.AddValues(se.Lo, rows.Value[start:offset], rows.Kind)
		sx.AddKind(se.Lo, rows.Kind[start:offset])
		sx.AddIndex(se.Lo, ids.B[start:offset], rows.Kind)
	}

	err = db.apply(ma)
	if err != nil {
		return err
	}
	err = db.saveMetadata(ma)
	if err != nil {
		return err
	}
	if db.deletion.Load() != 0 {
		// cleanup is expensive, avoid executing it in hot path.
		go db.cleanup()
	}
	return nil
}

func (db *Store) saveMetadata(ma views.Map) error {
	var key [8]byte
	return db.txt.Update(func(tx *bbolt.Tx) error {
		adminB := tx.Bucket(admin)
		cu := adminB.Cursor()
		var first views.Meta
		if _, v := cu.First(); v != nil {
			first = magic.ReinterpretSlice[views.Meta](v)[0]
		}
		shards := make([]uint64, 0, len(ma))
		for k := range ma {
			shards = append(shards, k)
		}
		slices.Sort(shards)

		retention := db.retention.Load()

		for _, shard := range shards {
			data := ma[shard]
			binary.BigEndian.PutUint64(key[:], shard)
			if k, v := cu.Seek(key[:]); v != nil && bytes.Equal(k, key[:]) {
				data.Meta.Update(&magic.ReinterpretSlice[views.Meta](v)[0])
			}

			err := adminB.Put(key[:], data.Meta.Bytes())
			if err != nil {
				return fmt.Errorf("updating shard data %w", err)
			}

			if retention != 0 && db.deletion.Load() == 0 && (data.Meta.Max-first.Max) >= retention {
				db.deletion.Store(shard)
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
