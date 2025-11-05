package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/gernest/bsi/internal/storage/seq"
	"github.com/gernest/bsi/internal/storage/single"
	"github.com/gernest/bsi/internal/storage/tsid"
	"github.com/gernest/roaring/shardwidth"
	"github.com/prometheus/common/promslog"
	"go.etcd.io/bbolt"
)

var tsidPool tsid.Pool

// Store implements timeseries database.
type Store struct {
	rbf single.Group[shardYM, *rbf.DB, string]
	txt *bbolt.DB
	lo  *slog.Logger

	dataPath  string
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

	db.rbf.Init(lo, func(ym shardYM, s string) (*rbf.DB, error) {
		path := filepath.Join(s, ym.ym.String(), fmt.Sprintf("%06d", ym.shard))
		da := rbf.NewDB(path, nil)
		err := da.Open()
		if err != nil {
			return nil, fmt.Errorf("opening rbf database %w", err)
		}
		return da, nil
	})
	db.dataPath = dataPath
	db.txt = tdb
	db.lo = lo
	return nil
}

func (db *Store) SetRetention(retention int64) {
	db.retention.Store(retention)
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
			ts = magic.ReinterpretSlice[meta](v)[0].min
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
			ts = magic.ReinterpretSlice[meta](v)[0].max
		}
		return nil
	})
	return
}

// AddRows index and store rows.
func (db *Store) AddRows(rows *Rows) error {

	ids := tsidPool.Get()
	defer tsidPool.Put(ids)

	hi, err := translate(db.txt, ids, rows)
	if err != nil {
		return fmt.Errorf("assigning tsid to rows %w", err)
	}

	ma := make(partitions)

	lo := hi - uint64(rows.Size())

	for start, se := range seq.RangeShardSequence(seq.Sequence{Lo: lo, Hi: hi}) {
		shard := se.Lo / shardwidth.ShardWidth
		offset := start + int(se.Hi-se.Lo) - 1

		for i := range offset - start {
			if rows.Kind[i] == None {
				continue
			}
			idx := start + i
			id := se.Lo + uint64(idx)
			ba := ma.get(shard, rows.Timestamp[idx])
			ba.Timestamp(id, rows.Timestamp[idx])
			ba.Value(id, rows.Value[idx])
			ba.Kind(id, rows.Kind[idx])
			ba.Index(id, ids.B[idx])
		}

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

func (db *Store) saveMetadata(ma partitions) error {
	var key [8]byte
	return db.txt.Update(func(tx *bbolt.Tx) error {
		adminB := tx.Bucket(admin)
		for k, v := range ma {
			pa, err := adminB.CreateBucketIfNotExists([]byte(k.String()))
			if err != nil {
				return fmt.Errorf("getting partition=%v %w", k, err)
			}
			cu := pa.Cursor()
			for shard, data := range v {
				binary.BigEndian.PutUint64(key[:], shard)
				if k, v := cu.Seek(key[:]); v != nil && bytes.Equal(k, key[:]) {
					data.meta.Update(&magic.ReinterpretSlice[meta](v)[0])
				}
				err := pa.Put(key[:], magic.ReinterpretSlice[byte]([]meta{data.meta}))
				if err != nil {
					return fmt.Errorf("updating shard=%d view=%s data %w", shard, k, err)
				}
			}
		}
		return nil
	})
}

func (db *Store) apply(ma partitions) error {
	for k, v := range ma {
		err := db.applyPartition(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Store) applyPartition(ym yyyyMM, ma batch) error {
	for shard, v := range ma {
		err := db.partition(ym, shard, true, func(tx *rbf.Tx) error {
			for col, ra := range v.columns {
				ra.Optimize()
				_, err := tx.AddRoaring(rbf.Key{Column: col, Shard: shard}, ra)
				if err != nil {
					return fmt.Errorf("writing bitmap %w", err)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}

	}
	return nil
}

func (db *Store) partition(key yyyyMM, shard uint64, writable bool, cb func(tx *rbf.Tx) error) error {
	da, done, err := db.rbf.Do(shardYM{shard: shard, ym: key}, db.dataPath)
	if err != nil {
		return err
	}
	defer done.Close()

	tx, err := da.Begin(writable)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return cb(tx)
}
