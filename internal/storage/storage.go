package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync/atomic"

	"github.com/gernest/bsi/internal/pools"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/gernest/bsi/internal/storage/seq"
	"github.com/gernest/bsi/internal/storage/tsid"
	"github.com/gernest/roaring/shardwidth"
	"github.com/prometheus/common/promslog"
	"go.etcd.io/bbolt"
)

var tsidPool = pools.Pool[*tsid.B]{Init: tsidItems{}}

type tsidItems struct{}

var _ pools.Items[*tsid.B] = (*tsidItems)(nil)

func (tsidItems) Init() *tsid.B {
	return &tsid.B{B: make([]tsid.ID, 0, 1<<10)}
}

func (tsidItems) Reset(v *tsid.B) *tsid.B {
	v.B = v.B[:0]
	return v
}

// Store implements timeseries database.
type Store struct {
	txt *bbolt.DB
	lo  *slog.Logger
	db  *rbf.DB

	retention atomic.Int64
	deletion  atomic.Uint64
}

// Init initializes store on dataPath.
func (db *Store) Init(dataPath string, lo *slog.Logger) error {
	if lo == nil {
		lo = promslog.NewNopLogger()
	}
	db.db = rbf.NewDB(dataPath, nil)
	err := db.db.Open()
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
		_, err = tx.CreateBucket(snapshots)
		if err != nil {
			return fmt.Errorf("creating snapshots bucket %w", err)
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

func (db *Store) SetRetention(retention int64) {
	db.retention.Store(retention)
}

// Close implements storage.Storage.
func (db *Store) Close() error {
	return errors.Join(
		db.txt.Close(), db.db.Close(),
	)
}

// MinTs returns the lowest timestamp currently observed in the database.
func (db *Store) MinTs() (ts int64, err error) {
	err = db.txt.View(func(tx *bbolt.Tx) error {
		ts = minimumTs(tx)
		return nil
	})
	return
}

func minimumTs(tx *bbolt.Tx) int64 {
	adminB := tx.Bucket(admin)
	k, _ := adminB.Cursor().First()
	if k != nil {
		_, v := adminB.Bucket(k).Cursor().First()
		if v != nil {
			return magic.ReinterpretSlice[bounds](v)[0].minMax.min
		}
	}
	return 0
}

// MaxTs returns the highest timestamp currently observed in the database.
func (db *Store) MaxTs() (ts int64, err error) {
	err = db.txt.View(func(tx *bbolt.Tx) error {
		ts = maximumTs(tx)
		return nil
	})
	return
}

func maximumTs(tx *bbolt.Tx) int64 {
	adminB := tx.Bucket(admin)
	k, _ := adminB.Cursor().Last()
	if k != nil {
		_, v := adminB.Bucket(k).Cursor().Last()
		if v != nil {
			return magic.ReinterpretSlice[bounds](v)[0].minMax.max
		}
	}
	return 0
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
		offset := start + int(se.Hi-se.Lo)
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
					updateBounds(data.meta, v)
				}
				out := buildBounds(data.meta)
				err := pa.Put(key[:], magic.ReinterpretSlice[byte](out))
				if err != nil {
					return fmt.Errorf("updating shard=%d view=%s data %w", shard, k, err)
				}
			}
		}
		return nil
	})
}

func (db *Store) apply(ma partitions) error {
	tx, err := db.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for pa, ba := range ma {
		for sha, da := range ba {
			for co, ra := range da.columns {
				ra.Optimize()
				_, err := tx.AddRoaring(rbf.Key{Column: co, Shard: sha, Year: uint16(pa.year), Month: uint8(pa.month)}, ra)
				if err != nil {
					return err
				}
			}
		}
	}
	return tx.Commit()
}
