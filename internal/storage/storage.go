package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/gernest/bsi/internal/pools"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/gernest/bsi/internal/storage/seq"
	"github.com/gernest/bsi/internal/storage/tsid"
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

	ma := make(data)

	lo := hi - uint64(rows.Size())

	for start, se := range seq.RangeShardSequence(seq.Sequence{Lo: lo, Hi: hi}) {
		offset := start + int(se.Hi-se.Lo)
		for i := range offset - start {
			if rows.Kind[i] == None {
				continue
			}
			idx := start + i
			id := se.Lo + uint64(idx)
			yy, mm, _ := time.UnixMilli(rows.Timestamp[idx]).Date()
			ma.Timestamp(yy, mm, id, rows.Timestamp[idx])
			ma.Value(yy, mm, id, rows.Value[idx])
			ma.Kind(yy, mm, id, rows.Kind[idx])
			ma.Index(yy, mm, id, ids.B[idx])
		}
	}

	err = db.apply(ma)
	if err != nil {
		return err
	}
	if db.deletion.Load() != 0 {
		// cleanup is expensive, avoid executing it in hot path.
		go db.cleanup()
	}
	return nil
}

func (db *Store) apply(ma data) error {
	tx, err := db.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for co, ra := range ma {
		ra.Optimize()
		_, err := tx.AddRoaring(co, ra)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
