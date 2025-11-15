package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"sync/atomic"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/seq"
	"github.com/gernest/roaring/shardwidth"
	"github.com/prometheus/common/promslog"
	"go.etcd.io/bbolt"
)

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
	ts, _, err = db.MinMax()
	return
}

// MaxTs returns the highest timestamp currently observed in the database.
func (db *Store) MaxTs() (ts int64, err error) {
	_, ts, err = db.MinMax()
	return
}

func (db *Store) MinMax() (lo, hi int64, err error) {
	tx, err := db.db.Begin(false)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return 0, 0, err
	}
	it := records.Iterator()
	{
		// first timestamp column
		it.Seek(rbf.Key{Column: MetricsTimestamp})
		name, page, ok := it.Next()
		if !ok || name.Column != MetricsTimestamp {
			return 0, 0, nil
		}

		cu := tx.CursorFromRoot(page)
		defer cu.Close()
		vx, _ := cu.Max()
		depth := vx / shardwidth.ShardWidth
		lo, _, err = bitmaps.Min(cu, nil, name.Shard, depth)
		if err != nil {
			return
		}
	}
	{
		// first timestamp column
		it.Seek(rbf.Key{Column: MetricsTimestamp, Shard: math.MaxUint64})
		name, page, ok := it.Next()
		if !ok || name.Column != MetricsTimestamp {
			name, page, ok = it.Prev()
			if !ok || name.Column != MetricsTimestamp {
				return 0, 0, nil
			}
		}

		cu := tx.CursorFromRoot(page)
		defer cu.Close()
		vx, _ := cu.Max()
		depth := vx / shardwidth.ShardWidth
		hi, _, err = bitmaps.Max(cu, nil, name.Shard, depth)
	}
	return
}

// AddRows index and store rows.
func (db *Store) AddRows(rows *Rows) error {

	hi, err := translate(db.txt, rows)
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
			ma.Timestamp(id, rows.Timestamp[idx])
			ma.Value(id, rows.Value[idx])
			ma.Kind(id, rows.Kind[idx])
			ma.Index(id, rows.ID[idx])
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
