package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/gernest/bsi/internal/bitmaps"
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/keys"
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/gernest/bsi/internal/storage/views"
	"go.etcd.io/bbolt"
)

func (db *Store) cleanup() {
	err := db.applyRetentionPolicy()
	if err != nil {
		db.lo.Error("failed applying retention policy", "err", err)
	}
}

func (db *Store) applyRetentionPolicy() error {
	shards := db.deletion.Swap(0)
	if shards == 0 {
		return nil
	}

	// first delete metadata, this ensures that we will never touch the index with
	// deleted shards.
	var meta []views.Meta
	var shard []uint64
	err := db.txt.Update(func(tx *bbolt.Tx) error {
		adminB := tx.Bucket(admin)
		var limit [8]byte
		binary.BigEndian.PutUint64(limit[:], shards)

		cu := adminB.Cursor()
		for k, v := cu.First(); v != nil && bytes.Compare(k, limit[:]) < 0; k, v = cu.Next() {
			shard = append(shard, binary.BigEndian.Uint64(k))
			meta = append(meta, magic.ReinterpretSlice[views.Meta](v)...)
			err := cu.Delete()
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("deleting metadata %w", err)
	}
	if len(shard) == 0 {
		return nil
	}

	// we retain series metadata forever. Here we only delete samples from the
	// rbf database.
	//
	// Histogram and exemplars can safely be cleared from txt database because we
	// always assign new uint64 for each value.
	//
	// To avoid performance tanking we avoid holding the write transaction for a long
	// time by reading first and slowly dropping shards.

	var (
		hs           = -1
		ex           = -1
		maxHistogram uint64
		maxExemplar  uint64
	)
	hsDepth := bits.Len64(uint64(keys.Histogram)) + 1
	exeDepth := bits.Len64(uint64(keys.Exemplar)) + 1

	for i := range meta {
		if meta[i].KindDepth >= uint8(exeDepth) {
			ex = i
		}
		if meta[i].KindDepth >= uint8(hsDepth) {
			hs = i
		}
	}

	if hs != -1 {
		maxHistogram, err = db.findMaxHistogram(shard[hs], meta[hs].KindDepth)
		if err != nil {
			return fmt.Errorf("searching maximum histogram value %w", err)
		}
	}
	if ex != -1 {
		maxExemplar, err = db.findMaxExemplar(shard[ex], meta[ex].KindDepth)
		if err != nil {
			return fmt.Errorf("searching maximum exemplar value %w", err)
		}
	}
	if maxHistogram != 0 || maxExemplar != 0 {
		err = db.txt.Update(func(tx *bbolt.Tx) error {
			if maxHistogram != 0 {
				histogramB := tx.Bucket(histogramData)
				var limit [8]byte
				binary.BigEndian.PutUint64(limit[:], maxHistogram)
				cu := histogramB.Cursor()
				for k, v := cu.First(); v != nil && bytes.Compare(k, limit[:]) < 1; k, v = cu.Next() {
					err := cu.Delete()
					if err != nil {
						return fmt.Errorf("deleting histogram %w", err)
					}
				}
			}
			if maxExemplar != 0 {
				exeB := tx.Bucket(exemplarData)
				var limit [8]byte
				binary.BigEndian.PutUint64(limit[:], maxExemplar)
				cu := exeB.Cursor()
				for k, v := cu.First(); v != nil && bytes.Compare(k, limit[:]) < 1; k, v = cu.Next() {
					err := cu.Delete()
					if err != nil {
						return fmt.Errorf("deleting exemplar %w", err)
					}
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	for i := range shard {
		err := db.deleteShard(shard[i])
		if err != nil {
			return err
		}
		db.lo.Info("deleting obsolete shard", "shard", shard[i])
	}
	return nil
}

func (db *Store) deleteShard(shard uint64) error {
	tx, err := db.rbf.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = tx.DeleteShard(shard)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (db *Store) findMaxHistogram(shard uint64, depth uint8) (uint64, error) {
	tx, err := db.rbf.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	records, err := tx.RootRecords()
	if err != nil {
		return 0, err
	}

	kind, ok := records.Get(rbf.Key{Column: keys.MetricsType, Shard: shard})
	if !ok {
		panic("missing metric type root records")
	}
	histogram, err := readBSIRange(tx, kind, shard, depth, bitmaps.EQ, int64(keys.Histogram), 0)
	if err != nil {
		return 0, err
	}
	floatHistogram, err := readBSIRange(tx, kind, shard, depth, bitmaps.EQ, int64(keys.FloatHistogram), 0)
	if err != nil {
		return 0, err
	}
	filter := histogram.Union(floatHistogram)
	if !filter.Any() {
		return 0, nil
	}
	value, ok := records.Get(rbf.Key{Column: keys.MetricsValue, Shard: shard})
	if !ok {
		panic("missing metric value root records")
	}
	return readBSIMax(tx, value, shard, depth, histogram.Union(floatHistogram))
}

func (db *Store) findMaxExemplar(shard uint64, depth uint8) (uint64, error) {
	tx, err := db.rbf.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	records, err := tx.RootRecords()
	if err != nil {
		return 0, err
	}

	kind, ok := records.Get(rbf.Key{Column: keys.MetricsType, Shard: shard})
	if !ok {
		panic("missing metric type root records")
	}

	exe, err := readBSIRange(tx, kind, shard, depth, bitmaps.EQ, int64(keys.Exemplar), 0)
	if err != nil {
		return 0, err
	}
	if !exe.Any() {
		return 0, nil
	}

	value, ok := records.Get(rbf.Key{Column: keys.MetricsValue, Shard: shard})
	if !ok {
		panic("missing metric value root records")
	}
	return readBSIMax(tx, value, shard, depth, exe)
}
