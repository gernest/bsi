package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"

	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"github.com/gernest/u128/internal/bitmaps"
	"github.com/gernest/u128/internal/checksum"
	"github.com/gernest/u128/internal/rbf"
	"github.com/gernest/u128/internal/storage/keys"
	"github.com/gernest/u128/internal/storage/magic"
	"github.com/gernest/u128/internal/storage/rows"
	"github.com/gernest/u128/internal/storage/seq"
	"github.com/gernest/u128/internal/storage/tsid"
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
			ts = magic.ReinterpretSlice[Shard](v)[0].Min
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

	ma := make(shardMap)

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

func (db *Store) saveMetadata(ma shardMap) error {
	var key [8]byte
	return db.txt.Update(func(tx *bbolt.Tx) error {
		adminB := tx.Bucket(admin)

		for shard, data := range ma {
			binary.BigEndian.PutUint64(key[:], shard)
			if v := adminB.Get(key[:]); v != nil {
				data.shard.Update(&magic.ReinterpretSlice[Shard](v)[0])
			}
			err := adminB.Put(key[:], data.shard.Bytes())
			if err != nil {
				return fmt.Errorf("updating shard data %w", err)
			}
		}
		return nil
	})
}

func (db *Store) apply(ma shardMap) error {

	tx, err := db.rbf.Begin(true)
	if err != nil {
		return fmt.Errorf("creating write transaction %w", err)
	}
	defer tx.Rollback()
	for shard, v := range ma {

		for col, ra := range v.ra {
			ra.Optimize()
			_, err = tx.AddRoaring(rbf.Key{Column: col, Shard: shard}, ra)
			if err != nil {
				return fmt.Errorf("writing bitmap %w", err)
			}
		}
	}
	return tx.Commit()
}

type Shard struct {
	Min         int64
	Max         int64
	MaxID       uint64
	TsDepth     uint8
	ValueDepth  uint8
	KindDepth   uint8
	LabelsDepth uint8
}

func (s *Shard) InRange(lo, hi int64) bool {
	return lo < s.Max && hi > s.Min
}

func (s *Shard) Update(other *Shard) {
	if s.Min == 0 {
		s.Min = other.Min
	}
	s.Min = min(s.Min, other.Min)
	s.Max = max(s.Max, other.Max)
	s.MaxID = max(s.MaxID, other.MaxID)
	s.TsDepth = max(s.TsDepth, other.TsDepth)
	s.ValueDepth = max(s.ValueDepth, other.ValueDepth)
}

func (s Shard) Bytes() []byte {
	return magic.ReinterpretSlice[byte]([]Shard{s})
}

type shardMap map[uint64]*shardData

func (s shardMap) Get(shard uint64) *shardData {
	r, ok := s[shard]
	if !ok {
		r = &shardData{ra: make(map[checksum.U128]*roaring.Bitmap)}
		s[shard] = r
	}
	return r
}

type shardData struct {
	shard Shard
	ra    map[checksum.U128]*roaring.Bitmap
}

func (s *shardData) AddIndex(start uint64, values []tsid.ID) {
	labels := s.Get(keys.MetricsLabels)
	var hi uint64

	for i := range values {
		la := &values[i]
		id := start + uint64(i)
		hi = max(la.ID, hi)
		bitmaps.BSI(labels, id, int64(la.ID))
		for j := range la.Views {
			bitmaps.Mutex(s.Get(la.Views[j]), id, la.Rows[j])
		}
	}
	s.shard.LabelsDepth = uint8(bits.Len64(hi)) + 1

}

func (s *shardData) AddTS(start uint64, values []int64) {
	ra := s.Get(keys.MetricsTimestamp)
	var lo, hi int64

	for i := range values {
		if lo == 0 {
			lo = values[i]
		}
		lo = min(lo, values[i])
		hi = max(hi, values[i])
		bitmaps.BSI(ra, start+uint64(i), values[i])
	}
	s.shard.Min = lo
	s.shard.Max = hi
	s.shard.TsDepth = uint8(bits.Len64(max(uint64(lo), uint64(hi)))) + 1
}

func (s *shardData) AddValues(start uint64, values []uint64) {
	ra := s.Get(keys.MetricsTimestamp)
	var hi uint64
	for i := range values {
		hi = max(hi, values[i])
		bitmaps.BSI(ra, start+uint64(i), int64(values[i]))
	}
	s.shard.ValueDepth = uint8(bits.Len64(hi)) + 1
}

func (s *shardData) AddKind(start uint64, values []keys.Kind) {
	ra := s.Get(keys.MetricsTimestamp)
	var hi keys.Kind
	for i := range values {
		hi = max(hi, values[i])
		bitmaps.BSI(ra, start+uint64(i), int64(values[i]))
	}
	s.shard.KindDepth = uint8(bits.Len64(uint64(hi))) + 1
}

func (s *shardData) Get(col checksum.U128) *roaring.Bitmap {
	r, ok := s.ra[col]
	if !ok {
		r = roaring.NewMapBitmap()
		s.ra[col] = r
	}
	return r
}
