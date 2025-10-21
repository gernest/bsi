package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"

	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"github.com/gernest/u128/bitmaps"
	"github.com/gernest/u128/checksum"
	"github.com/gernest/u128/rbf"
	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/magic"
	"github.com/gernest/u128/storage/tsid"
	"github.com/prometheus/prometheus/model/labels"
	"go.etcd.io/bbolt"
)

var (
	metricsSum  = []byte("sum")
	metricsData = []byte("data")
	search      = []byte("index")
)

// Unique ISO ISO 8601 (yer, week) tuple. Stores timeseries data using rbf for numerical
// data and bolt for non numerical data.
//
// Creates a total of 3 files.
// - data
// - wal
// - text
// dta and wal are managed by rbf and text is a bolt database.
type dbView struct {
	rbf  *rbf.DB
	meta *bbolt.DB
}

func (db *dbView) Close() error {
	return errors.Join(
		db.rbf.Close(),
		db.meta.Close(),
	)
}

// AllocateID assigns monotonically increasing sequences covering range size.
// Returns the upper bound which is exclusive. Count starts from 1.
//
// if size is 3 , it will return 4 which will yield sequence 1, 2, 3.
func (db *dbView) AllocateID(size uint64) (hi uint64, err error) {
	err = db.meta.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(metricsData)
		o := b.Sequence()
		if o == 0 {
			o++
		}
		hi = o + size
		return b.SetSequence(hi)
	})
	return
}

// GetTSID creates or returns existing TSID from storage.
func (db *dbView) GetTSID(out *tsid.ID, labels []byte) error {
	return db.meta.Update(func(tx *bbolt.Tx) error {
		metricsSumB := tx.Bucket(metricsSum)

		// This is the heart and building block of everything. We rely on speed and
		// cryptographic property to ensure series are correctly enumerated in each
		// view.
		sum := checksum.Hash(labels)
		if got := metricsSumB.Get(sum[:]); got != nil {
			// fast path: we have already processed labels in this view. We don't need
			// to do any more work.
			out.Decode(got)
			return nil
		}

		out.Reset()
		var err error
		out.ID, err = metricsSumB.NextSequence()
		if err != nil {
			return fmt.Errorf("generating metrics sequence %w", err)
		}

		searchIndexB := tx.Bucket(search)

		// Building index
		for name, value := range buffer.RangeLabels(labels) {
			labelNameB, err := searchIndexB.CreateBucketIfNotExists(name)
			if err != nil {
				return fmt.Errorf("creating label bucket %w", err)
			}
			if got := labelNameB.Get(value); got != nil {
				// fast path: we already assigned unique id for label value
				out.Views = append(out.Views, name)
				out.Rows = append(out.Rows, binary.BigEndian.Uint64(got))
			} else {
				// slow path: assign unique id to value
				nxt, err := labelNameB.NextSequence()
				if err != nil {
					return fmt.Errorf("assigning sequence id %w", err)
				}
				err = labelNameB.Put(value, binary.BigEndian.AppendUint64(nil, nxt))
				if err != nil {
					return fmt.Errorf("storing sequence id %w", err)
				}
				out.Views = append(out.Views, name)
				out.Rows = append(out.Rows, nxt)
			}

		}

		// 1. encode tsid
		buf := bytesPool.Get()
		defer bytesPool.Put(buf)
		out.Encode(buf)

		// 2. store checksum => tsid in checksums bucket
		err = metricsSumB.Put(sum[:], bytes.Clone(buf.B))
		if err != nil {
			return fmt.Errorf("storing metrics checksum %w", err)
		}

		// 3. store labels_sequence_id => labels_data in data bucket
		dB := tx.Bucket(metricsData)
		err = dB.Put(binary.BigEndian.AppendUint64(nil, out.ID), labels)
		if err != nil {
			return fmt.Errorf("storing metrics data %w", err)
		}
		return nil
	})
}

// Apply implements DB.
func (db *dbView) Apply(data iter.Seq2[string, *roaring.Bitmap]) error {
	tx, err := db.rbf.Begin(true)
	if err != nil {
		return fmt.Errorf("creating write transaction %w", err)
	}
	defer tx.Rollback()

	for k, v := range data {
		_, err := tx.AddRoaring(k, v)
		if err != nil {
			return fmt.Errorf("storing bitmap %v %w", k, err)
		}
	}
	return nil
}

// Search finds matching rows across multiple views and fields. Computing views is left
// to the caller, the only condition is they must be sorted in lexicographic order.
func (db *dbView) Search(startTs, endTs int64, selectors []*labels.Matcher) error {
	matchers := map[string]*roaring.Bitmap{}
	err := db.buildIndex(selectors, func(field string, ra *roaring.Bitmap) error {
		matchers[field] = ra
		return nil
	})
	if err != nil {
		return err
	}

	for _, l := range selectors {
		switch l.Type {
		case labels.MatchEqual, labels.MatchRegexp:
			if _, ok := matchers[l.Name]; !ok {
				//fast path: we will never be able to fulfil match conditions.
				// we know for a fact that the label values are not in our database.
				return nil
			}
		}
	}

	return nil
}

func (db *dbView) buildIndex(matchers []*labels.Matcher, cb func(field string, ra *roaring.Bitmap) error) error {
	return db.meta.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(search)
		for _, l := range matchers {
			sb := b.Bucket(magic.Slice(l.Name))
			if sb == nil {
				continue
			}
			switch l.Type {
			case labels.MatchEqual, labels.MatchNotEqual:
				v := sb.Get(magic.Slice(l.Value))
				if v != nil {
					err := cb(l.Name, roaring.NewBitmap(
						binary.BigEndian.Uint64(v),
					))
					if err != nil {
						return err
					}
				}
			case labels.MatchRegexp, labels.MatchNotRegexp:
				cu := sb.Cursor()
				px := magic.Slice(l.Prefix())
				ra := roaring.NewBitmap()
				for k, v := cu.Seek(px); v != nil && bytes.HasPrefix(k, px); k, v = cu.Next() {
					ra.DirectAdd(
						binary.BigEndian.Uint64(v),
					)
				}
				err := cb(l.Name, ra)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// readBSIRange performs a range search  in predicate...end bounds with upper bound being exclusive.
func readBSIRange(tx *rbf.Tx, root uint32, shard uint64, predicate, end int64) (*roaring.Bitmap, error) {
	cu := tx.CursorFromRoot(root)
	defer cu.Close()

	// compute bit depth
	mx, err := cu.Max()
	if err != nil {
		return nil, fmt.Errorf("computing max value %w", err)
	}
	depth := mx / shardwidth.ShardWidth
	return bitmaps.Range(cu, bitmaps.BETWEEN, shard, depth, predicate, end)
}
