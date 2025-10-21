package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/checksum"
	"github.com/gernest/u128/rbf"
	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/tsid"
	"go.etcd.io/bbolt"
)

var (
	sys         = []byte("sys")
	metrics     = []byte("metrics")
	metricsSum  = []byte("sum")
	metricsData = []byte("data")
	search      = []byte("index")
)

type db struct {
	rbf  *rbf.DB
	meta *bbolt.DB
}

var _ DB = (*db)(nil)

// AllocateID implements DB using bolt bucket sequence api.
func (db *db) AllocateID(size uint64) (hi uint64, err error) {
	err = db.meta.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(sys)
		hi = b.Sequence() + size
		return b.SetSequence(hi)
	})
	return
}

// GetTSID implements DB.
//
// This uses a single bolt database as the underlying storage. We rely heavily on
// nested buckets to map key hierarchy.
// Here is a simple outlook for generating tsid for metrics labels {foo=xxx, bar=yyy, baz=zzz}
// we use a = b to denote key value paris.
//
//	                                                     ┌──────┐
//	                                                     │ sys  │
//	                                                     │      │
//	                                                     └──────┘
//	                                                        │
//	                                                        ▼
//	                                                    ┌───────┐
//	                                                    │ view  │
//	                                                    │       │
//	                                                    └───────┘
//	                                                        │
//	                                                        ▼
//	                                                  ┌──────────┐
//	                                                  │ metrics  │
//	                                                  │          │
//	                                                  └──────────┘
//	                                                     │  │  │
//	                     ┌───────────────────────────────┘  │  └───────────────────────────┐
//	                     │                                  │                              │
//	                     ▼                                  ▼                              ▼
//	              ┌────────────┐                        ┌───────┐                    ┌──────────┐
//	              │ checksums  │                        │ data  │                    │  index   │
//	              │            │                        │       │                    │          │
//	              └────────────┘                        └───────┘                    └──────────┘
//	                     │                                  │                           │  │  │
//	                     │                                  │                  ┌────────┘  │  └───────┐
//	                     │                                  │                  │           │          │
//	                     ▼                                  ▼                  ▼           ▼          ▼
//	┌──────────────────────────────────────────┌────────────────────────────┌──────┐   ┌──────┐    ┌──────┐
//	│u128 = {id =1 , view=[foo, bar, baz], rows│1 = {foo=xxx, bar=yyy baz=zz│}foo  │   │ bar  │    │ baz  │
//	│                                          │           │                │  │   │   │      │    │      │
//	└──────────────────────────────────────────└────────────────────────────└──────┘   └──────┘    └──────┘
//	                                                                           │           │          │
//	                                                                           ▼           ▼          ▼
//	                                                                      ┌────────┐  ┌────────┐  ┌────────┐
//	                                                                      │xxx = 1 │  │yyy = 2 │  │zzz = 3 │
//	                                                                      │        │  │        │  │        │
//	                                                                      └────────┘  └────────┘  └────────┘
//
// We scope things under view bucket to simplify application of retention policies.
// We can simply delete the view bucket to remove all old data collected in it.
func (db *db) GetTSID(out *tsid.ID, view, labels []byte) error {
	return db.meta.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(sys)
		vb, err := b.CreateBucketIfNotExists(view)
		if err != nil {
			return err
		}
		metricsB, err := vb.CreateBucketIfNotExists(metrics)
		if err != nil {
			return err
		}

		metricsSumB, err := metricsB.CreateBucketIfNotExists(metricsSum)
		if err != nil {
			return err
		}
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
		out.ID, err = metricsB.NextSequence()

		searchIndexB, err := metricsB.CreateBucketIfNotExists(search)
		if err != nil {
			return err
		}

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
		dB, err := metricsB.CreateBucketIfNotExists(metricsData)
		if err != nil {
			return fmt.Errorf("creating metrics data bucket %w", err)
		}
		err = dB.Put(binary.BigEndian.AppendUint64(nil, out.ID), labels)
		if err != nil {
			return fmt.Errorf("storing metrics data %w", err)
		}
		return nil
	})
}

// Apply implements DB.
func (db *db) Apply(data iter.Seq2[string, *roaring.Bitmap]) error {
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
