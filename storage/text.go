package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"slices"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/checksum"
	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/rows"
	"github.com/gernest/u128/storage/tsid"
	"go.etcd.io/bbolt"
)

var (
	admin         = []byte("admin")
	metricsSum    = []byte("sum")
	metricsData   = []byte("data")
	histogramData = []byte("histograms")
	exemplarData  = []byte("exemplar")
	metaData      = []byte("meta")
	search        = []byte("index")
)

type textKey struct {
	column checksum.U128
	view   rows.View
}

func (t textKey) String() string {
	return t.Path("")
}

func (t textKey) Path(base string) string {
	return filepath.Join(
		base,
		fmt.Sprintf("%s_%x", t.view, t.column),
	)
}

type txtOptions struct {
	dataPath string
}

type txt struct {
	db *bbolt.DB
}

func openTxt(key textKey, opts txtOptions) (*txt, error) {
	file := key.Path(opts.dataPath)
	_, err := os.Stat(file)
	created := os.IsNotExist(err)
	db, err := bbolt.Open(file, 0600, nil)
	if err != nil {
		return nil, err
	}
	if created {
		db.Update(func(tx *bbolt.Tx) error {
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
	}

	return &txt{db: db}, nil
}

func (t *txt) Close() error {
	return t.db.Close()
}

func (t *txt) AllocateID(size uint64) (hi uint64, err error) {
	err = t.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(admin)
		o := b.Sequence()
		if o == 0 {
			o++
		}
		hi = o + size
		return b.SetSequence(hi)
	})
	return
}

// GetTSID assigns tsid to labels. We use u128 checksum to track processed labels.
// This is resource intensive, slow path includes assigning ids for all label entries.
func (t *txt) GetTSID(out *tsid.B, labels [][]byte) error {

	// we make sure out.B has the same size as labels.
	size := len(labels)
	out.B = slices.Grow(out.B[:0], size)[:size]

	return t.db.Update(func(tx *bbolt.Tx) error {
		metricsSumB := tx.Bucket(metricsSum)
		metricsDataB := tx.Bucket(metricsData)
		searchIndexB := tx.Bucket(search)

		for i := range labels {
			if i != 0 && bytes.Equal(labels[i], labels[i-1]) {
				// fast path: ingesting same series with multiple samples.
				out.B[i] = out.B[i-1]
				continue
			}
			sum := checksum.Hash(labels[i])
			if got := metricsSumB.Get(sum[:]); got != nil {
				// fast path: we have already processed labels in this view. We don't need
				// to do any more work.
				out.B[i].Decode(got)
				continue
			}

			// generate tsid
			id := &out.B[i]
			id.Reset()
			var err error
			id.ID, err = metricsSumB.NextSequence()
			if err != nil {
				return fmt.Errorf("generating metrics sequence %w", err)
			}

			// Building index
			for name, value := range buffer.RangeLabels(labels[i]) {
				labelNameB, err := searchIndexB.CreateBucketIfNotExists(name)
				if err != nil {
					return fmt.Errorf("creating label bucket %w", err)
				}
				view := checksum.Hash(name)

				if got := labelNameB.Get(value); got != nil {
					// fast path: we already assigned unique id for label value
					id.Views = append(id.Views, view)
					id.Rows = append(id.Rows, binary.BigEndian.Uint64(got))
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
					id.Views = append(id.Views, sum)
					id.Rows = append(id.Rows, nxt)
				}

			}

			// 2. store checksum => tsid in checksums bucket
			err = metricsSumB.Put(sum[:], id.Encode())
			if err != nil {
				return fmt.Errorf("storing metrics checksum %w", err)
			}

			// 3. store labels_sequence_id => labels_data in data bucket
			err = metricsDataB.Put(binary.BigEndian.AppendUint64(nil, id.ID), labels[i])
			if err != nil {
				return fmt.Errorf("storing metrics data %w", err)
			}
		}
		return nil
	})
}

func (t *txt) TranslateHistogram(values []uint64, data [][]byte) error {
	return t.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(histogramData)
		return translate(b, values, data)
	})
}

func (t *txt) TranslateExemplar(values []uint64, data [][]byte) error {
	return t.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(exemplarData)
		return translate(b, values, data)
	})
}

func (t *txt) TranslateMetadata(values []uint64, data [][]byte) error {
	return t.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(metaData)
		return translate(b, values, data)
	})
}

func translate(b *bbolt.Bucket, values []uint64, data [][]byte) error {
	for i := range data {
		if len(data[i]) == 0 {
			continue
		}
		nxt, err := b.NextSequence()
		if err != nil {
			return fmt.Errorf("assigning histogram sequence %w", err)
		}
		values[i] = nxt
		err = b.Put(binary.BigEndian.AppendUint64(nil, nxt), data[i])
		if err != nil {
			return fmt.Errorf("storing histogram sequence %w", err)
		}
	}
	return nil
}

// ReadLabels reads encoded labels.Labels value from the storage. Labels are stored in
// data bucket as (id, []byte) tuple. Here we search for all and call cb for every value
// found.
func (t *txt) ReadLabels(all *roaring.Bitmap, cb func(id uint64, value []byte) error) error {
	return t.db.View(func(tx *bbolt.Tx) error {

		cu := tx.Bucket(metricsData).Cursor()

		var lo, hi [8]byte
		for a, b := range rangeSetsRa(all) {
			binary.BigEndian.PutUint64(lo[:], a)
			binary.BigEndian.PutUint64(hi[:], b)

			for k, v := cu.Seek(lo[:]); v != nil && bytes.Compare(k, hi[:]) < 1; k, v = cu.Next() {
				key := binary.BigEndian.Uint64(k)
				err := cb(key, v)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// ReadHistograms reads histogram data from storage. all is a bitmap of histogram
// ids, for each id found cb is called with the value. Callers must copy value slice
// if they want to use it beyond this method scope.
func (t *txt) ReadHistograms(all *roaring.Bitmap, cb func(id uint64, value []byte) error) error {
	return t.db.View(func(tx *bbolt.Tx) error {

		cu := tx.Bucket(histogramData).Cursor()
		var lo, hi [8]byte
		for a, b := range rangeSetsRa(all) {
			binary.BigEndian.PutUint64(lo[:], a)
			binary.BigEndian.PutUint64(hi[:], b)

			for k, v := cu.Seek(lo[:]); v != nil && bytes.Compare(k, hi[:]) < 1; k, v = cu.Next() {
				key := binary.BigEndian.Uint64(k)
				err := cb(key, v)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func rangeSetsRa(ra *roaring.Bitmap) iter.Seq2[uint64, uint64] {
	return func(yield func(uint64, uint64) bool) {
		start := uint64(0)
		end := uint64(0)
		for v := range ra.RangeAll() {
			if start == 0 {
				start = v
				end = v
				continue
			}
			if v-end < 2 {
				end = v
				continue
			}
			if !yield(start, end) {
				return
			}
			start = v
			end = v
		}
		if start != 0 {
			yield(start, end)
		}
	}
}
