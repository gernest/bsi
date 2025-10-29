package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"slices"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/internal/checksum"
	"github.com/gernest/u128/internal/storage/buffer"
	"github.com/gernest/u128/internal/storage/magic"
	"github.com/gernest/u128/internal/storage/tsid"
	"github.com/prometheus/prometheus/model/labels"
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

func assignTSID(db *bbolt.DB, out *tsid.B, labels [][]byte) (hi uint64, err error) {

	// we make sure out.B has the same size as labels.
	size := len(labels)
	out.B = slices.Grow(out.B[:0], size)[:size]

	err = db.Update(func(tx *bbolt.Tx) error {

		// create new sequence
		{
			adminB := tx.Bucket(admin)
			id := adminB.Sequence()
			if id == 0 {
				id++
			}
			hi = id + uint64(len(labels))
			err = adminB.SetSequence(hi)
			if err != nil {
				return fmt.Errorf("creating sequence %w", err)
			}
		}

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
					id.Views = append(id.Views, view)
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
	return
}

func assignU64ToHistograms(db *bbolt.DB, values []uint64, data [][]byte) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(histogramData)
		return translate(b, values, data)
	})
}

func assignU64ToExemplars(db *bbolt.DB, values []uint64, data [][]byte) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(exemplarData)
		return translate(b, values, data)
	})
}

func assignU64ToMetadata(db *bbolt.DB, values []uint64, data [][]byte) error {
	return db.Update(func(tx *bbolt.Tx) error {
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

func readFromU64(b *bbolt.Bucket, all *roaring.Bitmap, cb func(id uint64, value []byte) error) error {
	cu := b.Cursor()
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
}

type Matchers struct {
	Columns []checksum.U128
	Negate  []bool
	Rows    []*roaring.Bitmap
}

// Any returns true if m conditions might match.
func (m *Matchers) Any() bool {
	// Prometheus labels matchers is an intersection of all condition. We can safely determine
	// state of m that will never satisfy conditions without touching rbf.

	for i := range m.Columns {
		if !m.Negate[i] && !m.Rows[i].Any() {
			// one of the condition is false. Intersection will always be false.
			return false
		}
	}
	return true
}

// LabelMatchers finds rows matching matchers and write them to results.
func LabelMatchers(db *bbolt.DB, result *Matchers, matches []*labels.Matcher) error {
	return db.View(func(tx *bbolt.Tx) error {
		searchB := tx.Bucket(search)

		for _, l := range matches {
			col := checksum.Hash(magic.Slice(l.Name))
			ra := roaring.NewBitmap()
			var negate bool
			switch l.Type {
			case labels.MatchEqual, labels.MatchNotEqual:
				negate = l.Type == labels.MatchNotEqual
				b := searchB.Bucket(magic.Slice(l.Name))
				if b != nil {
					v := b.Get(magic.Slice(l.Value))
					if v != nil {
						ra.DirectAdd(binary.BigEndian.Uint64(v))
					}
				}
			case labels.MatchRegexp, labels.MatchNotRegexp:
				negate = l.Type == labels.MatchNotRegexp
				b := searchB.Bucket(magic.Slice(l.Name))
				if b != nil {
					cu := b.Cursor()
					prefix := magic.Slice(l.Prefix())
					for k, v := cu.Seek(prefix); v != nil && bytes.HasPrefix(k, prefix); k, v = cu.Next() {
						ra.DirectAdd(binary.BigEndian.Uint64(v))
					}
				}
			}
			result.Columns = append(result.Columns, col)
			result.Negate = append(result.Negate, negate)
			result.Rows = append(result.Rows, ra)
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
