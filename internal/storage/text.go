package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"slices"

	"github.com/cespare/xxhash/v2"
	"github.com/gernest/bsi/internal/storage/buffer"
	"github.com/gernest/bsi/internal/storage/magic"
	"github.com/gernest/roaring"
	"github.com/prometheus/prometheus/model/labels"
	"go.etcd.io/bbolt"
)

var (
	admin         = []byte("admin")
	columns       = []byte("columns")
	snapshots     = []byte("snapshots")
	metricsSum    = []byte("sum")
	metricsData   = []byte("data")
	histogramData = []byte("histograms")
	exemplarData  = []byte("exemplar")
	metaData      = []byte("meta")
	search        = []byte("index")
)

func translate(db *bbolt.DB, r *Rows) (hi uint64, err error) {

	size := len(r.Labels)
	r.ID = slices.Grow(r.ID, size)[:size]

	err = db.Update(func(tx *bbolt.Tx) error {

		// create new sequence
		{
			adminB := tx.Bucket(admin)
			id := adminB.Sequence()
			if id == 0 {
				id++
			}
			hi = id + uint64(len(r.Labels))
			err = adminB.SetSequence(hi)
			if err != nil {
				return fmt.Errorf("creating sequence %w", err)
			}
		}

		metricsSumB := tx.Bucket(metricsSum)
		metricsDataB := tx.Bucket(metricsData)
		searchIndexB := tx.Bucket(search)
		columnsB := tx.Bucket(columns)

		var sumB [8]byte
		for i := range r.Labels {
			if i != 0 && bytes.Equal(r.Labels[i], r.Labels[i-1]) {
				// fast path: ingesting same series with multiple samples.
				r.ID[i] = append(r.ID[i][:0], r.ID[i-1]...)
				continue
			}
			sum := binary.BigEndian.AppendUint64(sumB[:0], xxhash.Sum64(r.Labels[i]))

			r.ID[i] = r.ID[i][:0]

			if got := metricsSumB.Get(sum); got != nil {
				r.ID[i] = append(r.ID[i], got...)
				continue
			}
			var err error
			tid, err := metricsSumB.NextSequence()
			if err != nil {
				return fmt.Errorf("generating metrics sequence %w", err)
			}

			r.ID[i].Append(MetricsLabels, tid)
			// 2. store checksum => tsid in checksums bucket
			err = metricsSumB.Put(sum[:], r.ID[i])
			if err != nil {
				return fmt.Errorf("storing metrics checksum %w", err)
			}

			// 3. store metrics_id => labels.Labels
			err = metricsDataB.Put(binary.AppendUvarint(nil, tid), r.Labels[i])
			if err != nil {
				return fmt.Errorf("storing metrics data %w", err)
			}

			// Building index
			for name, value := range buffer.RangeLabels(r.Labels[i]) {
				labelNameB, err := searchIndexB.CreateBucketIfNotExists(name)
				if err != nil {
					return fmt.Errorf("creating label bucket %w", err)
				}
				view, err := txt2ID(columnsB, name)
				if err != nil {
					return fmt.Errorf("assigning column id %w", err)
				}
				if got := labelNameB.Get(value); got != nil {
					va, _ := binary.Uvarint(got)
					r.ID[i].Append(view, va)
				} else {
					// slow path: assign unique id to value
					nxt, err := labelNameB.NextSequence()
					if err != nil {
						return fmt.Errorf("assigning sequence id %w", err)
					}
					err = labelNameB.Put(value, binary.AppendUvarint(nil, nxt))
					if err != nil {
						return fmt.Errorf("storing sequence id %w", err)
					}
					r.ID[i].Append(view, nxt)
				}
			}

		}

		if r.Has(Histogram) || r.Has(FloatHistogram) {
			err = txt2u64(tx.Bucket(histogramData), r.Value, r.Histogram)
			if err != nil {
				return err
			}
		}
		if r.Has(Exemplar) {
			err = txt2u64(tx.Bucket(exemplarData), r.Value, r.Exemplar)
			if err != nil {
				return err
			}
		}
		if r.Has(Metadata) {
			metaB := tx.Bucket(metaData)

			// Unlike other sample types, we only need to store mapping between metric name
			// and metadata found. There is no need to store anything in RBF, this data
			// can be retrieved directly from txt database.
			var lastMeta, lastLabels []byte

			for i := range r.Metadata {
				if r.Kind[i] != Metadata {
					continue
				}
				if i != 0 && bytes.Equal(lastLabels, r.Labels[i]) && bytes.Equal(lastMeta, r.Metadata[i]) {
					r.Delete(i)
					continue
				}

				lastMeta = r.Metadata[i]
				lastLabels = r.Labels[i]

				for key, value := range buffer.RangeLabels(lastLabels) {
					if magic.String(key) == labels.MetricName {
						if metaB.Get(value) == nil {
							err = metaB.Put(value, lastMeta)
							if err != nil {
								return fmt.Errorf("storing metadata value %w", err)
							}
						}
						break
					}
				}
				r.Delete(i)
			}

		}
		return nil
	})
	return
}

func txt2ID(b *bbolt.Bucket, data []byte) (uint64, error) {
	if got := b.Get(data); got != nil {
		val, _ := binary.Uvarint(got)
		return val, nil
	}
	nxt, err := b.NextSequence()
	if err != nil {
		return 0, fmt.Errorf("assigning sequence %w", err)
	}
	val := binary.AppendUvarint(nil, nxt)
	err = b.Put(data, val)
	if err != nil {
		return 0, fmt.Errorf("storing sequence %w", err)
	}
	return nxt, nil
}

func txt2u64(b *bbolt.Bucket, values []uint64, data [][]byte) error {
	for i := range data {
		if len(data[i]) == 0 {
			continue
		}
		nxt, err := b.NextSequence()
		if err != nil {
			return fmt.Errorf("assigning sequence %w", err)
		}
		values[i] = nxt
		err = b.Put(binary.AppendUvarint(nil, nxt), data[i])
		if err != nil {
			return fmt.Errorf("storing sequence %w", err)
		}
	}
	return nil
}

func readFromU64(b *bbolt.Bucket, all *roaring.Bitmap, cb func(id uint64, value []byte) error) error {
	cu := b.Cursor()
	var lo, hi []byte
	for a, b := range rangeSetsRa(all) {
		lo = binary.AppendUvarint(lo[:0], a)
		hi = binary.AppendUvarint(hi[:0], b)

		for k, v := cu.Seek(lo[:]); v != nil && bytes.Compare(k, hi[:]) < 1; k, v = cu.Next() {
			key, _ := binary.Uvarint(k)
			err := cb(key, v)
			if err != nil {
				return err
			}
		}
	}
	return nil
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
