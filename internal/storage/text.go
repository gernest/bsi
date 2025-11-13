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
	"github.com/gernest/bsi/internal/storage/tsid"
	"github.com/gernest/roaring"
	"github.com/prometheus/prometheus/model/labels"
	"go.etcd.io/bbolt"
)

var (
	admin         = []byte("admin")
	snapshots     = []byte("snapshots")
	metricsSum    = []byte("sum")
	metricsData   = []byte("data")
	histogramData = []byte("histograms")
	exemplarData  = []byte("exemplar")
	metaData      = []byte("meta")
	search        = []byte("index")
)

func translate(db *bbolt.DB, out *tsid.B, r *Rows) (hi uint64, err error) {

	// we make sure out.B has the same size as labels.
	size := len(r.Labels)
	out.B = slices.Grow(out.B[:0], size)[:size]

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

		var sumB [8]byte
		for i := range r.Labels {
			if i != 0 && bytes.Equal(r.Labels[i], r.Labels[i-1]) {
				// fast path: ingesting same series with multiple samples.
				out.B[i] = out.B[i-1]
				continue
			}
			sum := binary.BigEndian.AppendUint64(sumB[:0], xxhash.Sum64(r.Labels[i]))
			if got := metricsSumB.Get(sum); got != nil {
				// fast path: we have already processed labels in this view. We don't need
				// to do any more work.
				out.B[i] = append(out.B[i][:0], magic.ReinterpretSlice[tsid.Column](got)...)
				continue
			}

			// generate tsid
			out.B[i] = out.B[i][:0]

			var err error
			tid, err := metricsSumB.NextSequence()
			if err != nil {
				return fmt.Errorf("generating metrics sequence %w", err)
			}
			out.B[i] = append(out.B[i], tsid.Column{
				ID:    MetricsLabels,
				Value: tid,
			})

			// Building index
			for name, value := range buffer.RangeLabels(r.Labels[i]) {
				labelNameB, err := searchIndexB.CreateBucketIfNotExists(name)
				if err != nil {
					return fmt.Errorf("creating label bucket %w", err)
				}
				view := xxhash.Sum64(name)

				if got := labelNameB.Get(value); got != nil {
					// fast path: we already assigned unique id for label value
					out.B[i] = append(out.B[i], tsid.Column{
						ID:    view,
						Value: binary.BigEndian.Uint64(got),
					})
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
					out.B[i] = append(out.B[i], tsid.Column{
						ID:    view,
						Value: nxt,
					})
				}
			}

			// 2. store checksum => tsid in checksums bucket
			err = metricsSumB.Put(sum[:], magic.ReinterpretSlice[byte](out.B[i]))
			if err != nil {
				return fmt.Errorf("storing metrics checksum %w", err)
			}

			// 3. store labels_sequence_id => labels_data in data bucket
			err = metricsDataB.Put(binary.BigEndian.AppendUint64(nil, out.B[i][0].Value), r.Labels[i])
			if err != nil {
				return fmt.Errorf("storing metrics data %w", err)
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

func txt2u64(b *bbolt.Bucket, values []uint64, data [][]byte) error {
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
