package rows

import (
	"bytes"
	"iter"
	"math"

	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/keys"
	"github.com/prometheus/prometheus/model/labels"
)

type Row struct {
	Value     uint64
	Timestamp int64
	Kind      keys.Kind
}

// Rows is an in memory batch of metrics belonging to a single view.
type Rows struct {
	Labels    [][]byte
	Timestamp []int64
	Value     []uint64
	Histogram [][]byte
	Metadata  [][]byte
	Exemplar  [][]byte
	Kind      []keys.Kind
}

// Range iterates over all rows found in r.
func (r *Rows) Range() iter.Seq2[int, *Row] {
	return func(yield func(int, *Row) bool) {
		var o Row
		for i := range r.Timestamp {
			o.Value = r.Value[i]
			o.Timestamp = r.Timestamp[i]
			o.Kind = r.Kind[i]

			if !yield(i, &o) {
				return
			}
		}
	}
}

// AppendFloat stores float sample.
func (r *Rows) AppendFloat(la labels.Labels, ts int64, value float64) {
	r.Labels = append(r.Labels, bytes.Clone(buffer.UnwrapLabel(&la)))
	r.Timestamp = append(r.Timestamp, ts)
	r.Value = append(r.Value, math.Float64bits(value))

	r.Kind = append(r.Kind, keys.Float)
	r.Histogram = append(r.Histogram, nil)
	r.Metadata = append(r.Metadata, nil)
	r.Exemplar = append(r.Exemplar, nil)
}

// AppendHistogram stores histogram sample.
func (r *Rows) AppendHistogram(la labels.Labels, ts int64, value []byte) {
	r.Labels = append(r.Labels, bytes.Clone(buffer.UnwrapLabel(&la)))
	r.Timestamp = append(r.Timestamp, ts)
	r.Histogram = append(r.Histogram, value)

	r.Kind = append(r.Kind, keys.Histogram)
	r.Value = append(r.Value, 0)
	r.Metadata = append(r.Metadata, nil)
	r.Exemplar = append(r.Exemplar, nil)
}

// AppendExemplar adds exemplar row.
func (r *Rows) AppendExemplar(la labels.Labels, ts int64, value []byte) {
	r.Labels = append(r.Labels, bytes.Clone(buffer.UnwrapLabel(&la)))
	r.Timestamp = append(r.Timestamp, ts)
	r.Exemplar = append(r.Exemplar, value)

	r.Kind = append(r.Kind, keys.Exemplar)
	r.Value = append(r.Value, 0)
	r.Metadata = append(r.Metadata, nil)
	r.Histogram = append(r.Histogram, nil)
}

func (r *Rows) AppendMetadata(la labels.Labels, ts int64, value []byte) {
	r.Labels = append(r.Labels, bytes.Clone(buffer.UnwrapLabel(&la)))
	r.Timestamp = append(r.Timestamp, ts)
	r.Metadata = append(r.Metadata, value)

	r.Kind = append(r.Kind, keys.Metadata)
	r.Value = append(r.Value, 0)
	r.Histogram = append(r.Histogram, nil)
	r.Exemplar = append(r.Exemplar, nil)
}

// Reset resets r fields and retain capacity.
func (r *Rows) Reset() {
	r.Labels = reset(r.Labels)
	r.Timestamp = reset(r.Timestamp)
	r.Value = reset(r.Value)
	r.Histogram = reset(r.Histogram)
	r.Metadata = reset(r.Metadata)
	r.Exemplar = reset(r.Exemplar)
	r.Kind = reset(r.Kind)
}

func reset[T any](a []T) []T {
	clear(a)
	return a[:0]
}
