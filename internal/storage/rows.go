package storage

import (
	"bytes"
	"math"

	"github.com/gernest/bsi/internal/storage/buffer"
	"github.com/prometheus/prometheus/model/labels"
)

// Rows is an in memory batch of metrics belonging to a single view.
type Rows struct {
	Labels    [][]byte
	Timestamp []int64
	Value     []uint64
	Histogram [][]byte
	Metadata  [][]byte
	Exemplar  [][]byte
	Kind      []Kind

	none  int
	flags Kind
}

func (r *Rows) Size() int {
	return len(r.Labels) - r.none
}

func (r *Rows) Delete(i int) {
	r.Kind[i] = None
	r.none++
}

// Has returns true if r has kind sample. It is a quick way to see if r needs to be
// translated.
func (r *Rows) Has(kind Kind) bool {
	return r.flags&kind == kind
}

// AppendFloat stores float sample.
func (r *Rows) AppendFloat(la labels.Labels, ts int64, value float64) {
	r.Labels = append(r.Labels, bytes.Clone(buffer.UnwrapLabel(&la)))
	r.Timestamp = append(r.Timestamp, ts)
	r.Value = append(r.Value, math.Float64bits(value))

	r.Kind = append(r.Kind, Float)
	r.Histogram = append(r.Histogram, nil)
	r.Metadata = append(r.Metadata, nil)
	r.Exemplar = append(r.Exemplar, nil)
	r.flags |= Float
}

// AppendHistogram stores histogram sample.
func (r *Rows) AppendHistogram(la labels.Labels, ts int64, value []byte, isFloat bool) {
	r.Labels = append(r.Labels, bytes.Clone(buffer.UnwrapLabel(&la)))
	r.Timestamp = append(r.Timestamp, ts)
	r.Histogram = append(r.Histogram, value)
	kind := Histogram
	if isFloat {
		kind = FloatHistogram
	}
	r.Kind = append(r.Kind, kind)
	r.Value = append(r.Value, 0)
	r.Metadata = append(r.Metadata, nil)
	r.Exemplar = append(r.Exemplar, nil)
	r.flags |= kind
}

// AppendExemplar adds exemplar row.
func (r *Rows) AppendExemplar(la labels.Labels, ts int64, value []byte) {
	r.Labels = append(r.Labels, bytes.Clone(buffer.UnwrapLabel(&la)))
	r.Timestamp = append(r.Timestamp, ts)
	r.Exemplar = append(r.Exemplar, value)

	r.Kind = append(r.Kind, Exemplar)
	r.Value = append(r.Value, 0)
	r.Metadata = append(r.Metadata, nil)
	r.Histogram = append(r.Histogram, nil)
	r.flags |= Exemplar
}

func (r *Rows) AppendMetadata(la labels.Labels, ts int64, value []byte) {
	r.Labels = append(r.Labels, bytes.Clone(buffer.UnwrapLabel(&la)))
	r.Timestamp = append(r.Timestamp, ts)
	r.Metadata = append(r.Metadata, value)

	r.Kind = append(r.Kind, Metadata)
	r.Value = append(r.Value, 0)
	r.Histogram = append(r.Histogram, nil)
	r.Exemplar = append(r.Exemplar, nil)
	r.flags |= Metadata
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
	r.flags = 0
}

func reset[T any](a []T) []T {
	clear(a)
	return a[:0]
}
