package rows

import (
	"bytes"
	"math"
	"sync"

	"github.com/gernest/u128/internal/storage/buffer"
	"github.com/gernest/u128/internal/storage/keys"
	"github.com/prometheus/prometheus/model/labels"
)

type Pool struct {
	p sync.Pool
}

func (p *Pool) Get() *Rows {
	if v := p.p.Get(); v != nil {
		return v.(*Rows)
	}
	return &Rows{}
}

func (p *Pool) Put(r *Rows) {
	r.Reset()
	p.p.Put(r)
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

	none  int
	flags keys.Kind
}

func (r *Rows) Size() int {
	return len(r.Labels) - r.none
}

func (r *Rows) Delete(i int) {
	r.Kind[i] = keys.None
	r.none++
}

// Has returns true if r has kind sample. It is a quick way to see if r needs to be
// translated.
func (r *Rows) Has(kind keys.Kind) bool {
	return r.flags&kind == kind
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
	r.flags |= keys.Float
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
	r.flags |= keys.Histogram
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
	r.flags |= keys.Exemplar
}

func (r *Rows) AppendMetadata(la labels.Labels, ts int64, value []byte) {
	r.Labels = append(r.Labels, bytes.Clone(buffer.UnwrapLabel(&la)))
	r.Timestamp = append(r.Timestamp, ts)
	r.Metadata = append(r.Metadata, value)

	r.Kind = append(r.Kind, keys.Metadata)
	r.Value = append(r.Value, 0)
	r.Histogram = append(r.Histogram, nil)
	r.Exemplar = append(r.Exemplar, nil)
	r.flags |= keys.Metadata
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
