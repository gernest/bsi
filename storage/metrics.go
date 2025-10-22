package storage

import (
	"encoding/binary"
	"errors"
	"iter"
	"math"
	"slices"
	"unsafe"

	"github.com/gernest/roaring"
	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/magic"
	"github.com/gernest/u128/storage/prefix"
)

const (
	offsetSize = uint32(unsafe.Sizeof(Offsets{}))

	// headerBaseSize is the size in bytes of the cookie, flags, and rows count
	// at the beginning of a file.
	headerBaseSize = 3 + 1 + 4

	serialCookieNoHistogram = 12345 // only float values
	serialCookieNoFloats    = 12346 // only histograms
	serialCookie            = 12347 // mixed histograms and floats
)

// Rows is an in memory batch of metrics belonging to a single view.
type Rows struct {
	Labels    [][]byte
	Timestamp []int64
	Value     []uint64
	Histogram [][]byte
}

// AppendFloat stores float sample.
func (r *Rows) AppendFloat(la []byte, ts int64, value float64) {
	r.Labels = append(r.Labels, la)
	r.Timestamp = append(r.Timestamp, ts)
	r.Value = append(r.Value, math.Float64bits(value))
	r.Histogram = append(r.Histogram, nil)
}

// AppendHistogram stores histogram sample.
func (r *Rows) AppendHistogram(la []byte, ts int64, value []byte) {
	r.Labels = append(r.Labels, la)
	r.Timestamp = append(r.Timestamp, ts)
	r.Value = append(r.Value, 0)
	r.Histogram = append(r.Histogram, value)
}

// Reset resets r fields and retain capacity.
func (r *Rows) Reset() {
	ra := roaring.NewBitmap()
	ra.MarshalBinary()
	clear(r.Labels)
	clear(r.Histogram)
	r.Labels = r.Labels[:0]
	r.Timestamp = r.Timestamp[:0]
	r.Value = r.Value[:0]
	r.Histogram = r.Histogram[:0]
}

// Serialize encodes r into w in a way that we avoid copying when deserializing.
func (r *Rows) Serialize(w *buffer.B) {
	w.B = slices.Grow(w.B[:0], headerBaseSize)[:headerBaseSize]
	var histCount int

	for i := range r.Histogram {
		if len(r.Histogram[i]) != 0 {
			histCount++
			break
		}
	}
	cookie := serialCookie
	switch {
	case histCount == 0:
		cookie = serialCookieNoHistogram
	case histCount == len(r.Timestamp):
		cookie = serialCookieNoFloats
	}

	binary.LittleEndian.PutUint32(w.B, uint32(cookie))
	binary.LittleEndian.PutUint32(w.B[4:], uint32(len(r.Timestamp)))

	var offsets Offsets

	offsets.labels.start = uint32(len(w.B))
	offsets.labels.end = writeSlice(w, r.Labels)
	offsets.timestamp.start = uint32(len(w.B))
	offsets.timestamp.end = writeMagic(w, r.Timestamp)
	if cookie != serialCookieNoFloats {
		offsets.value.start = uint32(len(w.B))
		offsets.value.end = writeMagic(w, r.Value)
	}
	if cookie != serialCookieNoHistogram {
		offsets.histogram.start = uint32(len(w.B))
		offsets.histogram.end = writeSlice(w, r.Histogram)
	}
	writeMagic(w, []Offsets{offsets})
}

func writeSlice(w *buffer.B, src [][]byte) uint32 {
	for i := range src {
		w.B = prefix.Encode(w.B, src[i])
	}
	return uint32(len(w.B))
}

func writeMagic[T any](w *buffer.B, src []T) uint32 {
	w.B = append(w.B, magic.ReinterpretSlice[byte](src)...)
	return uint32(len(w.B))
}

// Offsets store positions of encoded Rows fields.
type Offsets struct {
	labels    pos
	timestamp pos
	value     pos
	histogram pos
}

var zero pos

// HasHistograms returns true if offsets hae histogram rows.
func (o Offsets) HasHistograms() bool {
	return o.histogram != zero
}

// HasFloats returns true if offsets hae float rows.
func (o Offsets) HasFloats() bool {
	return o.value != zero
}

// GetOffsets returns Offsets struct encoded src.
func GetOffsets(src []byte) (Offsets, error) {
	if len(src) < headerBaseSize {
		return Offsets{}, errors.New("invalid data: not long enough to be a serialized rows header")
	}
	return magic.ReinterpretSlice[Offsets](src[len(src)-int(offsetSize):])[0], nil
}

type pos struct {
	start, end uint32
}

// RangeLabels iterates over all labels encoded in src
func (o Offsets) RangeLabels(src []byte) iter.Seq[[]byte] {
	return rangeSLice(src[o.labels.start:o.labels.end])
}

// GetTimestamp returns timestamps encoded in src.
func (o Offsets) GetTimestamp(src []byte) []int64 {
	return magic.ReinterpretSlice[int64](src[o.timestamp.start:o.timestamp.end])
}

// GetValue return values encoded in src.
func (o Offsets) GetValue(src []byte) []uint64 {
	return magic.ReinterpretSlice[uint64](src[o.value.start:o.value.end])
}

// RangeHistogram iterates over all histograms encoded in src
func (o Offsets) RangeHistogram(src []byte) iter.Seq[[]byte] {
	return rangeSLice(src[o.histogram.start:o.histogram.end])
}

func rangeSLice(src []byte) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		left := src
		var la []byte
		for len(left) > 0 {
			la, left = prefix.Decode(left)
			if !yield(la) {
				return
			}
		}
	}
}
