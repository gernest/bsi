package tsid

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/magic"
	"github.com/gernest/u128/storage/prefix"
)

var (
	bytePool buffer.Pool
	idPool   = &sync.Pool{New: func() any { return new(ID) }}
)

// ID is a unique identifier for metrics group that is used for indexing. This is only
// used during ingestion. The uint64 value in ID field is the one used during search,
// the rest of the fields are here to speed up ingestion.
type ID struct {
	Views [][]byte
	Rows  []uint64
	ID    uint64
}

// Get returns a new pooled ID. Make sure to call Release when done using it to return
// it back to the pool.
func Get() *ID {
	return idPool.Get().(*ID)
}

// Reset clears id but rain capacity.
func (id *ID) Reset() {
	id.ID = 0
	clear(id.Views)
	id.Views = id.Views[:0]
	id.Rows = id.Rows[:0]
}

// Release returns id back to the pool to be reused.
func (id *ID) Release() {
	id.Reset()
	idPool.Put(id)
}

// Encode serialize id into w buffer.
func (id *ID) Encode(w *buffer.B) {
	id.Reset()
	w.B = w.B[:0]

	// id is the first item
	w.B = binary.AppendUvarint(w.B, id.ID)

	// views are prefix encoded
	b := bytePool.Get()
	defer bytePool.Put(b)

	for i := range id.Views {
		b.B = prefix.Encode(b.B, id.Views[i])
	}
	w.B = prefix.Encode(w.B, b.B)

	// we already require databases to be platform specific. Faster decoding by
	// reinterpreting the slice for rows.
	w.B = prefix.Encode(w.B, magic.ReinterpretSlice[byte](id.Rows))
}

// Decode unpacks data into id.
func (id *ID) Decode(data []byte) {
	var n int
	id.ID, n = binary.Uvarint(data)
	data = data[n:]
	views, left := prefix.Decode(data)

	var view []byte
	for len(views) > 0 {
		view, views = prefix.Decode(views)
		id.Views = append(id.Views, bytes.Clone(view))
	}
	rows, _ := prefix.Decode(left)
	id.Rows = append(id.Rows[:0], magic.ReinterpretSlice[uint64](rows)...)
}
