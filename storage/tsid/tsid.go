package tsid

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/magic"
	"github.com/gernest/u128/storage/prefix"
	_ "github.com/prometheus/prometheus/model/labels"
)

var bytePool buffer.Pool
var idPool = &sync.Pool{New: func() any { return new(ID) }}

type Raw []byte

// ID identifies stored timeseries
type ID struct {
	Views [][]byte
	Rows  []uint64
	ID    uint64
}

func Get() *ID {
	return idPool.Get().(*ID)
}

func (id *ID) Reset() {
	id.ID = 0
	clear(id.Views)
	id.Views = id.Views[:0]
	id.Rows = id.Rows[:0]
}

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

// Decode unpacks data into id.  This does not copy, use Clone method to create a copy.
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
