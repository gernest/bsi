package tsid

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/gernest/u128/checksum"
	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/magic"
	"github.com/gernest/u128/storage/prefix"
)

var (
	bytesPool buffer.Pool
)

type B struct {
	B []ID
}

type Pool struct {
	pool sync.Pool
}

func (p *Pool) Get() *B {
	v := p.pool.Get()
	if v != nil {
		return v.(*B)
	}
	return new(B)
}

func (p *Pool) Put(b *B) {
	b.B = b.B[:0]
	p.pool.Put(b)
}

// ID is a unique identifier for metrics group that is used for indexing. This is only
// used during ingestion. The uint64 value in ID field is the one used during search,
// the rest of the fields are here to speed up ingestion.
type ID struct {
	Views []checksum.U128
	Rows  []uint64
	ID    uint64
}

func (id ID) String() string {
	var o bytes.Buffer
	fmt.Fprintf(&o, "%d", id.ID)
	for i := range id.Views {
		fmt.Fprintf(&o, " %x=%d", id.Views[i], id.Rows[i])
	}
	return o.String()
}

func (id *ID) Reset() {
	id.Views = id.Views[:0]
	id.Rows = id.Rows[:0]
	id.ID = 0
}

// Encode serialize id into w buffer.
func (id *ID) Encode() []byte {
	w := bytesPool.Get()
	defer bytesPool.Put(w)

	w.B = binary.AppendUvarint(w.B, id.ID)
	w.B = prefix.Encode(w.B, magic.ReinterpretSlice[byte](id.Views))
	w.B = prefix.Encode(w.B, magic.ReinterpretSlice[byte](id.Rows))
	return bytes.Clone(w.B)
}

// Decode unpacks data into id.
func (id *ID) Decode(data []byte) {
	var n int
	id.ID, n = binary.Uvarint(data)
	data = data[n:]
	views, left := prefix.Decode(data)
	id.Views = append(id.Views, magic.ReinterpretSlice[checksum.U128](views)...)
	rows, _ := prefix.Decode(left)
	id.Rows = append(id.Rows[:0], magic.ReinterpretSlice[uint64](rows)...)
}
