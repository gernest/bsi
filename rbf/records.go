package rbf

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"unsafe"

	"github.com/benbjohnson/immutable"
	"github.com/gernest/roaring"
	"github.com/gernest/u128/checksum"
)

const recordSize = int(unsafe.Sizeof(Record{}))

// Records holds immutable mapping of bitmap keys to root page number.
type Records = immutable.SortedMap[Key, uint32]

type Key struct {
	Column checksum.U128
	Shard  uint64
	Year   uint16
	Week   uint8
}

func (k Key) String() string {
	return fmt.Sprintf("%x_%d", k.Column, k.Shard)
}

var zero Key

// IsEmpty returns true if key is zero.
func (k Key) IsEmpty() bool {
	return k == zero
}

type Record struct {
	Column checksum.U128
	Shard  uint64
	Page   uint32
	Year   uint16
	Week   uint8
}

func (r *Record) Key() Key {
	return Key{
		Column: r.Column,
		Shard:  r.Shard,
		Year:   r.Year,
		Week:   r.Week,
	}
}

var zeroRecord Record

// IsEmpty returns true if r is zero.
func (r Record) IsEmpty() bool {
	return r == zeroRecord
}

// WriteRecord copies rec to data and returns the remaining data slice.
func WriteRecord(data []byte, rec Record) (remaining []byte, err error) {
	if len(data) < recordSize {
		return data, io.ErrShortBuffer
	}
	r := (*Record)(unsafe.Pointer(&data[0]))
	*r = rec
	return data[recordSize:], nil
}

// ReadRecord decodes root record from data.
func ReadRecord(data []byte) (rec Record, remaining []byte, err error) {
	if len(data) < recordSize {
		return Record{}, data, nil
	}
	r := (*Record)(unsafe.Pointer(&data[0]))
	return *r, data[recordSize:], nil
}

type CompareRecord struct{}

func (CompareRecord) Compare(a, b Key) int {
	i := cmp.Compare(a.Year, b.Year)
	if i != 0 {
		return i
	}
	i = cmp.Compare(a.Week, b.Week)
	if i != 0 {
		return i
	}
	i = bytes.Compare(a.Column[:], b.Column[:])
	if i != 0 {
		return i
	}
	return cmp.Compare(a.Shard, b.Shard)
}

type Map map[Key]*roaring.Bitmap

func (r Map) Get(column checksum.U128, shard uint64) *roaring.Bitmap {
	k := Key{Shard: shard, Column: column}
	x, ok := r[k]
	if ok {
		return x
	}
	x = roaring.NewMapBitmap()
	r[k] = x
	return x
}
