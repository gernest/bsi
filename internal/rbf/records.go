package rbf

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/benbjohnson/immutable"
	"github.com/gernest/roaring"
	"github.com/gernest/u128/internal/checksum"
)

const recordSize = int(unsafe.Sizeof(Record{}))

// Records holds immutable mapping of bitmap keys to root page number.
type Records = immutable.SortedMap[Key, uint32]

type View struct {
	Year uint16
	Week uint8
}

var zeroView View

func (v View) IsEmpty() bool {
	return v == zeroView
}

func ViewUnixMilli(t int64) View {
	return ViewTS(time.UnixMilli(t))
}

func ViewTS(ts time.Time) View {
	y, w := ts.ISOWeek()
	return View{
		Year: uint16(y),
		Week: uint8(w),
	}
}

func (v *View) Compare(other *View) int {
	i := cmp.Compare(v.Year, other.Year)
	if i != 0 {
		return i
	}
	return cmp.Compare(v.Week, other.Week)
}

func (v View) String() string {
	return v.Path("")
}

// Path returns path to the view directory.
func (v View) Path(base string) string {
	return filepath.Join(base, fmt.Sprintf("%04d_%02d", v.Year, v.Week))
}

type Key struct {
	Column checksum.U128
	Shard  uint64
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
}

func (r *Record) Key() Key {
	return Key{
		Column: r.Column,
		Shard:  r.Shard,
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
	i := bytes.Compare(a.Column[:], b.Column[:])
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
