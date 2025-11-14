package rbf

import (
	"cmp"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"unsafe"

	"github.com/benbjohnson/immutable"
)

const recordSize = int(unsafe.Sizeof(Record{}))

// Records holds immutable mapping of bitmap keys to root page number.
type Records = immutable.SortedMap[Key, uint32]

type Key struct {
	Column uint64
	Shard  uint64
}

var zero Key

func (k Key) IsEmpty() bool {
	return k == zero
}

func (k Key) String() string {
	return fmt.Sprintf("%06d_%06d", k.Shard, k.Column)
}

type Record struct {
	Column uint64
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
	i := cmp.Compare(a.Column, b.Column)
	if i != 0 {
		return i
	}
	return cmp.Compare(a.Shard, b.Shard)
}

type Size struct {
	Pages [SizePageTypeBitmap + 1]uint64
}

func (s *Size) Bytes() (t uint64) {
	for i := range s.Pages {
		t += s.Pages[i]
	}
	t *= PageSize
	return
}

type SizedPageType byte

const (
	SizePageTypeRootRecord SizedPageType = iota
	SizePageTypeLeaf
	SizePageTypeBranch
	SizePageTypeBitmapHeader
	SizePageTypeBitmap
)

var szt = [...]string{
	"root",
	"leaf",
	"branch",
	"bitmap_header",
	"bitmap",
}

func (s SizedPageType) String() string {
	return szt[s]
}

func toSized(typ int) SizedPageType {
	switch typ {
	case PageTypeRootRecord:
		return SizePageTypeRootRecord
	case PageTypeLeaf:
		return SizePageTypeLeaf
	case PageTypeBranch:
		return SizePageTypeBranch
	case PageTypeBitmapHeader:
		return SizePageTypeBitmapHeader
	case PageTypeBitmap:
		return SizePageTypeBitmap
	default:
		panic(fmt.Sprintf("unexpected page type%v", typ))
	}
}
func (tx *Tx) RangeSize() iter.Seq2[Key, Size] {
	lo := tx.db.logger
	if lo == nil {
		lo = slog.Default()
	}
	records, err := tx.RootRecords()
	if err != nil {
		lo.Error("failed reading root records", "er", err)
		return func(_ func(Key, Size) bool) {}
	}

	return func(yield func(Key, Size) bool) {

		for itr := records.Iterator(); !itr.Done(); {
			kx, pgno, _ := itr.Next()
			var sz Size
			err := tx.walkTree(pgno, 0, func(_, _, typ uint32, err error) error {
				t := toSized(int(typ))
				sz.Pages[t]++
				return err
			})
			if err != nil {
				lo.Error("failed walking tree", "key", kx, "er", err)
			}
			if !yield(kx, sz) {
				return
			}
		}
	}
}
