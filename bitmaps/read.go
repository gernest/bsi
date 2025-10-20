package bitmaps

import (
	"io"

	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
)

const (
	shardVsContainerExponent = 4
)

// Wrap wraps [roaring.ContainerIterator] to implement [Cursor].
// Used mostly in tests.
type Wrap struct {
	it roaring.ContainerIterator
	mx uint64
}

// NewBitmapCursor initializes [Wrap].
func NewBitmapCursor(ra *roaring.Bitmap) *Wrap {
	it, _ := ra.Containers.Iterator(0)
	return &Wrap{it: it, mx: ra.Max()}
}

var _ Cursor = (*Wrap)(nil)

// Close implements [Cursor].
func (w *Wrap) Close() {}

// First implements [Cursor].
func (w *Wrap) First() error {
	return w.Next()
}
func (w *Wrap) Max() (uint64, error) {
	return w.mx, nil
}

// Value implements [Cursor].
func (w *Wrap) Value() (uint64, *roaring.Container) {
	return w.it.Value()
}

// Next implements [Cursor].
func (w *Wrap) Next() error {
	if !w.it.Next() {
		return io.EOF
	}
	return nil
}

// Row reads all column ids for rowID returning them as [roaring.Bitmap].
func Row(ra OffsetRanger, shard, rowID uint64) (*roaring.Bitmap, error) {
	return ra.OffsetRange(shardwidth.ShardWidth*shard,
		shardwidth.ShardWidth*rowID,
		shardwidth.ShardWidth*(rowID+1))
}

// Existence Reads an EXISTENCE bitmap. Only works with BSI or EXISTENCE encoded bitmaps.
func Existence(tx OffsetRanger, shard uint64) (*roaring.Bitmap, error) {
	return Row(tx, shard, bsiExistsBit)
}
