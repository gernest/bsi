package keys

import (
	"encoding/binary"
	"fmt"

	"github.com/gernest/u128/storage/buffer"
	"github.com/gernest/u128/storage/prefix"
)

var (
	DataIndex   = []byte("d")
	SearchIndex = []byte("s")

	MetricsValue     = []byte("v")
	MetricsTimestamp = []byte("t")
	MetricsLabels    = []byte("l")
)

// View builds ISO 8601 year and week view.
func View(b *buffer.B, year, week int) []byte {
	b.B = fmt.Appendf(b.B[:0], "%04d_%02d", year, week)
	return b.B
}

// Key constructs  a key we use for bitmaps in rbf storage.
func Key(w *buffer.B, index, field, view []byte, shard uint64) []byte {
	w.B = w.B[:0]

	// Index is encoded as length prefix
	w.B = prefix.Encode(w.B, index)
	// Field is encoded as length prefixed
	w.B = prefix.Encode(w.B, field)

	// View is encoded as length prefixed
	w.B = prefix.Encode(w.B, view)

	// we use last 8 bytes as shard without length prefix. We use big endian to make
	// sure the strings are sorted.
	w.B = binary.BigEndian.AppendUint64(w.B, shard)
	return w.B
}
